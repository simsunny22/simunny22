#include "hive/commitlog/hive_commitlog.hh"
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <malloc.h>
#include <regex>
#include <boost/range/adaptor/map.hpp>
#include <unordered_map>
#include <unordered_set>

#include <core/align.hh>
#include <core/reactor.hh>
#include <core/scollectd.hh>
#include <core/future-util.hh>
#include <core/file.hh>
#include <core/rwlock.hh>
#include <core/gate.hh>
#include <core/sleep.hh>
#include <core/fstream.hh>
#include <core/shared_mutex.hh>
#include <seastar/core/memory.hh>
#include <net/byteorder.hh>

#include "db/config.hh"
#include "utils/data_input.hh"
#include "utils/runtime.hh"
#include "hive/utils/flush_queue.hh"
#include "log.hh"
#include "service/priority_manager.hh"

#include <boost/range/numeric.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "checked-file-impl.hh"
#include "disk-error-handler.hh"

#include "hive/journal_proxy.hh"
#include "hive/journal/primary_journal.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/utils/crc.hh"

logging::logger dltest_log("dltest"); //tododl:test

using namespace std::chrono_literals;
namespace hive {

//tododl:test
uint32_t g_already_exception = 0;


static logging::logger logger("hive_commitlog");

class crc32_nbo {
    crc32 _c;
public:
    template <typename T>
    void process(T t) {
        _c.process(net::hton(t));
    }
    uint32_t checksum() const {
        return _c.get();
    }
    void process_bytes(const uint8_t* data, size_t size) {
        return _c.process(data, size);
    }
    void process_bytes(const int8_t* data, size_t size) {
        return _c.process(reinterpret_cast<const uint8_t*>(data), size);
    }
    void process_bytes(const char* data, size_t size) {
        return _c.process(reinterpret_cast<const uint8_t*>(data), size);
    }
}; //crc32_nbo


std::ostream& operator<<(std::ostream& out, const hive::replay_position& p) {
    return out << "{" << p.shard_id() << ", " << p.base_id() << ", " << p.pos << "}";
}


//////////////////////////////////////
// define for hive_commitlog::config
//////////////////////////////////////
hive_commitlog::config::config(const db::config& cfg, sstring commitlog_location)
    : commit_log_location(commitlog_location)
    //tododl:yellow need config in vega_hive.yaml
    , commitlog_total_space_in_mb( 
        cfg.commitlog_total_space_in_mb() >= 0 ? cfg.commitlog_total_space_in_mb() : memory::stats().total_memory() >> 20)
    , commitlog_segment_size_in_mb(cfg.commitlog_segment_size_in_mb())
    , commitlog_sync_period_in_ms(cfg.commitlog_sync_period_in_ms())
    , mode(cfg.commitlog_sync() == "batch" ? sync_mode::BATCH : sync_mode::PERIODIC)
    , commitlog_cycle_sleep_in_us(cfg.commitlog_cycle_sleep_in_us())
{}


/////////////////////////////////////
// define for hive_commitlog::config
/////////////////////////////////////
hive_commitlog::descriptor::descriptor(sstring commitlog_id, segment_id_type i, uint32_t v)
        : commitlog_id(commitlog_id), id(i), ver(v) {
}

hive_commitlog::descriptor::descriptor(sstring commitlog_id, replay_position p)
        : descriptor(commitlog_id, p.id) {
}

hive_commitlog::descriptor::descriptor(sstring commitlog_id, std::pair<uint64_t, uint32_t> p)
        : descriptor(commitlog_id, p.first, p.second) {
}

hive_commitlog::descriptor::descriptor(sstring filename) {
    std::smatch m;
    // match both legacy and new version of commitlogs Ex: CommitLog-$commitlog_id-4-12345.log.
    std::regex rx("(?:.*/)?" + FILENAME_PREFIX +"([a-zA-Z0-9_]+[_][0-9]+)" + SEPARATOR + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
    std::string sfilename = filename;
    if (!std::regex_match(sfilename, m, rx)) {
        throw std::domain_error("Cannot parse the version of the file: " + filename);
    }
    if (m[4].length() == 0) {
        // CMH. Can most likely ignore this
        //tododl:yellow how to deal with this version problem?
        throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
    }
    
    id = std::stoull(m[4].str().substr(1));
    ver = std::stoul(m[3].str());
    commitlog_id = m[1].str();
}

sstring hive_commitlog::descriptor::filename()const{
    return FILENAME_PREFIX + commitlog_id.c_str() + SEPARATOR + std::to_string(ver) + SEPARATOR
            + std::to_string(id) + FILENAME_EXTENSION;
}

hive_commitlog::descriptor::operator replay_position() const {
    return replay_position(id);
}

const std::string hive_commitlog::descriptor::SEPARATOR("-");
const std::string hive_commitlog::descriptor::FILENAME_PREFIX(
        "CommitLog" + SEPARATOR);
const std::string hive_commitlog::descriptor::FILENAME_EXTENSION(".log");


////////////////////////////////////////////////////
// define for class hive_commitlog::segment_manager
////////////////////////////////////////////////////
class hive_commitlog::segment_manager : public ::enable_shared_from_this<segment_manager> {
public:
    sstring _volume_id;
    sstring _commitlog_id;
    config cfg;
    const uint64_t max_size;
    const uint64_t max_mutation_size;
    // Divide the size-on-disk threshold by #cpus used, since we assume
    // we distribute stuff more or less equally across shards.
    const uint64_t max_disk_size; // per-shard

    bool _shutdown = false;

    semaphore _new_segment_semaphore;
    semaphore _write_semaphore;
    semaphore _flush_semaphore;

    scollectd::registrations _regs;

    primary_journal* _parent_journal = nullptr;

    // TODO: verify that we're ok with not-so-great granularity
    using clock_type = lowres_clock;
    using time_point = clock_type::time_point;
    using sseg_ptr = lw_shared_ptr<segment>;

    struct stats {
        uint64_t cycle_count = 0;
        uint64_t flush_count = 0;
        uint64_t allocation_count = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_slack = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_writes = 0;
        uint64_t pending_flushes = 0;
        uint64_t pending_allocations = 0;
        uint64_t write_limit_exceeded = 0;
        uint64_t flush_limit_exceeded = 0;
        uint64_t total_size = 0;
        uint64_t buffer_list_bytes = 0;
        uint64_t total_size_on_disk = 0;
    };

    stats totals;

    //during the process of segment create, the cpu will changes some times
    //when on_timer run, if allocated a segment id(2) but not create over so the _reserve_segments is empty
    //this time the write comming, check the _reserve_segments is empty will create a new segment id is 3
    //the segment(id = 3) will be used first and the segment(id=2) will be used later
    //if so, will cause the 'replay_position_reordered_exception' in the primary_journal
    seastar::shared_mutex _create_segment_mutex;

    void set_parent_journal(primary_journal* parent_journal){
        _parent_journal = parent_journal; 
    }

    future<> begin_write() {
        ++totals.pending_writes; // redundant, given semaphore. but easier to read
        if (totals.pending_writes >= cfg.max_active_writes) {
            ++totals.write_limit_exceeded;
            logger.trace("Write ops overflow: {}. Will block.", totals.pending_writes);
        }
        return _write_semaphore.wait();
    }
    void end_write() {
        _write_semaphore.signal();
        --totals.pending_writes;
    }

    future<> begin_flush() {
        ++totals.pending_flushes;
        if (totals.pending_flushes >= cfg.max_active_flushes) {
            ++totals.flush_limit_exceeded;
            logger.trace("Flush ops overflow: {}. Will block.", totals.pending_flushes);
        }
        return _flush_semaphore.wait();
    }
    void end_flush() {
        _flush_semaphore.signal();
        --totals.pending_flushes;
    }

    bool should_wait_for_write() const {
        return cfg.mode == sync_mode::BATCH || _write_semaphore.waiters() > 0 || _flush_semaphore.waiters() > 0;
    }

    segment_manager(sstring commitlog_id, sstring volume_id, db::config c, sstring commitlog_location)
        : _volume_id(volume_id)
        , _commitlog_id(commitlog_id)
        ,cfg([&c, commitlog_location] {
            config cfg(c, commitlog_location);

            if (cfg.commit_log_location.empty()) {
                logger.error("[segemnt_manager] error, the config of commit_log_location is empty");
                assert(false);
            }

            if (cfg.max_active_writes == 0) {
                //tododl:yellow need config in vega_hive.yaml
                cfg.max_active_writes = 25 * smp::count;
            }
            cfg.max_active_writes = std::max(uint64_t(1), cfg.max_active_writes / smp::count);
            if (cfg.max_active_flushes == 0) {
                //tododl:yellow need config in vega_hive.yaml
                cfg.max_active_flushes = 5 * smp::count;
            }
            cfg.max_active_flushes = std::max(uint64_t(1), cfg.max_active_flushes / smp::count);

            return cfg;
        }())
        , max_size(std::min<size_t>(std::numeric_limits<position_type>::max()
                                  , std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1) * 1024 * 1024))
        , max_mutation_size(max_size >> 1)
        , max_disk_size(size_t(std::ceil(cfg.commitlog_total_space_in_mb / double(smp::count))) * 1024 * 1024)
        , _write_semaphore(cfg.max_active_writes)
        , _flush_semaphore(cfg.max_active_flushes)
    {
        assert(max_size > 0);

        logger.trace("Commitlog {} maximum disk size: {} MB / cpu ({} cpus)",
                cfg.commit_log_location, max_disk_size / (1024 * 1024),
                smp::count);

        _regs = create_counters();
    }
    ~segment_manager() {
        logger.trace("Commitlog {} disposed", cfg.commit_log_location);
    }

    uint64_t next_id() {
        return ++_ids;
    }

    future<> init();
    future<sseg_ptr> new_segment();
    future<sseg_ptr> active_segment();
    future<sseg_ptr> allocate_segment(bool active);

    future<> clear();
    future<> sync_all_segments(bool shutdown = false);
    future<> shutdown();

    scollectd::registrations create_counters();

    void orphan_all();

    void discard_unused_segments();
    void discard_completed_segments(const replay_position& pos);
    void on_timer();
    void sync();
    void arm(uint32_t extra = 0) {
        if (!_shutdown) {
            _timer.arm(std::chrono::milliseconds(cfg.commitlog_sync_period_in_ms + extra));
        }
    }

    std::vector<sstring> get_active_names() const;
    uint64_t get_num_dirty_segments() const;
    uint64_t get_num_active_segments() const;

    using buffer_type = temporary_buffer<char>;

    buffer_type acquire_buffer(size_t s);
    void release_buffer(buffer_type&&);

    future<std::vector<descriptor>> list_descriptors(sstring dir);

    flush_handler_id add_flush_handler(flush_handler h) {
        auto id = ++_flush_ids;
        _flush_handlers[id] = std::move(h);
        return id;
    }
    void remove_flush_handler(flush_handler_id id) {
        _flush_handlers.erase(id);
    }

    void flush_segments(bool = false);

private:
    segment_id_type _ids = 0;
    std::vector<sseg_ptr> _segments;
    std::deque<sseg_ptr> _reserve_segments;
    std::vector<buffer_type> _temp_buffers;
    std::unordered_map<flush_handler_id, flush_handler> _flush_handlers;
    flush_handler_id _flush_ids = 0;
    replay_position _flush_position;
    timer<clock_type> _timer;
    size_t _reserve_allocating = 0;
    // # segments to try to keep available in reserve
    // i.e. the amount of segments we expect to consume inbetween timer
    // callbacks.
    // The idea is that since the files are 0 len at start, and thus cost little,
    // it is easier to adapt this value compared to timer freq.
    size_t _num_reserve_segments = 0;
    seastar::gate _gate;
    uint64_t _new_counter = 0;
}; //class hive_commitlog::segment_manager



////////////////////////////////////////////
// define for class hive_commitlog::segment
////////////////////////////////////////////
class hive_commitlog::segment: public enable_lw_shared_from_this<segment> {
    primary_journal* _parent_journal = nullptr;
    sstring _volume_id;
    sstring _commitlog_id;
    ::shared_ptr<segment_manager> _segment_manager;
    descriptor _desc;
    file _file;
    sstring _file_name;

    uint64_t _file_pos = 0;
    uint64_t _flush_pos = 0;
    uint64_t _buf_pos = 0;
    bool _closed = false;

    size_t _needed_size = 0;

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr = segment_manager::sseg_ptr;
    using clock_type = segment_manager::clock_type;
    using time_point = segment_manager::time_point;

    buffer_type _buffer;
    //std::unordered_map<cf_id_type, position_type> _cf_dirty;
    time_point _sync_time;
    seastar::gate _gate;
    uint64_t _write_waiters = 0;
    hive::flush_queue<replay_position> _pending_ops;
    uint64_t _num_allocs = 0;
    std::unordered_set<table_schema_version> _known_schema_versions;

    position_type _dirty_pos; 
    bool          _can_delete = false;
    uint64_t _commitlog_cycle_sleep_in_us = 0;

    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    future<> begin_flush() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _segment_manager->begin_flush();
    }

    void end_flush() {
        _segment_manager->end_flush();
    }

    future<> begin_write() {
        // This is maintaining the semantica of only using the write-lock
        // as a gate for flushing, i.e. once we've begun a flush for position X
        // we are ok with writes to positions > X
        return _segment_manager->begin_write();
    }

    void end_write() {
        _segment_manager->end_write();
    }

    //tododl:test
    void print_queue_items(){
        auto& queue_items = _pending_ops.get_queue_items();
        dltest_log.error("dltest print_queue_items flush_queue size:{}", queue_items.size());
        for(auto& item : queue_items) {
            auto rp = item.first;
            size_t count = item.second.count;
            dltest_log.error("dltest print_queue_items rp:{}, count:{}", rp, count);
        }
    }

    size_t get_queue_items_size(){
        return _pending_ops.get_queue_items().size(); 
    }

public:
    struct cf_mark {
        const segment& s;
    };
    friend std::ostream& operator<<(std::ostream&, const cf_mark&);

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    static constexpr size_t entry_overhead_size = 3 * sizeof(uint32_t);
    static constexpr size_t segment_overhead_size = 2 * sizeof(uint32_t);
    static constexpr size_t descriptor_header_size = 5 * sizeof(uint32_t);
    static constexpr uint32_t segment_magic = ('S'<<24) |('C'<< 16) | ('L' << 8) | 'C';

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static constexpr size_t sync_marker_size = 2 * sizeof(uint32_t);

    static constexpr size_t alignment = 4096;
    // TODO : tune initial / default size
    static constexpr size_t default_size = align_up<size_t>(128 * 1024, alignment);

    segment(primary_journal* parent_journal
          , sstring volume_id
          , sstring commitlog_id
          , ::shared_ptr<segment_manager> m
          , const descriptor& d
          , file && f
          , bool active
          , uint64_t commitlog_cycle_sleep_in_us)
              : _parent_journal(parent_journal)
              , _volume_id(volume_id)
              , _commitlog_id(commitlog_id)
              , _segment_manager(std::move(m))
              , _desc(std::move(d))
              , _file(std::move(f))
              , _file_name(_segment_manager->cfg.commit_log_location + "/" + _desc.filename())
              , _sync_time(clock_type::now())
              , _commitlog_cycle_sleep_in_us(commitlog_cycle_sleep_in_us)
    {
        ++_segment_manager->totals.segments_created;
        logger.debug("Created new {} segment {}", active ? "active" : "reserve", *this);
    }
    ~segment() {
        if (is_clean()) {
            logger.debug("Segment {} is no longer active and will be deleted now", *this);
            ++_segment_manager->totals.segments_destroyed;
            _segment_manager->totals.total_size_on_disk -= size_on_disk();
            _segment_manager->totals.total_size -= (size_on_disk() + _buffer.size());
            try {
                commit_io_check(::unlink, _file_name.c_str());
            } catch (...) {
                logger.error("Could not delete segment {}: {}", *this, std::current_exception());
            }
        } else {
            logger.warn("Segment {} is dirty and is left on disk.", *this);
        }
    }

    bool is_schema_version_known(schema_ptr s) {
        return _known_schema_versions.count(s->version());
    }
    void add_schema_version(schema_ptr s) {
        _known_schema_versions.emplace(s->version());
    }
    void forget_schema_versions() {
        _known_schema_versions.clear();
    }

    bool must_sync() {
        if (_segment_manager->cfg.mode == sync_mode::BATCH) {
            return false;
        }
        auto now = clock_type::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - _sync_time).count();
        if ((_segment_manager->cfg.commitlog_sync_period_in_ms * 2) < uint64_t(ms)) {
            logger.debug("{} needs sync. {} ms elapsed", *this, ms);
            return true;
        }
        return false;
    }
    /**
     * Finalize this segment and get a new one
     */
    future<sseg_ptr> finish_and_get_new() {
        _closed = true;
        return maybe_wait_for_write(sync()).then([](sseg_ptr s) {
            return s->_segment_manager->active_segment();
        });
    }
    void reset_sync_time() {
        _sync_time = clock_type::now();
    }
    // See class comment for info
    future<sseg_ptr> sync(bool shutdown = false) {
        /**
         * If we are shutting down, we first
         * close the allocation gate, thus no new
         * data can be appended. Then we just issue a
         * flush, which will wait for any queued ops
         * to complete as well. Then we close the ops
         * queue, just to be sure.
         */
        if (shutdown) {
            auto me = shared_from_this();
            return _gate.close().then([me] {
                return me->sync().finally([me] {
                    // When we get here, nothing should add ops,
                    // and we should have waited out all pending.
                    return me->_pending_ops.close();
                });
            });
        }

        // Note: this is not a marker for when sync was finished.
        // It is when it was initiated
        reset_sync_time();
        return cycle(true);
    }
    // See class comment for info
    future<sseg_ptr> flush(uint64_t pos = 0) {
        auto me = shared_from_this();
        assert(!me.owned());
        if (pos == 0) {
            pos = _file_pos;
        }

        logger.trace("Syncing {} {} -> {}", *this, _flush_pos, pos);
        // Only run the flush when all write ops at lower rp:s
        // have completed.
        replay_position rp(_desc.id, position_type(pos));

        // Run like this to ensure flush ordering, and making flushes "waitable"
        return _pending_ops.run_with_ordered_post_op(rp, [] { return make_ready_future<>(); }, [this, pos, me, rp] {
            assert(_pending_ops.has_operation(rp));
            return do_flush(pos);
        });
    }

    future<sseg_ptr> do_flush(uint64_t pos) {
        auto me = shared_from_this();
        return begin_flush().then([this, pos]() {
            if (pos <= _flush_pos) {
                logger.trace("{} already synced! ({} < {})", *this, pos, _flush_pos);
                return make_ready_future<>();
            }
            //tododl:test
            //this->print_queue_items();
            return _file.flush().then_wrapped([this, pos](future<> f) {
                try {
                    f.get();
                    // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                    // we fast-fail the whole commit.
                    _flush_pos = std::max(pos, _flush_pos);
                    ++_segment_manager->totals.flush_count;
                    logger.trace("{} synced to {}", *this, _flush_pos);
                } catch (...) {
                    logger.error("error Failed to flush commits to disk: {}", std::current_exception());
                    throw;
                }
            });
        }).finally([this] {
            end_flush();
        }).then([me] {
            return make_ready_future<sseg_ptr>(me);
        });
    }

    /**
     * Allocate a new buffer
     */
    void new_buffer(size_t s) {
        assert(_buffer.empty());

        s += _needed_size;
        _needed_size = 0;

        auto overhead = segment_overhead_size;
        if (_file_pos == 0) {
            overhead += descriptor_header_size;
        }

        auto a = align_up(s + overhead, alignment);
        auto k = std::max(a, default_size);

        for (;;) {
            try {
                _buffer = _segment_manager->acquire_buffer(k);
                break;
            } catch (std::bad_alloc&) {
                logger.warn("Could not allocate {} k bytes output buffer ({} k required)", k / 1024, a / 1024);
                if (k > a) {
                    k = std::max(a, k / 2);
                    logger.debug("Trying reduced size: {} k", k / 1024);
                    continue;
                }
                throw;
            }
        }
        _buf_pos = overhead;
        auto * p = reinterpret_cast<uint32_t *>(_buffer.get_write());
        std::fill(p, p + overhead, 0);
        _segment_manager->totals.total_size += k;
    }

    bool buffer_is_empty() const {
        return _buf_pos <= segment_overhead_size
                        || (_file_pos == 0 && _buf_pos <= (segment_overhead_size + descriptor_header_size));
    }
    /**
     * Send any buffer contents to disk and get a new tmp buffer
     */
    // See class comment for info
    future<sseg_ptr> cycle(bool flush_after = false) {
        if (_buffer.empty()) {
            return flush_after ? flush() : make_ready_future<sseg_ptr>(shared_from_this());
        }

        auto size = clear_buffer_slack();
        auto buf = std::move(_buffer);
        auto off = _file_pos;
        auto top = off + size;
        auto num = _num_allocs;
        
        _file_pos = top;
        _buf_pos = 0;
        _num_allocs = 0;

        auto me = shared_from_this();
        assert(!me.owned());

        auto * p = buf.get_write();
        assert(std::count(p, p + 2 * sizeof(uint32_t), 0) == 2 * sizeof(uint32_t));

        data_output out(p, p + buf.size());

        auto header_size = 0;

        if (off == 0) {
            // first block. write file header.
            out.write(segment_magic);
            out.write(_desc.ver);
            out.write(_desc.id);
            crc32_nbo crc;
            crc.process(_desc.ver);
            crc.process<int32_t>(_desc.id & 0xffffffff);
            crc.process<int32_t>(_desc.id >> 32);
            out.write(crc.checksum());
            header_size = descriptor_header_size;
        }

        // write chunk header
        crc32_nbo crc;
        crc.process<int32_t>(_desc.id & 0xffffffff);
        crc.process<int32_t>(_desc.id >> 32);
        crc.process(uint32_t(off + header_size));

        out.write(uint32_t(_file_pos));
        out.write(crc.checksum());

        forget_schema_versions();

        replay_position rp(_desc.id, position_type(off));

        logger.debug("Writing {} entries, {} k in {} -> {}", num, size, off, off + size);
        // The write will be allowed to start now, but flush (below) must wait for not only this,
        // but all previous write/flush pairs.
        return _pending_ops.run_with_ordered_post_op(rp, [this, size, off, top, rp, buf = std::move(buf)]() mutable {
            return do_sync(off, size, std::move(buf)).then_wrapped([this, off, top, rp](auto f){
                try{
                    f.get(); 
                    return make_ready_future<>();
                }catch(...){
                    _file_pos = off; 
                    _flush_pos = _file_pos;
                    std::ostringstream out;
                    out << "[cycle] error, do_sync failed, rp:" << rp;
                    out << ", exception:" << std::current_exception();
                    auto error_info = out.str();
                    _parent_journal->mark_all_write_context_exception(error_info);
                    logger.error(error_info.c_str());
                    return make_exception_future<>(std::runtime_error(error_info));
                }
            });
        }, [me, flush_after, top, rp] { // lambda instead of bind, so we keep "me" alive.
            assert(me->_pending_ops.has_operation(rp));
            return flush_after ? me->do_flush(top) : make_ready_future<sseg_ptr>(me);
        });
    }

#if 0
    //write local and remote
    future<> do_sync(uint64_t off, size_t size, buffer_type&& buf) {
        //tododl:test
        //if(++g_already_exception % 50  == 0){
        //    logger.error("error dltest_chain_exception inner do_sync, this:{}, off:{}, size:{}", this, off, size);
        //    print_queue_items();
        //    return make_exception_future<>(std::runtime_error("dltest make exception")); 
        //}

        logger.debug("[{}] start, segment:{}, off:{}, size:{}", __func__, *this, off, size);
        return do_with(std::move(buf), [this, size, off](buffer_type& buf)mutable{
            return sync_remote(off, size, buf).then([this, off, size, &buf](){
                return sync_locally(off, size, buf); 
            }).then_wrapped([this, off, size](auto f){
                try{
                    f.get();
                    logger.debug("[do_sync] end, segment:{}, off:{}, size:{}", *this, off, size);
                    return make_ready_future<>();
                }catch (...){
                    logger.error("[do_sync]error while do_sync, exception:{}", std::current_exception()); 
                    throw;
                }
            });
        });
    }
#endif   

    //write local and remote
    future<> do_sync(uint64_t off, size_t size, buffer_type&& buf) {
        //tododl:test
        //if(++g_already_exception % 50  == 0){
        //    logger.error("error dltest_chain_exception inner do_sync, this:{}, off:{}, size:{}", this, off, size);
        //    print_queue_items();
        //    return make_exception_future<>(std::runtime_error("dltest make exception")); 
        //}

        logger.debug("[{}] start, segment:{}, off:{}, size:{}", __func__, *this, off, size);
        return do_with(std::move(buf), [this, size, off](buffer_type& buf)mutable{
            std::vector<future<>> futures;
            auto fut_remote = sync_remote(off, size, buf);
            futures.push_back(std::move(fut_remote));

            auto fut_locally = sync_locally(off, size, buf);
            futures.push_back(std::move(fut_locally));

            return when_all(futures.begin(), futures.end()).discard_result();
        });
    }

    future<> sync_remote(uint64_t off, size_t size, buffer_type& buf) {
        logger.debug("[{}] start, off:{}, size:{}", __func__, off, size);
        bytes sync_data(reinterpret_cast<const signed char*>(buf.begin()), size);
        auto& local_journal_proxy = hive::get_local_journal_proxy();
        return local_journal_proxy.sync_commitlog(_volume_id, _commitlog_id, _desc.filename(), off, size, std::move(sync_data)); 
    }

    future<> sync_locally(uint64_t off, size_t size, buffer_type& buf) {
        return begin_write().then([this, off, size, &buf]() mutable {
            logger.debug("[{}] start, segment:{}, off:{}, size:{}", __func__, *this, off, size);
            auto written = make_lw_shared<size_t>(0);
            auto p = buf.get();
            return repeat([this, size, off, written, p]() mutable {
                auto&& priority_class = service::get_local_commitlog_priority();
                return _file.dma_write(off + *written, p + *written, size - *written, priority_class).then_wrapped(
                    [this, size, written](future<size_t>&& f) {
                    try {
                        auto bytes = std::get<0>(f.get());
                        *written += bytes;
                        _segment_manager->totals.bytes_written += bytes;
                        _segment_manager->totals.total_size_on_disk += bytes;
                        ++_segment_manager->totals.cycle_count;

                        if (*written == size) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                        // gah, partial write. should always get here with dma chunk sized
                        // "bytes", but lets make sure...
                        *written = align_down(*written, alignment);
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                        // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                        // we fast-fail the whole commit.
                    } catch (...) {
                        logger.error("[sync_locally]error, failed to persist commits to disk for segment:{}, exception:{}"
                            , *this, std::current_exception());
                        throw;
                    }
               });
           });
       }).finally([this, off, size]() {
           logger.debug("[sync_locally] end, segment:{}, off:{}, size:{}", *this, off, size);
           end_write(); // release
       });
    }

    future<sseg_ptr> maybe_wait_for_write(future<sseg_ptr> f) {
        if (_segment_manager->should_wait_for_write()) {
            ++_write_waiters;
            logger.trace("Too many pending writes. Must wait.");
            return f.finally([this] {
                --_write_waiters;
            });
        }
        return make_ready_future<sseg_ptr>(shared_from_this());
    }

    /**
     * If an allocation causes a write, and the write causes a block,
     * any allocations post that need to wait for this to finish,
     * other wise we will just continue building up more write queue
     * eventually (+ loose more ordering)
     *
     * Some caution here, since maybe_wait_for_write actually
     * releases _all_ queued up ops when finishing, we could get
     * "bursts" of alloc->write, causing build-ups anyway.
     * This should be measured properly. For now I am hoping this
     * will work out as these should "block as a group". However,
     * buffer memory usage might grow...
     */
    bool must_wait_for_alloc() {
        // Note: write_waiters is decremented _after_ both semaphores and
        // flush queue might be cleared. So we should not look only at it.
        // But we still don't want to look at "should_wait_for_write" directly,
        // since that is "global" and includes other segments, and we want to
        // know if _this_ segment has blocking write ops pending.
        // So we also check that the flush queue is non-empty.
        return _write_waiters > 0 && !_pending_ops.empty();
    }

    future<sseg_ptr> wait_for_alloc() {
        auto me = shared_from_this();
        ++_segment_manager->totals.pending_allocations;
        logger.trace("Previous allocation is blocking. Must wait.");
        return _pending_ops.wait_for_pending().then([me] { // TODO: do we need a finally?
            --me->_segment_manager->totals.pending_allocations;
            return make_ready_future<sseg_ptr>(me);
        });
    }

    future<sseg_ptr> batch_cycle() {
        /**
         * For batch mode we force a write "immediately".
         * However, we first wait for all previous writes/flushes
         * to complete.
         *
         * This has the benefit of allowing several allocations to
         * queue up in a single buffer.
         */
        auto me = shared_from_this();
        auto fp = _file_pos;
        return _pending_ops.wait_for_pending().then([me = std::move(me), fp] {
            if (fp != me->_file_pos) {
                // some other request already wrote this buffer.
                // If so, wait for the operation at our intended file offset
                // to finish, then we know the flush is complete and we
                // are in accord.
                // (Note: wait_for_pending(pos) waits for operation _at_ pos (and before),
                replay_position rp(me->_desc.id, position_type(fp));
                return me->_pending_ops.wait_for_pending(rp).then([me, fp] {
                    return make_ready_future<sseg_ptr>(me);
                });
            }
            return me->sync();
        });
        
    }
    /**
     * Add a "mutation" to the segment.
     */
    future<uint64_t> allocate(shared_ptr<entry_writer> writer) {
        const auto size = writer->size(*this);
        const auto s = size + entry_overhead_size; // total size
        if (s > _segment_manager->max_mutation_size) {
            return make_exception_future<uint64_t>( 
                std::invalid_argument("Mutation of " + std::to_string(s)
                + " bytes is too large for the maxiumum size of "
                + std::to_string(_segment_manager->max_mutation_size))
            );
        }

        std::experimental::optional<future<sseg_ptr>> op;

        if (must_sync()) {
            op = sync();
        } else if (must_wait_for_alloc()) {
            op = wait_for_alloc();
        } else if (!is_still_allocating() || position() + s > _segment_manager->max_size) { // would we make the file too big?
            // do this in next segment instead.
            op = finish_and_get_new();
        } else if (_buffer.empty()) {
            new_buffer(s);
        } else if (s > (_buffer.size() - _buf_pos)) { // enough data?
            _needed_size += s; // hint to next new_buffer, in case we are not first.
            if (_segment_manager->cfg.mode == sync_mode::BATCH) {
                // TODO: this could cause starvation if we're really unlucky.
                // If we run batch mode and find ourselves not fit in a non-empty
                // buffer, we must force a cycle and wait for it (to keep flush order)
                // This will most likely cause parallel writes, and consecutive flushes.
                op = cycle(true);
            } else {
                op = maybe_wait_for_write(cycle());
            }
        }

        if (op) {
            return op->then([writer = std::move(writer)] (sseg_ptr new_seg) mutable {
                return new_seg->allocate(std::move(writer));
            });
        }

        _gate.enter(); // this might throw. I guess we accept this?

        replay_position rp(_desc.id, position());
        uint64_t write_order_id = _parent_journal->register_write_context(rp);

        auto pos = _buf_pos;
        _buf_pos += s;
        update_dirty_pos(rp.pos);

        auto * p = _buffer.get_write() + pos;
        auto * e = _buffer.get_write() + pos + s - sizeof(uint32_t);

        data_output out(p, e);
        crc32_nbo crc;

        out.write(uint32_t(s));
        crc.process(uint32_t(s));
        out.write(crc.checksum());

        // actual data
        writer->write(*this, out);

        crc.process_bytes(p + 2 * sizeof(uint32_t), size);

        out = data_output(e, sizeof(uint32_t));
        out.write(crc.checksum());

        ++_segment_manager->totals.allocation_count;
        ++_num_allocs;
        _gate.leave();

        if (_segment_manager->cfg.mode == sync_mode::BATCH) {
            return batch_cycle().then_wrapped([this, write_order_id, rp](auto f) {
                try {
                    f.get(); 
                } catch(...){
                    std::ostringstream out;
                    out << "[allocate] error, rp:" << rp << ", write_order_id:" << write_order_id;
                    out << ", exception:" << std::current_exception();
                    sstring error_info = out.str();
                    _parent_journal->mark_write_context_exception(write_order_id, error_info);
                }
                //if has exception will solved by the after flow 
                return make_ready_future<uint64_t>(write_order_id);
            });
        }
       
        return make_ready_future<uint64_t>(write_order_id);
    }

    position_type position() const {
        return position_type(_file_pos + _buf_pos);
    }
    
    size_t size_on_disk() const {
        return _file_pos;
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    // a.k.a. zero the tail.
    size_t clear_buffer_slack() {
        auto size = align_up(_buf_pos, alignment);
        std::fill(_buffer.get_write() + _buf_pos, _buffer.get_write() + size,
                0);
        _segment_manager->totals.bytes_slack += (size - _buf_pos);
        return size;
    }
    
    void update_dirty_pos(position_type pos){
        _dirty_pos = pos;
        _can_delete = false;
    }

    void mark_clean(position_type pos) {
        if (_dirty_pos <= pos) {
            _can_delete = true;
        }
    }
    void mark_clean(const replay_position& pos) {
        if (pos.id == _desc.id) {
            mark_clean(pos.pos);
        } else if (pos.id > _desc.id) {
            mark_clean(std::numeric_limits<position_type>::max());
        }
    }
    void mark_clean() {
        _can_delete = true;
    }
    bool is_still_allocating() const {
        return !_closed && position() < _segment_manager->max_size;
    }
    bool is_clean() const {
        return _can_delete;
    }
    bool is_unused() const {
        return !is_still_allocating() && is_clean();
    }
    bool is_flushed() const {
        return position() <= _flush_pos;
    }
    bool can_delete() const {
        return is_unused() && is_flushed();
    }
    bool contains(const replay_position& pos) const {
        return pos.id == _desc.id;
    }
    sstring get_segment_name() const {
        return _desc.filename();
    }
}; //class hive_commitlog::segment

const size_t hive_commitlog::segment::default_size;


future<std::vector<hive_commitlog::descriptor>>
hive_commitlog::segment_manager::list_descriptors(sstring dirname) {
    struct helper {
        sstring _dirname;
        file _file;
        subscription<directory_entry> _list;
        std::vector<hive_commitlog::descriptor> _result;

        helper(helper&&) = default;
        helper(sstring n, file && f)
                : _dirname(std::move(n)), _file(std::move(f)), _list(
                        _file.list_directory(
                                std::bind(&helper::process, this,
                                        std::placeholders::_1))) {
        }

        future<> process(directory_entry de) {
            auto entry_type = [this](const directory_entry & de) {
                if (!de.type && !de.name.empty()) {
                    return engine().file_type(_dirname + "/" + de.name);
                }
                return make_ready_future<std::experimental::optional<directory_entry_type>>(de.type);
            };
            return entry_type(de).then([this, de](std::experimental::optional<directory_entry_type> type) {
                if (type == directory_entry_type::regular && de.name[0] != '.' && !is_cassandra_segment(de.name)) {
                    try {
                        _result.emplace_back(de.name);
                    } catch (std::domain_error& e) {
                        logger.warn(e.what());
                    }
                }
                return make_ready_future<>();
            });
        }

        future<> done() {
            return _list.done();
        }

        static bool is_cassandra_segment(sstring name) {
            // We want to ignore commitlog segments generated by Cassandra-derived tools (#1112)
            auto c = sstring("Cassandra");
            if (name.size() < c.size()) {
                return false;
            }
            return name.substr(0, c.size()) == c;
        }
    };

    return open_checked_directory(commit_error, dirname).then([this, dirname](file dir) {
        auto h = make_lw_shared<helper>(std::move(dirname), std::move(dir));
        return h->done().then([h]() {
            return make_ready_future<std::vector<hive_commitlog::descriptor>>(std::move(h->_result));
        }).finally([h] {});
    });
}

future<> hive_commitlog::segment_manager::init() {
    return list_descriptors(cfg.commit_log_location).then([this](std::vector<descriptor> descs) {
        segment_id_type id = std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count() + 1;
        for (auto& d : descs) {
            id = std::max(id, replay_position(d.id).base_id());
        }

        // base id counter is [ <shard> | <base> ]
        _ids = replay_position(engine().cpu_id(), id).id;
        // always run the timer now, since we need to handle segment pre-alloc etc as well.
        _timer.set_callback(std::bind(&segment_manager::on_timer, this));
        auto delay = engine().cpu_id() * std::ceil(double(cfg.commitlog_sync_period_in_ms) / smp::count);
        logger.trace("Delaying timer loop {} ms", delay);
        this->arm(delay);
    });
}

scollectd::registrations hive_commitlog::segment_manager::create_counters() {
    using scollectd::add_polled_metric;
    using scollectd::make_typed;
    using scollectd::type_instance_id;
    using scollectd::per_cpu_plugin_instance;
    using scollectd::data_type;

    return {
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "segments")
                , make_typed(data_type::GAUGE
                        , std::bind(&decltype(_segments)::size, &_segments))
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "allocating_segments")
                , make_typed(data_type::GAUGE
                        , [this]() {
                            return std::count_if(_segments.begin(), _segments.end(), [](const sseg_ptr & s) {
                                        return s->is_still_allocating();
                                    });
                        })
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "unused_segments")
                , make_typed(data_type::GAUGE
                        , [this]() {
                            return std::count_if(_segments.begin(), _segments.end(), [](const sseg_ptr & s) {
                                        return s->is_unused();
                                    });
                        })
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "alloc")
                , make_typed(data_type::DERIVE, totals.allocation_count)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "cycle")
                , make_typed(data_type::DERIVE, totals.cycle_count)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "flush")
                , make_typed(data_type::DERIVE, totals.flush_count)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_bytes", "written")
                , make_typed(data_type::DERIVE, totals.bytes_written)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_bytes", "slack")
                , make_typed(data_type::DERIVE, totals.bytes_slack)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "pending_writes")
                , make_typed(data_type::GAUGE, totals.pending_writes)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "queue_length", "pending_flushes")
                , make_typed(data_type::GAUGE, totals.pending_flushes)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "write_limit_exceeded")
                , make_typed(data_type::DERIVE, totals.write_limit_exceeded)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "total_operations", "flush_limit_exceeded")
                , make_typed(data_type::DERIVE, totals.flush_limit_exceeded)
        ),

        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "memory", "total_size")
                , make_typed(data_type::GAUGE, totals.total_size)
        ),
        add_polled_metric(type_instance_id("commitlog"
                        , per_cpu_plugin_instance, "memory", "buffer_list_bytes")
                , make_typed(data_type::GAUGE, totals.buffer_list_bytes)
        ),
    };
}
//tododl:yellow this function can delete for us 
void hive_commitlog::segment_manager::flush_segments(bool force) {
    if (_segments.empty()) {
        return;
    }
    // defensive copy.
    auto callbacks = boost::copy_range<std::vector<flush_handler>>(_flush_handlers | boost::adaptors::map_values);
    auto& active = _segments.back();

    // RP at "start" of segment we leave untouched.
    replay_position high(active->_desc.id, 0);

    // But if all segments are closed or we force-flush,
    // include all.
    if (force || !active->is_still_allocating()) {
        high = replay_position(high.id + 1, 0);
    }

    // Now get a set of used CF ids:
    //std::unordered_set<cf_id_type> ids;
    //std::for_each(_segments.begin(), _segments.end() - 1, [&ids](sseg_ptr& s) {
    //    for (auto& id : s->_cf_dirty | boost::adaptors::map_keys) {
    //        ids.insert(id);
    //    }
    //});

    logger.debug("Flushing ({}) to {}", force, high);

    // For each CF id: for each callback c: call c(id, high)
    for (auto& f : callbacks) {
       try {
           f(high);
       } catch (...) {
           logger.error("Exception during flush request {}/{}: {}",_commitlog_id, high, std::current_exception());
       }
    }
}

future<hive_commitlog::segment_manager::sseg_ptr> hive_commitlog::segment_manager::allocate_segment(bool active) {
    descriptor d(_commitlog_id, next_id());
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    return open_checked_file_dma(commit_error, cfg.commit_log_location + "/" + d.filename(), open_flags::wo | open_flags::create, opt)
            .then([this, d, active](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        return f.truncate(max_size).then([this, d, active, f] () mutable {
            auto cycle_sleep_in_us = this->cfg.commitlog_cycle_sleep_in_us;
            auto s = make_lw_shared<segment>(this->_parent_journal, _volume_id, _commitlog_id, this->shared_from_this()
                , d, std::move(f), active, cycle_sleep_in_us);
            return make_ready_future<sseg_ptr>(s);
        });
    });
}

future<hive_commitlog::segment_manager::sseg_ptr> hive_commitlog::segment_manager::new_segment() {
    if (_shutdown) {
        throw std::runtime_error("Commitlog has been shut down. Cannot add data");
    }

    return with_lock(_create_segment_mutex, [this](){
        ++_new_counter;
        
        if (_reserve_segments.empty()) {
            if (_num_reserve_segments < cfg.max_reserve_segments) {
                ++_num_reserve_segments;
                logger.trace("Increased segment reserve count to {}", _num_reserve_segments);
            }
            return allocate_segment(true).then([this](sseg_ptr s) {
                _segments.push_back(s);
                return make_ready_future<sseg_ptr>(s);
            });
        }

        _segments.push_back(_reserve_segments.front());
        _reserve_segments.pop_front();
        _segments.back()->reset_sync_time();
        logger.trace("Acquired segment {} from reserve", _segments.back());
        return make_ready_future<sseg_ptr>(_segments.back());
    });
}

future<hive_commitlog::segment_manager::sseg_ptr> hive_commitlog::segment_manager::active_segment() {
    if (_segments.empty() || !_segments.back()->is_still_allocating()) {
        return _new_segment_semaphore.wait().then([this]() {
            if (_segments.empty() || !_segments.back()->is_still_allocating()) {
                return new_segment();
            }
            return make_ready_future<sseg_ptr>(_segments.back());
        }).finally([this]() {
            _new_segment_semaphore.signal();
        });
    }
    return make_ready_future<sseg_ptr>(_segments.back());
}

/**
 * go through all segments, clear id up to pos. if segment becomes clean and unused by this,
 * it is discarded.
 */
void hive_commitlog::segment_manager::discard_completed_segments(const replay_position& pos) {
    logger.debug("Discard completed segments for {}", pos);
    for (auto&s : _segments) {
        s->mark_clean(pos);
    }
    discard_unused_segments();
}

void hive_commitlog::segment_manager::discard_unused_segments() {
    logger.trace("Checking for unused segments ({} active)", _segments.size());

    auto i = std::remove_if(_segments.begin(), _segments.end(), [=](sseg_ptr s) {
        if (s->can_delete()) {
            logger.debug("Segment {} is unused", *s);
            return true;
        }
        if (s->is_still_allocating()) {
            logger.debug("Not safe to delete segment {}; still allocating.", s);
        } else if (!s->is_clean()) {
            logger.debug("Not safe to delete segment {}; dirty is {}", s, segment::cf_mark {*s});
        } else {
            logger.debug("Not safe to delete segment {}; disk ops pending", s);
        }
        return false;
    });
    if (i != _segments.end()) {
        _segments.erase(i, _segments.end());
    }
}

future<> hive_commitlog::segment_manager::sync_all_segments(bool shutdown) {
    logger.debug("Issuing sync for all segments");
    return parallel_for_each(_segments, [this, shutdown](sseg_ptr s) {
        return s->sync(shutdown).then([](sseg_ptr s) {
            logger.debug("Synced segment {}", *s);
        });
    });
}

future<> hive_commitlog::segment_manager::shutdown() {
    if (!_shutdown) {
        _shutdown = true; // no re-arm, no create new segments.
        _timer.cancel(); // no more timer calls
        // Now first wait for periodic task to finish, then sync and close all
        // segments, flushing out any remaining data.
        return _gate.close().then(std::bind(&segment_manager::sync_all_segments, this, true));
    }
    return make_ready_future<>();
}

void hive_commitlog::segment_manager::orphan_all() {
    _segments.clear();
    _reserve_segments.clear();
}

/*
 * Sync all segments, then clear them out. To ensure all ops are done.
 * (Assumes you have barriered adding ops!)
 * Only use from tests.
 */
future<> hive_commitlog::segment_manager::clear() {
    logger.debug("Clearing commitlog");
    return shutdown().then([this] {
        logger.debug("Clearing all segments");
        for (auto& s : _segments) {
            s->mark_clean();
        }
        orphan_all();
    });
}
/**
 * Called by timer in periodic mode.
 */
void hive_commitlog::segment_manager::sync() {
    for (auto s : _segments) {
        s->sync(); // we do not care about waiting...
    }
}

void hive_commitlog::segment_manager::on_timer() {
    // Gate, because we are starting potentially blocking ops
    // without waiting for them, so segement_manager could be shut down
    // while they are running.
    seastar::with_gate(_gate, [this] {
        if (cfg.mode != sync_mode::BATCH) {
            sync();
        }
        // IFF a new segment was put in use since last we checked, and we're
        // above threshold, request flush.
        if (_new_counter > 0) {
            auto max = max_disk_size;
            auto cur = totals.total_size_on_disk;
            if (max != 0 && cur >= max) {
                _new_counter = 0;
                logger.debug("Size on disk {} MB exceeds local maximum {} MB", cur / (1024 * 1024), max / (1024 * 1024));
                flush_segments();
            }
        }
        // take outstanding allocations into regard. This is paranoid,
        // but if for some reason the file::open takes longer than timer period,
        // we could flood the reserve list with new segments
        //
        // #482 - _reserve_allocating is decremented in the finally clause below.
        // This is needed because if either allocate_segment _or_ emplacing into
        // _reserve_segments should throw, we still need the counter reset
        // However, because of this, it might be that emplace was done, but not decrement,
        // when we get here again. So occasionally we might get a sum of the two that is
        // not consistent. It should however always just potentially be _to much_, i.e.
        // just an indicator that we don't need to do anything. So lets do that.
        auto n = std::min(_reserve_segments.size() + _reserve_allocating, _num_reserve_segments);
        return parallel_for_each(boost::irange(n, _num_reserve_segments), [this, n](auto i) {
            return with_lock(_create_segment_mutex, [this, n, i](){
                ++_reserve_allocating;
                return this->allocate_segment(false).then([this](sseg_ptr s) {
                    if (!_shutdown) {
                        // insertion sort.
                        auto i = std::upper_bound(_reserve_segments.begin(), _reserve_segments.end(), s, [](sseg_ptr s1, sseg_ptr s2) {
                            const descriptor& d1 = s1->_desc;
                            const descriptor& d2 = s2->_desc;
                            return d1.id < d2.id;
                        });
                        i = _reserve_segments.emplace(i, std::move(s));
                        logger.trace("Added reserve segment {}", *i);
                    }
                }).finally([this] {
                    --_reserve_allocating;
                });
            });
        });
    }).handle_exception([](std::exception_ptr ep) {
        logger.warn("Exception in segment reservation: {}", ep);
    });
    arm();
}

std::vector<sstring> hive_commitlog::segment_manager::get_active_names() const {
    std::vector<sstring> res;
    for (auto i: _segments) {
        if (!i->is_unused()) {
            // Each shared is located in its own directory
            res.push_back(cfg.commit_log_location + "/" + i->get_segment_name());
        }
    }
    return res;
}

uint64_t hive_commitlog::segment_manager::get_num_dirty_segments() const {
    return std::count_if(_segments.begin(), _segments.end(), [](sseg_ptr s) {
        return !s->is_still_allocating() && !s->is_clean();
    });
}

uint64_t hive_commitlog::segment_manager::get_num_active_segments() const {
    return std::count_if(_segments.begin(), _segments.end(), [](sseg_ptr s) {
        return s->is_still_allocating();
    });
}


hive_commitlog::segment_manager::buffer_type hive_commitlog::segment_manager::acquire_buffer(size_t s) {
    auto i = _temp_buffers.begin();
    auto e = _temp_buffers.end();

    while (i != e) {
        if (i->size() >= s) {
            auto r = std::move(*i);
            _temp_buffers.erase(i);
            totals.buffer_list_bytes -= r.size();
            return r;
        }
        ++i;
    }
    auto a = ::memalign(segment::alignment, s);
    if (a == nullptr) {
        throw std::bad_alloc();
    }
    logger.trace("Allocated {} k buffer", s / 1024);
    return buffer_type(reinterpret_cast<char *>(a), s, make_free_deleter(a));
}

void hive_commitlog::segment_manager::release_buffer(buffer_type&& b) {
    _temp_buffers.emplace_back(std::move(b));
    std::sort(_temp_buffers.begin(), _temp_buffers.end(), [](const buffer_type& b1, const buffer_type& b2) {
        return b1.size() < b2.size();
    });

    constexpr const size_t max_temp_buffers = 4;

    if (_temp_buffers.size() > max_temp_buffers) {
        logger.trace("Deleting {} buffers", _temp_buffers.size() - max_temp_buffers);
        _temp_buffers.erase(_temp_buffers.begin() + max_temp_buffers, _temp_buffers.end());
    }
    totals.buffer_list_bytes = boost::accumulate(
	    _temp_buffers | boost::adaptors::transformed(std::mem_fn(&buffer_type::size)),
            size_t(0), std::plus<size_t>());
}

future<uint64_t> hive_commitlog::add_entry(const hive_commitlog_entry_writer& cew)
{
    assert(_parent_journal); //ensure _parent_journal not nullptr, because it will be used after
    class cl_entry_writer final : public entry_writer {
        hive_commitlog_entry_writer _writer;
    public:
        cl_entry_writer(const hive_commitlog_entry_writer& wr) : _writer(wr) { }
        virtual size_t size(segment& seg) override {
            _writer.set_with_schema(!seg.is_schema_version_known(_writer.schema()));
            return _writer.size();
        }
        virtual void write(segment& seg, output& out) override {
            if (_writer.with_schema()) {
                seg.add_schema_version(_writer.schema());
            }
            _writer.write(out);
        }
    };
    auto writer = ::make_shared<cl_entry_writer>(cew);
    return _segment_manager->active_segment().then([writer] (auto s) {
        return s->allocate(writer);
    });
}

hive_commitlog::hive_commitlog(sstring volume_id, sstring commitlog_id, db::config cfg, sstring commitlog_location)
        : _replay_position_log(::make_shared<replay_position_log>(volume_id, cfg)) 
        , _volume_id(volume_id)
        , _segment_manager(::make_shared<segment_manager>(commitlog_id, volume_id, std::move(cfg), commitlog_location)) 
        , _commitlog_id(commitlog_id){
}

hive_commitlog::hive_commitlog(hive_commitlog&& v) noexcept
        : _replay_position_log(v._replay_position_log)
        , _volume_id(v._volume_id)
        , _segment_manager(std::move(v._segment_manager))
        , _commitlog_id(v._commitlog_id){
}

hive_commitlog::~hive_commitlog() {
    if (_segment_manager != nullptr) {
        _segment_manager->orphan_all();
    }
}

void hive_commitlog::set_parent_journal(primary_journal* parent_journal){
    assert(parent_journal);
    _parent_journal = parent_journal;
    _segment_manager->set_parent_journal(parent_journal);
}

future<hive_commitlog> hive_commitlog::create_commitlog(sstring volume_id
                                                      , sstring commitlog_id
                                                      , db::config cfg
                                                      , sstring commitlog_location) {
    hive_commitlog c(volume_id, commitlog_id, std::move(cfg), commitlog_location);
    auto f = c._segment_manager->init();
    return f.then([c = std::move(c)]() mutable {
        return make_ready_future<hive_commitlog>(std::move(c));
    });
}

hive_commitlog::flush_handler_anchor::flush_handler_anchor(flush_handler_anchor&& f)
    : _cl(f._cl), _id(f._id)
{
    f._id = 0;
}

hive_commitlog::flush_handler_anchor::flush_handler_anchor(hive_commitlog& cl, flush_handler_id id)
    : _cl(cl), _id(id)
{}

hive_commitlog::flush_handler_anchor::~flush_handler_anchor() {
    unregister();
}

hive_commitlog::flush_handler_id hive_commitlog::flush_handler_anchor::release() {
    flush_handler_id id = 0;
    std::swap(_id, id);
    return id;
}

void hive_commitlog::flush_handler_anchor::unregister() {
    auto id = release();
    if (id != 0) {
        _cl.remove_flush_handler(id);
    }
}

hive_commitlog::flush_handler_anchor hive_commitlog::add_flush_handler(flush_handler h) {
    return flush_handler_anchor(*this, _segment_manager->add_flush_handler(std::move(h)));
}

void hive_commitlog::remove_flush_handler(flush_handler_id id) {
    _segment_manager->remove_flush_handler(id);
}

void hive_commitlog::discard_completed_segments(const replay_position& pos) {
    _segment_manager->discard_completed_segments(pos);
}

future<> hive_commitlog::sync_all_segments() {
    return _segment_manager->sync_all_segments();
}

future<> hive_commitlog::shutdown() {
    return _segment_manager->shutdown();
}

size_t hive_commitlog::max_record_size() const {
    return _segment_manager->max_mutation_size - segment::entry_overhead_size;
}

uint64_t hive_commitlog::max_active_writes() const {
    return _segment_manager->cfg.max_active_writes;
}

uint64_t hive_commitlog::max_active_flushes() const {
    return _segment_manager->cfg.max_active_flushes;
}

future<> hive_commitlog::clear() {
    return _segment_manager->clear();
}

const hive_commitlog::config& hive_commitlog::active_config() const {
    return _segment_manager->cfg;
}

// No commit_io_check needed in the log reader since the database will fail
// on error at startup if required
future<std::unique_ptr<subscription<temporary_buffer<char>, replay_position>>>
hive_commitlog::read_log_file(const sstring& filename, commit_load_reader_func next, position_type off) {
    return open_checked_file_dma(commit_error, filename, open_flags::ro).then([next = std::move(next), off](file f) {
       return std::make_unique<subscription<temporary_buffer<char>, replay_position>>(
           read_log_file(std::move(f), std::move(next), off));
    });
}

// No commit_io_check needed in the log reader since the database will fail
// on error at startup if required
subscription<temporary_buffer<char>, replay_position>
hive_commitlog::read_log_file(file f, commit_load_reader_func next, position_type off) {
    struct work {
        file f;
        stream<temporary_buffer<char>, replay_position> s;
        input_stream<char> fin;
        input_stream<char> r;
        uint64_t id = 0;
        size_t pos = 0;
        size_t next = 0;
        size_t start_off = 0;
        size_t skip_to = 0;
        size_t file_size = 0;
        size_t corrupt_size = 0;
        bool eof = false;
        bool header = true;
        bool failed = false;
        bool already_set_exception = false;

        work(file f, position_type o = 0)
                : f(f), fin(make_file_input_stream(f)), start_off(o) {
        }
        work(work&&) = default;

        bool advance(const temporary_buffer<char>& buf) {
            pos += buf.size();
            if (buf.size() == 0) {
                eof = true;
            }
            return !eof;
        }
        bool end_of_file() const {
            return eof;
        }
        bool end_of_chunk() const {
            return eof || next == pos;
        }
        future<> skip(size_t bytes) {
            skip_to = pos + bytes;
            return do_until([this] { return pos == skip_to || eof; }, [this, bytes] {
                auto s = std::min<size_t>(4096, skip_to - pos);
                // should eof be an error here?
                return fin.read_exactly(s).then([this](auto buf) {
                    this->advance(buf);
                });
            });
        }
        future<> stop() {
            eof = true;
            return make_ready_future<>();
        }
        
        future<> fail(){
            failed = true;
            return stop();
        }

        future<> read_header() {
            return fin.read_exactly(segment::descriptor_header_size).then([this](temporary_buffer<char> buf) {
                if (!advance(buf)) {
                    // zero length file. accept it just to be nice.
                    return make_ready_future<>();
                }
                // Will throw if we got eof
                data_input in(buf);
                auto magic = in.read<uint32_t>();
                auto ver = in.read<uint32_t>();
                auto id = in.read<uint64_t>();
                auto checksum = in.read<uint32_t>();

                if (magic == 0 && ver == 0 && id == 0 && checksum == 0) {
                    // let's assume this was an empty (pre-allocated)
                    // file. just skip it.
                    return stop();
                }

                if (magic != segment::segment_magic) {
                    throw std::invalid_argument("Not a scylla format commitlog file");
                }
                crc32_nbo crc;
                crc.process(ver);
                crc.process<int32_t>(id & 0xffffffff);
                crc.process<int32_t>(id >> 32);

                auto cs = crc.checksum();
                if (cs != checksum) {
                    throw std::runtime_error("Checksum error in file header");
                }

                this->id = id;
                this->next = 0;

                return make_ready_future<>();
            });
        } //read_header

        future<> read_chunk() {
            return fin.read_exactly(segment::segment_overhead_size).then([this](temporary_buffer<char> buf) {
                auto start = pos;

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                data_input in(buf);
                auto next = in.read<uint32_t>();
                auto checksum = in.read<uint32_t>();

                if (next == 0 && checksum == 0) {
                    // in a pre-allocating world, this means eof
                    return stop();
                }

                crc32_nbo crc;
                crc.process<int32_t>(id & 0xffffffff);
                crc.process<int32_t>(id >> 32);
                crc.process<uint32_t>(start);

                auto cs = crc.checksum();
                if (cs != checksum) {
                    // if a chunk header checksum is broken, we shall just assume that all
                    // remaining is as well. We cannot trust the "next" pointer, so...
                    logger.debug("Checksum error in segment chunk at {}.", pos);
                    corrupt_size += (file_size - pos);
                    return stop();
                }

                this->next = next;

                if (start_off >= next) {
                    return skip(next - pos);
                }

                return do_until(std::bind(&work::end_of_chunk, this), std::bind(&work::read_entry, this));
            });
        }//read_chunk

        future<> read_entry() {
            static constexpr size_t entry_header_size = segment::entry_overhead_size - sizeof(uint32_t);

            /**
             * #598 - Must check that data left in chunk is enough to even read an entry.
             * If not, this is small slack space in the chunk end, and we should just go
             * to the next.
             */
            assert(pos <= next);
            if ((pos + entry_header_size) >= next) {
                return skip(next - pos);
            }

            return fin.read_exactly(entry_header_size).then([this](temporary_buffer<char> buf) {
                replay_position rp(id, position_type(pos));

                if (!advance(buf)) {
                    return make_ready_future<>();
                }

                data_input in(buf);

                auto size = in.read<uint32_t>();
                auto checksum = in.read<uint32_t>();

                crc32_nbo crc;
                crc.process(size);

                if (size < 3 * sizeof(uint32_t) || checksum != crc.checksum()) {
                    auto slack = next - pos;
                    if (size != 0) {
                        logger.debug("Segment entry at {} has broken header. Skipping to next chunk ({} bytes)", rp, slack);
                        corrupt_size += slack;
                    }
                    // size == 0 -> special scylla case: zero padding due to dma blocks
                    return skip(slack);
                }

                return fin.read_exactly(size - entry_header_size).then(
                        [this, size, crc = std::move(crc), rp](temporary_buffer<char> buf) mutable {
                    advance(buf);

                    data_input in(buf);

                    auto data_size = size - segment::entry_overhead_size;
                    in.skip(data_size);
                    auto checksum = in.read<uint32_t>();

                    crc.process_bytes(buf.get(), data_size);

                    if (crc.checksum() != checksum) {
                        // If we're getting a checksum error here, most likely the rest of
                        // the file will be corrupt as well. But it does not hurt to retry.
                        // Just go to the next entry (since "size" in header seemed ok).
                        logger.debug("Segment entry at {} checksum error. Skipping {} bytes", rp, size);
                        corrupt_size += size;
                        return make_ready_future<>();
                    }
                    return s.produce(buf.share(0, data_size), rp).handle_exception([this](auto ep){
                        return this->fail();
                    });

                });
            });
        } //read_entry

        future<> read_file() {
            return f.size().then([this](uint64_t size) {
                file_size = size;
            }).then([this] {
                return read_header().then(
                        [this] {
                            return do_until(std::bind(&work::end_of_file, this), std::bind(&work::read_chunk, this));
                }).then([this] {
                  if (corrupt_size > 0) {
                      throw segment_data_corruption_error("Data corruption", corrupt_size);
                  }
                });
            });
        }
    }; //struct work

    auto w = make_lw_shared<work>(std::move(f), off);
    auto ret = w->s.listen(std::move(next));
    w->s.started().then(std::bind(&work::read_file, w.get())).then([w] {
        if(!w->failed){
            w->s.close();
        }
    }).handle_exception([w](auto ep) {
            w->s.set_exception(ep);
    });

    return ret;
} 

std::vector<sstring> hive_commitlog::get_active_segment_names() const {
    return _segment_manager->get_active_names();
}

uint64_t hive_commitlog::get_total_size() const {
    return _segment_manager->totals.total_size;
}

uint64_t hive_commitlog::get_completed_tasks() const {
    return _segment_manager->totals.allocation_count;
}

uint64_t hive_commitlog::get_flush_count() const {
    return _segment_manager->totals.flush_count;
}

uint64_t hive_commitlog::get_pending_tasks() const {
    return _segment_manager->totals.pending_writes
                    + _segment_manager->totals.pending_flushes;
}

uint64_t hive_commitlog::get_pending_writes() const {
    return _segment_manager->totals.pending_writes;
}

uint64_t hive_commitlog::get_pending_flushes() const {
    return _segment_manager->totals.pending_flushes;
}

uint64_t hive_commitlog::get_pending_allocations() const {
    return _segment_manager->totals.pending_allocations;
}

uint64_t hive_commitlog::get_write_limit_exceeded_count() const {
    return _segment_manager->totals.write_limit_exceeded;
}

uint64_t hive_commitlog::get_flush_limit_exceeded_count() const {
    return _segment_manager->totals.flush_limit_exceeded;
}

uint64_t hive_commitlog::get_num_segments_created() const {
    return _segment_manager->totals.segments_created;
}

uint64_t hive_commitlog::get_num_segments_destroyed() const {
    return _segment_manager->totals.segments_destroyed;
}

uint64_t hive_commitlog::get_num_dirty_segments() const {
    return _segment_manager->get_num_dirty_segments();
}

uint64_t hive_commitlog::get_num_active_segments() const {
    return _segment_manager->get_num_active_segments();
}

future<std::vector<hive_commitlog::descriptor>> hive_commitlog::list_existing_descriptors() const {
    return list_existing_descriptors(active_config().commit_log_location);
}

future<std::vector<hive_commitlog::descriptor>> hive_commitlog::list_existing_descriptors(const sstring& dir) const {
    return _segment_manager->list_descriptors(dir);
}

future<std::vector<sstring>> hive_commitlog::list_existing_segments() const {
    return list_existing_segments(active_config().commit_log_location);
}

future<std::vector<sstring>> hive_commitlog::list_existing_segments(const sstring& dir) const {
    return list_existing_descriptors(dir).then([dir](auto descs) {
        std::vector<sstring> paths;
        std::transform(descs.begin(), descs.end(), std::back_inserter(paths), [&](auto& d) {
           return dir + "/" + d.filename();
        });
        return make_ready_future<std::vector<sstring>>(std::move(paths));
    });
}

sstring hive_commitlog::_get_commitlog_id(){
    return _commitlog_id;
}

std::ostream& operator<<(std::ostream& out, const hive_commitlog::segment& s) {
    return out << s._desc.filename();
}

std::ostream& operator<<(std::ostream& out, const hive_commitlog::segment::cf_mark& m) {
    return out << m.s._commitlog_id;
}



} //namespace hive







