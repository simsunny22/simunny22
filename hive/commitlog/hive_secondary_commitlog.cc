#include "hive/commitlog/hive_secondary_commitlog.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/hive_tools.hh"
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
#include <core/fstream.hh>
#include "core/shared_mutex.hh"
#include <seastar/core/memory.hh>
#include <net/byteorder.hh>

#include "db/config.hh"
#include "utils/data_input.hh"
#include "utils/crc.hh"
#include "utils/runtime.hh"
#include "utils/flush_queue.hh"
#include "log.hh"
#include "service/priority_manager.hh"

#include <boost/range/numeric.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "checked-file-impl.hh"
#include "disk-error-handler.hh"

static logging::logger logger("hive_secondary_commitlog");

namespace hive {

hive_secondary_commitlog::config::config(const db::config& cfg, sstring commitlog_location)
    : commit_log_location(commitlog_location)
    , commitlog_total_space_in_mb(cfg.commitlog_total_space_in_mb() >= 0 ? cfg.commitlog_total_space_in_mb() : memory::stats().total_memory() >> 20)
    , commitlog_segment_size_in_mb(cfg.commitlog_segment_size_in_mb())
{}

hive_secondary_commitlog::descriptor::descriptor(sstring commitlog_id, segment_id_type i, uint32_t v)
        : commitlog_id(commitlog_id), id(i), ver(v) {
}

hive_secondary_commitlog::descriptor::descriptor(sstring commitlog_id, replay_position p)
        : descriptor(commitlog_id, p.id) {
}

hive_secondary_commitlog::descriptor::descriptor(sstring commitlog_id, std::pair<uint64_t, uint32_t> p)
        : descriptor(commitlog_id, p.first, p.second) {
}

hive_secondary_commitlog::descriptor::descriptor(sstring filename) {
    std::smatch m;
    // match both legacy and new version of commitlogs Ex: CommitLog-$commitlog_id-4-12345.log.
    std::regex rx("(?:.*/)?" + FILENAME_PREFIX +"([a-zA-Z0-9_]+[_][0-9]+)" + SEPARATOR + "((\\d+)(" + SEPARATOR + "\\d+)?)" + FILENAME_EXTENSION);
    std::string sfilename = filename;
    if (!std::regex_match(sfilename, m, rx)) {
        throw std::domain_error("Cannot parse the version of the file: " + filename);
    }
    if (m[4].length() == 0) {
        // CMH. Can most likely ignore this
        throw std::domain_error("Commitlog segment is too old to open; upgrade to 1.2.5+ first");
    }
    
    id = std::stoull(m[4].str().substr(1));
    ver = std::stoul(m[3].str());
    commitlog_id = m[1].str();
}

sstring hive_secondary_commitlog::descriptor::filename()const{
    return FILENAME_PREFIX + commitlog_id.c_str() + SEPARATOR + std::to_string(ver) + SEPARATOR
            + std::to_string(id) + FILENAME_EXTENSION;
}

hive_secondary_commitlog::descriptor::operator replay_position() const {
    return replay_position(id);
}

const std::string hive_secondary_commitlog::descriptor::SEPARATOR("-");
const std::string hive_secondary_commitlog::descriptor::FILENAME_PREFIX(
        "CommitLog" + SEPARATOR);
const std::string hive_secondary_commitlog::descriptor::FILENAME_EXTENSION(".log");

class hive_secondary_commitlog::segment_manager : public ::enable_shared_from_this<segment_manager> {
public:
    sstring _commitlog_id;
    config cfg;
    const uint64_t max_size;
    const uint64_t max_disk_size; // per-shard

    bool _shutdown = false;

    semaphore _new_segment_semaphore;
    semaphore _write_semaphore;

    // TODO: verify that we're ok with not-so-great granularity
    using clock_type = lowres_clock;
    using time_point = clock_type::time_point;
    using sseg_ptr = lw_shared_ptr<segment>;

    struct stats {
        uint64_t allocation_count = 0;
        uint64_t bytes_written = 0;
        uint64_t segments_created = 0;
        uint64_t segments_destroyed = 0;
        uint64_t pending_writes = 0;
        uint64_t pending_allocations = 0;
        uint64_t write_limit_exceeded = 0;
        uint64_t total_size_on_disk = 0;
    };

    stats totals;

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

    segment_manager(sstring commitlog_id, db::config c, sstring commitlog_location)
        : _commitlog_id(commitlog_id)
        ,cfg([&c, commitlog_location] {
            config cfg(c, commitlog_location);

            if (cfg.commit_log_location.empty()) {
                cfg.commit_log_location = "/var/lib/scylla/commitlog";
            }

            if (cfg.max_active_writes == 0) {
                cfg.max_active_writes = // TODO: call someone to get an idea...
                                25 * smp::count;
            }
            cfg.max_active_writes = std::max(uint64_t(1), cfg.max_active_writes / smp::count);

            return cfg;
        }())
        , max_size(std::min<size_t>(std::numeric_limits<position_type>::max()
                 , std::max<size_t>(cfg.commitlog_segment_size_in_mb, 1) * 1024 * 1024))
        , max_disk_size(size_t(std::ceil(cfg.commitlog_total_space_in_mb / double(smp::count))) * 1024 * 1024)
        , _new_segment_semaphore(1) 
        , _write_semaphore(cfg.max_active_writes)
    {
        assert(max_size > 0);

        logger.trace("Commitlog {} maximum disk size: {} MB / cpu ({} cpus)",
                cfg.commit_log_location, max_disk_size / (1024 * 1024),
                smp::count);
    }
    ~segment_manager() {
        logger.trace("Commitlog {} disposed", cfg.commit_log_location);
    }

    future<> init();
    future<sseg_ptr> new_segment(sstring segment_file_path);
    future<sseg_ptr> find_or_create_segment(sstring segment_file_path);
    future<sseg_ptr> allocate_segment(sstring segment_file_path);
    future<sseg_ptr> recover_segment(sstring segment_file_path);

    future<> clear();
    future<> shutdown();

    void discard_unused_segments();
    void discard_completed_segments(sstring commitlog_id, const replay_position& pos);

    std::vector<sstring> get_active_names() const;
    uint64_t get_num_dirty_segments() const;
    uint64_t get_num_active_segments() const;
    using buffer_type = temporary_buffer<char>;

    future<std::vector<descriptor>> list_descriptors(sstring dir);

private:
    std::vector<sseg_ptr> _segments;
};

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 *
 * The IO flow is somewhat convoluted and goes something like this:
 *
 * Mutation path:
 *  - Adding data to the segment usually writes into the internal buffer
 *  - On EOB or overflow we issue a write to disk ("cycle").
 *      - A cycle call will acquire the segment read lock and send the
 *        buffer to the corresponding position in the file
 *  - If we are periodic and crossed a timing threshold, or running "batch" mode
 *    we might be forced to issue a flush ("sync") after adding data
 *      - A sync call acquires the write lock, thus locking out writes
 *        and waiting for pending writes to finish. It then checks the
 *        high data mark, and issues the actual file flush.
 *        Note that the write lock is released prior to issuing the
 *        actual file flush, thus we are allowed to write data to
 *        after a flush point concurrently with a pending flush.
 *
 * Sync timer:
 *  - In periodic mode, we try to primarily issue sync calls in
 *    a timer task issued every N seconds. The timer does the same
 *    operation as the above described sync, and resets the timeout
 *    so that mutation path will not trigger syncs and delay.
 *
 * Note that we do not care which order segment chunks finish writing
 * to disk, other than all below a flush point must finish before flushing.
 *
 * We currently do not wait for flushes to finish before issueing the next
 * cycle call ("after" flush point in the file). This might not be optimal.
 *
 * To close and finish a segment, we first close the gate object that guards
 * writing data to it, then flush it fully (including waiting for futures create
 * by the timer to run their course), and finally wait for it to
 * become "clean", i.e. get notified that all mutations it holds have been
 * persisted to sstables elsewhere. Once this is done, we can delete the
 * segment. If a segment (object) is deleted without being fully clean, we
 * do not remove the file on disk.
 *
 */

class hive_secondary_commitlog::segment: public enable_lw_shared_from_this<segment> {
    ::shared_ptr<segment_manager> _segment_manager;

    descriptor _desc;
    file       _file;
    sstring    _file_name;
    uint64_t   _file_pos = 0;
    size_t     _needed_size = 0;
    seastar::shared_mutex _segment_lock; 

    using buffer_type = segment_manager::buffer_type;
    using sseg_ptr    = segment_manager::sseg_ptr;
    using clock_type  = segment_manager::clock_type;
    using time_point  = segment_manager::time_point;

    uint64_t  _write_waiters = 0;
    semaphore _queue;

    uint64_t _num_allocs = 0;
    bool     _can_delete = false;
    bool     _closed = true;
    
    semaphore _write_semaphore;

public:
    friend std::ostream& operator<<(std::ostream&, const segment&);
    friend class segment_manager;

    future<> begin_write() {
        //return _segment_manager->begin_write();
        return _write_semaphore.wait();
    }

    void end_write() {
        //_segment_manager->end_write();
        _write_semaphore.signal();
        _closed = true;
    }

    //struct cf_mark {
    //    const segment& s;
    //};
    //friend std::ostream& operator<<(std::ostream&, const cf_mark&);
    static constexpr size_t alignment = 4096;

    segment(::shared_ptr<segment_manager> m, const descriptor& d, file&& f)
            : _segment_manager(std::move(m))
            , _desc(std::move(d))
            , _file(std::move(f))
            , _file_name(_segment_manager->cfg.commit_log_location + "/" + _desc.filename())
            , _queue(0) 
            , _write_semaphore(1)
    {
        ++_segment_manager->totals.segments_created;
        logger.debug("Created new segment {}", *this);
    }
    ~segment() {
        if(can_delete()) {
            logger.debug("Segment {} is no longer active and will be deleted now", *this);
            ++_segment_manager->totals.segments_destroyed;
            _segment_manager->totals.total_size_on_disk -= size_on_disk();
            try {
                commit_io_check(::unlink, _file_name.c_str());
            } catch (...) {
                logger.error("Could not delete segment {}: {}", *this, std::current_exception());
            }
        } else {
           logger.warn("Segment {} is left on disk.", *this); 
        }
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
        return _write_waiters > 0;
    }

    future<sseg_ptr> wait_for_alloc() {
        auto me = shared_from_this();
        ++_segment_manager->totals.pending_allocations;
        logger.trace("Previous allocation is blocking. Must wait.");
        return _queue.wait().then([me] { // TODO: do we need a finally?
            --me->_segment_manager->totals.pending_allocations;
            return make_ready_future<sseg_ptr>(me);
        });
    }

    future<> allocate(const uint64_t off, const bytes& content) {
        return with_lock(_segment_lock, [this, off, &content](){
            _closed = false;
            const auto size = content.size();
            std::experimental::optional<future<sseg_ptr>> op;
            if(must_wait_for_alloc()){
                op = wait_for_alloc();
            }

            if (op) {
                return op->then([off, &content] (sseg_ptr new_seg) mutable {
                    return new_seg->allocate(off, content);
                });
            }
            _write_waiters++;
            return write_segment(size, off, content).finally([this]() {
                if (--_write_waiters == 0) {
                    _queue.signal(_queue.waiters());
                }
            });
        });

    }

    //write local and remote
    future<> write_segment(size_t size, uint64_t off, const bytes& content) {
        logger.debug("hive_secondary_commitlog, write_segment start !");
        return sync_locally(size, off, content).then_wrapped([](future<> f){
            try{
                f.get(); 
                logger.debug("hive_secondary_commitlog, write_segment done!");
                return make_ready_future<>();
            }catch(...){
                logger.error("[do_sync]error to write local remote commitlog, exception:{}", std::current_exception()); 
                throw;
            }
        });
    }

    future<> sync_locally(size_t size, uint64_t off, const bytes& content) {
        logger.debug("hive_secondary_commitlog, sync_locally start !");
        return begin_write().then([this, size, off, &content]() mutable {
            auto written = make_lw_shared<size_t>(0);
            auto p = content.begin();
            return repeat([this, size, off, written, p]() mutable {
                auto&& priority_class = service::get_local_commitlog_priority();
                return _file.dma_write(off + *written, p + *written, size - *written, priority_class).then_wrapped(
                    [this, size, written](future<size_t>&& f) {
                    try {
                        auto bytes = std::get<0>(f.get());
                        *written += bytes;
                        _segment_manager->totals.bytes_written += bytes;
                        _segment_manager->totals.total_size_on_disk += bytes;
                        if (*written == size) {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                        // gah, partial write. should always get here with dma chunk sized
                        // "bytes", but lets make sure...
                        logger.debug("Partial write {}: {}/{} bytes", *this, *written, size);
                        *written = align_down(*written, alignment);
                        logger.debug("hive_secondary_commitlog, dma_write done!");
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                        // TODO: retry/ignore/fail/stop - optional behaviour in origin.
                        // we fast-fail the whole commit.
                    } catch (...) {
                        logger.error("Failed to persist commits to disk for {}: {}", *this, std::current_exception());
                        throw;
                    }
               });
            });
        }).then([this](){
            return flush();
        }).then([this, off, size](){
            _file_pos = off + size; 
        }).finally([this]() {
            logger.debug("hive_secondary_commitlog, sync_locally done!");
            end_write(); // release
        });
    }

    future<> flush() {
        return _file.flush().then_wrapped([this](future<> f) {
            try {
                f.get();
                return make_ready_future<>();
            } catch (...) {
                logger.error("Failed to flush commits to disk: {}", std::current_exception());
                throw;
            }
        });
    }

    position_type position() const {
        return position_type(_file_pos);
    }

    size_t size_on_disk() const {
        return _file_pos;
    }

    bool contains(const replay_position& pos) const {
        return pos.id == _desc.id;
    }

    sstring get_segment_name() const {
        return _desc.filename();
    }

    void mark_delete(sstring commitlog_id, const replay_position& rp){
        if(_desc.commitlog_id == commitlog_id
          && (_desc.id < rp.id  //segment id
            ||(_desc.id == rp.id && _file_pos < rp.pos)  
          )
        ){
            _can_delete = true; 
        }
    }

    bool can_delete() {
        return _can_delete; 
    }
};

future<std::vector<hive_secondary_commitlog::descriptor>>
hive_secondary_commitlog::segment_manager::list_descriptors(sstring dirname) {
    struct helper {
        sstring _dirname;
        file _file;
        subscription<directory_entry> _list;
        std::vector<hive_secondary_commitlog::descriptor> _result;

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
            return make_ready_future<std::vector<hive_secondary_commitlog::descriptor>>(std::move(h->_result));
        }).finally([h] {});
    });
}

future<> hive_secondary_commitlog::segment_manager::init() {
    return list_descriptors(cfg.commit_log_location).then([this](std::vector<descriptor> descs) {
        std::vector<future<>> futures;
        for (auto& d : descs) {
            auto parsed_name = hive_tools::parse_commitlog_file_name(d.filename());
            if(parsed_name.commitlog_id == _commitlog_id) {
                sstring file_path = cfg.commit_log_location + "/" + d.filename();
                auto recover_future = recover_segment(file_path).then([this](sseg_ptr s){
                    _segments.push_back(s); 
                    return make_ready_future<>();
                }); 
                futures.push_back(std::move(recover_future));
            }
        }

        return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> ret){
            try{
                for(auto& f : ret){
                    f.get();
                }
                return make_ready_future<>();
            }catch(...){
                logger.error("error, failed to recover secondary segments, reason:{}", std::current_exception()); 
                throw;
            }
        });
    });
}

future<hive_secondary_commitlog::segment_manager::sseg_ptr> 
hive_secondary_commitlog::segment_manager::allocate_segment(sstring segment_file_path) {
    descriptor d(segment_file_path);
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    return open_checked_file_dma(commit_error, cfg.commit_log_location + "/" + d.filename()
            , open_flags::wo | open_flags::create, opt).then([this, d](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        return f.truncate(max_size).then([this, d, f] () mutable {
            auto s = make_lw_shared<segment>(this->shared_from_this(), d, std::move(f));
            return make_ready_future<sseg_ptr>(s);
        });
    });
}

future<hive_secondary_commitlog::segment_manager::sseg_ptr> 
hive_secondary_commitlog::segment_manager::recover_segment(sstring segment_file_path) {
    descriptor d(segment_file_path);
    file_open_options opt;
    return open_checked_file_dma(commit_error, cfg.commit_log_location + "/" + d.filename()
            , open_flags::wo, opt).then([this, d](file f) {
        // xfs doesn't like files extended betond eof, so enlarge the file
        auto s = make_lw_shared<segment>(this->shared_from_this(), d, std::move(f));
        return make_ready_future<sseg_ptr>(s);
    });
}
future<hive_secondary_commitlog::segment_manager::sseg_ptr> 
hive_secondary_commitlog::segment_manager::new_segment(sstring segment_file_path) {
    if (_shutdown) {
        throw std::runtime_error("Commitlog has been shut down. Cannot new segment");
    }
        
    return allocate_segment(segment_file_path).then([this](sseg_ptr s) {
        _segments.push_back(s);
        logger.debug("dltest==>999 _segments:{}, size:{}", &_segments, _segments.size());
        return make_ready_future<sseg_ptr>(s);
    });

}

future<hive_secondary_commitlog::segment_manager::sseg_ptr> 
hive_secondary_commitlog::segment_manager::find_or_create_segment(sstring segment_file_path) {
    logger.debug("hive_secondary_commitlog, find_or_create_segment start, segment_file_path:{}, _segments.size:{}"
        , segment_file_path, _segments.size());
    for (auto&s : _segments) {
        logger.debug("dltest==>111 _segments:{}, exist_segment_name:{}, need_segment_name:{}"
            , &_segments, s->get_segment_name(), segment_file_path);
        if(s->get_segment_name() == segment_file_path){
            logger.debug("hive_secondary_commitlog, find segment done, segment_file_path {}", segment_file_path);
            return make_ready_future<sseg_ptr>(s);
        }
        logger.debug("dltest==>111 not equal exist_segment_name:{}, need_segment_name:{}"
            , s->get_segment_name(), segment_file_path);
    }
    logger.debug("hive_secondary_commitlog, not found segment, start to wait _new_segment_semahore, segment_file_path {}, new_segment_semaphore current {}"
        , segment_file_path, _new_segment_semaphore.current());
    return _new_segment_semaphore.wait().then([this, segment_file_path]() {
        logger.debug("dltest==>222 _segments:{}, size:{}", &_segments, _segments.size());
        for (auto&s : _segments) {
            logger.debug("dltest==>222 exist_segment_name:{}, need_segment_name:{}"
                , s->get_segment_name(), segment_file_path);
            if(s->get_segment_name() == segment_file_path){
                logger.debug("hive_secondary_commitlog, found in wait, segment_file_path {}, new seq {}"
                    , segment_file_path, _new_segment_semaphore.current());
                return make_ready_future<sseg_ptr>(s);
            }
            logger.debug("dltest==>222 not equal exist_segment_name:{}, need_segment_name:{}"
                , s->get_segment_name(), segment_file_path);
        }
        logger.debug("hive_secondary_commitlog, not found segment, start to new segment, segment_file_path {}"
            , segment_file_path);
        return new_segment(segment_file_path).then([this](auto active_seg_ptr){
            return make_ready_future<sseg_ptr>(active_seg_ptr);
        });
    }).finally([this]{
        _new_segment_semaphore.signal();
    });;
}

/**
 * go through all segments, clear id up to pos. if segment becomes clean and unused by this,
 * it is discarded.
 */
void hive_secondary_commitlog::segment_manager::discard_completed_segments(
        sstring commitlog_id, const replay_position& rp) {
    logger.debug("Discard completed segments for {}, table {}", rp, commitlog_id);
    for (auto&s : _segments) {
        if(s->_closed==true){
            s->mark_delete(commitlog_id, rp);
        }
    }
    discard_unused_segments();
}



void hive_secondary_commitlog::segment_manager::discard_unused_segments() {
    logger.trace("Checking for unused segments ({} active)", _segments.size());

    auto i = std::remove_if(_segments.begin(), _segments.end(), [=](sseg_ptr s) {
        if (s->can_delete()) {
            logger.debug("Segment {} is unused", *s);
            return true;
        }
        return false;
    });

    if (i != _segments.end()) {
        _segments.erase(i, _segments.end());
    }
}

future<> hive_secondary_commitlog::segment_manager::shutdown() {
    if (!_shutdown) {
        _shutdown = true; // no re-arm, no create new segments.
    }
    return make_ready_future<>();
}

/*
 * Sync all segments, then clear them out. To ensure all ops are done.
 * (Assumes you have barriered adding ops!)
 * Only use from tests.
 */
future<> hive_secondary_commitlog::segment_manager::clear() {
    logger.debug("Clearing commitlog");
    return shutdown().then([this] {
       _segments.clear();
    });
}


std::vector<sstring> hive_secondary_commitlog::segment_manager::get_active_names() const {
    std::vector<sstring> res;
    for (auto i: _segments) {
        res.push_back(cfg.commit_log_location + "/" + i->get_segment_name());
    }
    return res;
}


future<> hive_secondary_commitlog::add_entry(const sstring segment_file_path, const uint64_t off, const bytes& content)
{
   logger.debug("hive_secondary_commitlog, add_entry start, segment_file_path {}, off {}", segment_file_path, off);
   return _segment_manager->find_or_create_segment(segment_file_path).then([off, &content] (auto s) {
        logger.debug("hive_secondary_commitlog, allocate start !");
        return s->allocate(off, content);
    });
}

hive_secondary_commitlog::hive_secondary_commitlog(sstring commitlog_id, db::config cfg, sstring commitlog_location)
        :_segment_manager(::make_shared<segment_manager>(commitlog_id, std::move(cfg), commitlog_location)) 
        , _commitlog_id(commitlog_id)
{}

hive_secondary_commitlog::hive_secondary_commitlog(hive_secondary_commitlog&& v) noexcept
        : _segment_manager(std::move(v._segment_manager))
        ,_commitlog_id(v._commitlog_id){
}

hive_secondary_commitlog::~hive_secondary_commitlog() {
}

future<hive_secondary_commitlog> hive_secondary_commitlog::create_commitlog(sstring commitlog_id
                                                      , db::config cfg
                                                      , sstring commitlog_location) {
    hive_secondary_commitlog c(commitlog_id, std::move(cfg), commitlog_location);
    auto f = c._segment_manager->init();
    return f.then([c = std::move(c)]() mutable {
        return make_ready_future<hive_secondary_commitlog>(std::move(c));
    });
}

void hive_secondary_commitlog::discard_completed_segments(sstring commitlog_id
                                                        , const replay_position& rp) {
    _segment_manager->discard_completed_segments(commitlog_id, rp);
}

future<> hive_secondary_commitlog::shutdown() {
    return _segment_manager->shutdown();
}

future<> hive_secondary_commitlog::clear() {
    return _segment_manager->clear();
}

const hive_secondary_commitlog::config& hive_secondary_commitlog::active_config() const {
    return _segment_manager->cfg;
}

std::vector<sstring> hive_secondary_commitlog::get_active_segment_names() const {
    return _segment_manager->get_active_names();
}

uint64_t hive_secondary_commitlog::get_pending_allocations() const {
    return _segment_manager->totals.pending_allocations;
}

uint64_t hive_secondary_commitlog::get_write_limit_exceeded_count() const {
    return _segment_manager->totals.write_limit_exceeded;
}


uint64_t hive_secondary_commitlog::get_num_segments_created() const {
    return _segment_manager->totals.segments_created;
}

uint64_t hive_secondary_commitlog::get_num_segments_destroyed() const {
    return _segment_manager->totals.segments_destroyed;
}


future<std::vector<hive_secondary_commitlog::descriptor>> hive_secondary_commitlog::list_existing_descriptors() const {
    return list_existing_descriptors(active_config().commit_log_location);
}

future<std::vector<hive_secondary_commitlog::descriptor>> hive_secondary_commitlog::list_existing_descriptors(const sstring& dir) const {
    return _segment_manager->list_descriptors(dir);
}

future<std::vector<sstring>> hive_secondary_commitlog::list_existing_segments() const {
    return list_existing_segments(active_config().commit_log_location);
}

future<std::vector<sstring>> hive_secondary_commitlog::list_existing_segments(const sstring& dir) const {
    return list_existing_descriptors(dir).then([dir](auto descs) {
        std::vector<sstring> paths;
        std::transform(descs.begin(), descs.end(), std::back_inserter(paths), [&](auto& d) {
           return dir + "/" + d.filename();
        });
        return make_ready_future<std::vector<sstring>>(std::move(paths));
    });
}


std::ostream& operator<<(std::ostream& out, const hive_secondary_commitlog::segment& s) {
    return out << s._desc.filename();
}

} //namespace hive
