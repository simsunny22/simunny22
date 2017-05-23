#pragma once

#include <memory>

#include "utils/data_output.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "utils/UUID.hh"
#include "hive/commitlog/replay_position_log.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/commitlog/hive_commitlog_entry.hh"

class file;

namespace db {
class config;
}

namespace hive {

//using namespace db;

class primary_journal;
using cf_id_type = utils::UUID;

class hive_commitlog {
public:
    class segment_manager;
    class segment;
public:
    ::shared_ptr<replay_position_log> _replay_position_log;
private:
    sstring _volume_id;
    ::shared_ptr<segment_manager> _segment_manager;
    sstring _commitlog_id;
    primary_journal* _parent_journal = nullptr;
public:
    sstring _get_commitlog_id();
    enum class sync_mode {
        PERIODIC, BATCH
    };
    struct config {
        config() = default;
        config(const config&) = default;
        config(const db::config&, sstring commitlog_location);

        sstring commit_log_location;
        uint64_t commitlog_total_space_in_mb = 0;
        uint64_t commitlog_segment_size_in_mb = 32;
        uint64_t commitlog_sync_period_in_ms = 10 * 1000; //TODO: verify default!
        // Max number of segments to keep in pre-alloc reserve.
        // Not (yet) configurable from scylla.conf.
        uint64_t max_reserve_segments = 2;
        // Max active writes/flushes. Default value
        // zero means try to figure it out ourselves
        uint64_t max_active_writes = 0;
        uint64_t max_active_flushes = 0;

        sync_mode mode = sync_mode::PERIODIC;
        uint64_t commitlog_cycle_sleep_in_us = 0;
    };

    struct descriptor {
    private:
        descriptor(sstring commitlog_id, std::pair<uint64_t, uint32_t> p);
    public:
        static const std::string SEPARATOR;
        static const std::string FILENAME_PREFIX;
        static const std::string FILENAME_EXTENSION;

        descriptor(descriptor&&) = default;
        descriptor(const descriptor&) = default;
        descriptor(sstring commitlog_id, segment_id_type i, uint32_t v = 1);
        descriptor(sstring commitlog_id, replay_position p);
        descriptor(sstring filename);

        sstring filename() const;
        operator replay_position() const;

        sstring commitlog_id;
        segment_id_type id;
        uint32_t ver;
    };

    hive_commitlog(hive_commitlog&&) noexcept;
    ~hive_commitlog();

    /**
     * Commitlog is created via a factory func.
     * This of course because it needs to access disk to get up to speed.
     * Optionally, could have an "init" func and require calling this.
     */
    static future<hive_commitlog> create_commitlog(sstring volume_id
                                                 , sstring commitlog_id
                                                 , db::config
                                                 , sstring commitlog_location);

    /**
     * Note: To be able to keep impl out of header file,
     * actual data writing is done via a std::function.
     * If it is proven that this has unacceptable overhead, this can be replace
     * by iter an interface or move segments and stuff into the header. But
     * I hope not.
     *
     * A serializing func is provided along with a parameter indicating the size
     * of data to be written. (See add).
     * Don't write less, absolutely don't write more...
     */
    using output = data_output;
    using serializer_func = std::function<void(output&)>;
    
    //return: write_order_id
    future<uint64_t> add_entry(const hive_commitlog_entry_writer& entry_writer);

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the replay position of the flush
     */
    void discard_completed_segments(const replay_position&);

    /**
     * A 'flush_handler' is invoked when the CL determines that size on disk has
     * exceeded allowable threshold. It is called once for every currently active
     * CF id with the highest replay_position which we would prefer to free "until".
     * I.e. a the highest potentially freeable position in the CL.
     *
     * Whatever the callback does to help (or not) this desire is up to him.
     * This is called synchronously, so callee might want to instigate async ops
     * in the background.
     *
     */
    typedef std::function<void(replay_position)> flush_handler;
    typedef uint64_t flush_handler_id;

    class flush_handler_anchor {
    public:
        friend class hive_commitlog;
        ~flush_handler_anchor();
        flush_handler_anchor(flush_handler_anchor&&);
        flush_handler_anchor(const flush_handler_anchor&) = delete;

        flush_handler_id release(); // disengage anchor - danger danger.
        void unregister();

    private:
        flush_handler_anchor(hive_commitlog&, flush_handler_id);

        hive_commitlog & _cl;
        flush_handler_id _id;
    };

    flush_handler_anchor add_flush_handler(flush_handler);
    void remove_flush_handler(flush_handler_id);

    /**
     * Returns a vector of the segment names
     */
    std::vector<sstring> get_active_segment_names() const;

    uint64_t get_total_size() const;
    uint64_t get_completed_tasks() const;
    uint64_t get_flush_count() const;
    uint64_t get_pending_tasks() const;
    uint64_t get_pending_writes() const;
    uint64_t get_pending_flushes() const;
    uint64_t get_pending_allocations() const;
    uint64_t get_write_limit_exceeded_count() const;
    uint64_t get_flush_limit_exceeded_count() const;
    uint64_t get_num_segments_created() const;
    uint64_t get_num_segments_destroyed() const;
    /**
     * Get number of inactive (finished), segments lingering
     * due to still being dirty
     */
    uint64_t get_num_dirty_segments() const;
    /**
     * Get number of active segments, i.e. still being allocated to
     */
    uint64_t get_num_active_segments() const;

    /**
     * Returns the largest amount of data that can be written in a single "mutation".
     */
    size_t max_record_size() const;

    /**
     * Return max allowed pending writes (per this shard)
     */
    uint64_t max_active_writes() const;
    /**
     * Return max allowed pending flushes (per this shard)
     */
    uint64_t max_active_flushes() const;

    future<> clear();

    const config& active_config() const;

    /**
     * Issues disk sync on all (allocating) segments. I.e. ensures that
     * all data written up until this call is indeed on disk.
     * _However_, if you issue new "add" ops while this is executing,
     * those can/will be missed.
     */
    future<> sync_all_segments();
    /**
     * Shuts everything down and causes any
     * incoming writes to throw exceptions
     */
    future<> shutdown();

    future<std::vector<descriptor>> list_existing_descriptors() const;
    future<std::vector<descriptor>> list_existing_descriptors(const sstring& dir) const;

    future<std::vector<sstring>> list_existing_segments() const;
    future<std::vector<sstring>> list_existing_segments(const sstring& dir) const;

    typedef std::function<future<>(temporary_buffer<char>, replay_position)> commit_load_reader_func;

    class segment_data_corruption_error: public std::runtime_error {
    public:
        segment_data_corruption_error(std::string msg, uint64_t s)
                : std::runtime_error(msg), _bytes(s) {
        }
        uint64_t bytes() const {
            return _bytes;
        }
    private:
        uint64_t _bytes;
    };

    static subscription<temporary_buffer<char>, replay_position> read_log_file(file, commit_load_reader_func, position_type = 0);
    static future<std::unique_ptr<subscription<temporary_buffer<char>, replay_position>>> read_log_file(
            const sstring&, commit_load_reader_func, position_type = 0);

    void set_parent_journal(primary_journal* parent_journal);
private:
    hive_commitlog(sstring volume_id, sstring commitlog_id, db::config, sstring commitlog_location);

    struct entry_writer {
        virtual size_t size(segment&) = 0;
        virtual void write(segment&, output&) = 0;
    };
};

} //namespace hive
