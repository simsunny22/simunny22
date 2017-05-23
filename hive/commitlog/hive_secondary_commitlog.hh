#pragma once

#include <memory>
#include "utils/data_output.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "utils/UUID.hh"

#include "hive/commitlog/replay_position.hh"
class file;

namespace db {
class config;
}

namespace hive {

using namespace db;

using cf_id_type = utils::UUID;

/*
 * Commit Log tracks every write operation into the system. The aim of
 * the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable.
 *
 * This impl is cassandra log format compatible (for what it is worth).
 * The behaviour is similar, but not 100% identical as "stock cl".
 *
 * Files are managed with "normal" file writes (as normal as seastar
 * gets) - no mmapping. Data is kept in internal buffers which, when
 * full, are written to disk (see below). Files are also flushed
 * periodically (or always), ensuring all data is written + writes are
 * complete.
 *
 * In BATCH mode, every write to the log will also send the data to disk
 * + issue a flush and wait for both to complete.
 *
 * In PERIODIC mode, most writes will only add to the internal memory
 * buffers. If the mem buffer is saturated, data is sent to disk, but we
 * don't wait for the write to complete. However, if periodic (timer)
 * flushing has not been done in X ms, we will write + flush to file. In
 * which case we wait for it.
 *
 * The commitlog does not guarantee any ordering between "add" callers
 * (due to the above). The actual order in the commitlog is however
 * identified by the replay_position returned.
 *
 * Like the stock cl, the log segments keep track of the highest dirty
 * (added) internal position for a given table id (cf_id_type / UUID).
 * Code should ensure to use discard_completed_segments with UUID +
 * highest rp once a memtable has been flushed. This will allow
 * discarding used segments. Failure to do so will keep stuff
 * indefinately.
 */
class hive_secondary_commitlog {
public:
    class segment_manager;
    class segment;
private:
    ::shared_ptr<segment_manager> _segment_manager;
    sstring _volume_id;
    sstring _commitlog_id;
public:
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

    hive_secondary_commitlog(hive_secondary_commitlog&&) noexcept;
    ~hive_secondary_commitlog();

    /**
     * Commitlog is created via a factory func.
     * This of course because it needs to access disk to get up to speed.
     * Optionally, could have an "init" func and require calling this.
     */
    static future<hive_secondary_commitlog> create_commitlog(sstring commitlog_id
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

    /**
     * Add an entry to the commit log.
     *
     * @param entry_writer a writer responsible for writing the entry
     */
    future<> add_entry(const sstring segment_file_path, const uint64_t off, const bytes& content);

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the replay position of the flush
     */
    void discard_completed_segments(sstring commitlog_id, const replay_position& rp);

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
    /**
     * Returns a vector of the segment names
     */
    std::vector<sstring> get_active_segment_names() const;

    uint64_t get_total_size() const;
    uint64_t get_pending_allocations() const;
    uint64_t get_write_limit_exceeded_count() const;
    uint64_t get_num_segments_created() const;
    uint64_t get_num_segments_destroyed() const;
    /**
     * Return max allowed pending writes (per this shard)
     */
    uint64_t max_active_writes() const;
    future<> clear();

    const config& active_config() const;

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
private:
    hive_secondary_commitlog(sstring commitlog_id, db::config, sstring commitlog_location);

    struct entry_writer {
        virtual size_t size(segment&) = 0;
        virtual void write(segment&, output&) = 0;
    };
};

} //namespace hive
