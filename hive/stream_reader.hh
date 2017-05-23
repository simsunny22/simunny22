#pragma once
#include "hive/journal/primary_journal.hh"
#include "hive/journal/volume_revision.hh"
#include "hive/commitlog/hive_commitlog.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/hive_result.hh"
#include "hive/hive_request.hh"
#include "hive/extent_datum.hh"
#include "hive/context/disk_context_entry.hh"
#include "hive/hive_plan.hh"
#include "hive/trail/access_trail.hh"
#include "hive/metadata_service/calculate/io_planner.hh"
#include "hive/cache/stream_cache.hh"

#include "core/shared_ptr.hh"
#include "core/timer.hh"
#include "utils/histogram.hh"
#include <chrono>
#include <boost/circular_buffer.hpp>
#include "log.hh"

namespace hive{

struct cache_revision{
    sstring key;
    uint64_t offset_in_cache_datum;
    uint64_t length;
    uint64_t offset_in_extent;
    lw_shared_ptr<bytes> content;
};

class stream_reader {
private:
    sstring _volume_id;
    read_plan _read_plan;
    bool _cache_enabled;
private:
    future<std::vector<hive_read_subcommand>> build_read_subcommand();
    future<std::vector<volume_revision>> load_journal_data(hive_read_subcommand read_subcmd);
    future<bytes> load_extent_store_data(hive_read_subcommand read_subcmd);
    future<bytes> load_data(hive_read_subcommand read_subcmd);
    future<bytes> load_cache_data(hive_read_subcommand read_subcmd);

    void replay_revisions_on_datum(
        std::vector<volume_revision>& revisions
        , bytes& datum
        , uint64_t datum_data_offset_in_extent );
    future<> write_trail_log(
        sstring volume_id
        , sstring extent_group_id
        , sstring disk_ids
        , uint64_t length
        , sstring options
        , access_trail_type trail_type);

    future<std::tuple<uint64_t, bytes>> do_read_volume(uint64_t order_id, hive_read_subcommand read_subcmd);
    future<bytes> execute_subcommads(std::vector<hive_read_subcommand> read_subcmds);

public:
    stream_reader(){}
    stream_reader(sstring volume_id, read_plan plan);
    ~stream_reader();

    future<bytes> execute();

}; //class stream_reader 

}//namespace hive
