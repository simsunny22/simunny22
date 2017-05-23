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

#include "core/shared_ptr.hh"
#include "core/timer.hh"
#include "utils/histogram.hh"
#include <chrono>
#include <boost/circular_buffer.hpp>
#include "log.hh"

#define NANO_TO_SECOND 1000000000
#define TIMEOUT 2000000000
#define MAX_BANDWIDTH 1000000000
#define MAX_IOPS 100000

namespace hive{


class stream_writer {
private:
    sstring _volume_id;
    bool _cache_enabled;
    hive_write_command _write_cmd;

    future<> set_extent_group_context(sstring extent_group_id, sstring disk_ids, int64_t vclock);
    //for rwrite
    future<> do_rwrite_volume(sstring volume_id, volume_revision revision);

    future<std::vector<volume_revision>> build_volume_revisions(
        hive_write_command write_cmd
        ,std::vector<rw_split_item> split_items);

    future<uint64_t> get_vclock(sstring volume_id, uint64_t count);
    future<uint64_t> 
    attach_vclock_to_volume_revision(sstring volume_id, std::vector<volume_revision>& revisions);
    future<> do_rwrite_volume(volume_revision revision);
    future<> commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks);

public:
    stream_writer(sstring volume_id, hive_write_command write_cmd);
    stream_writer();
    ~stream_writer();

    future<> execute();

}; //class stream_writer

}//namespace hive
