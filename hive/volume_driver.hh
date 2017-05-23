#pragma once

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/shared_mutex.hh"
#include "db/config.hh"
#include "hive/context/volume_context_entry.hh"
#include "hive/context/context_service.hh"
#include "hive/drain/drain_plan.hh"

#include "hive/hive_plan.hh"
#include "hive/metadata_service/entity/volume_entity.hh"
#include "hive/metadata_service/calculate/io_planner.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/drain_extent_group_task.hh"

namespace hive{

struct volume_stats{
    int64_t rwrite_requests = 0; // client no longer waits for the write
    int64_t rwrite_bytes = 0;
    int64_t swrite_requests = 0; // client no longer waits for the write
    int64_t swrite_bytes = 0;
};

struct driver_info{
    uint64_t vclock;
    bool need_redirect = false;
    sstring redirect_ip;
};

class volume_driver {
private:
    sstring        _volume_id;
    volume_entity  _volume_entity;

    uint64_t        _vclock;
    uint64_t        _write_count;
    volume_stats _stats;
    io_planner   _planner;
    std::unordered_map<sstring, sstring> _cache;
    seastar::shared_mutex _driver_lock;

    future<> start_volume_stream();
    future<sstring> get_extent_group_id(sstring container_name, sstring volume_id, sstring extent_id);

    std::vector<drain_extent_group_task> make_drain_tasks(sstring volume_id, std::unordered_map<sstring, drain_extent_group_task> extent_group_tasks);
    future<allocate_item> allocate_extent_group(sstring volume_id, sstring extent_id);
    allocate_item allocate_from_new_extent_group(sstring container_name, sstring container_uuid, sstring volume_id, sstring extent_id);
    allocate_item allocate_from_last_extent_group(sstring container_name, sstring volume_id, sstring extent_group_id, sstring extent_id);
    allocate_item allocate_from_exist_extent_group(sstring container_name, sstring volume_id, sstring extent_group_id, sstring extent_id);

public:
    volume_driver(sstring volume_id);
    ~volume_driver();

    future<> init();
    
    future<> rebuild_volume_stream();
    future<> stop_volume_stream();
    future<volume_context_entry> init_volume_context();
    inline uint64_t get_vclock(uint64_t count = 1){ 
        uint64_t ret_vclock = _vclock;
        _vclock = (_vclock+count) < 0 ? 0 : _vclock+count;
        return ret_vclock;
    }

    sstring get_extent_group_id_from_cache(sstring extent_id);
    future<> load_volume_metadata();
    future<write_plan> get_write_plan(sstring volume_id, uint64_t offset, uint64_t length);
    future<> commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks);
    future<> commit_create_group(std::vector<create_action> create_actions);

    future<read_plan> get_read_plan(sstring volume_id, uint64_t offset, uint64_t length);
    future<drain_plan> make_drain_plan(std::vector<volume_revision_view> volume_revision_views);
    future<drain_plan> rescue_drain_plan(std::vector<drain_extent_group_task> tasks);

    //test function
    future<> test_drain_plan();
    future<drain_plan> make_drain_plan_for_test(sstring disk_ids, std::vector<volume_revision_view> volume_revision_views );
    read_action make_read_plan_test(rw_split_item item);
}; //class volume_driver

}//namespace hive
