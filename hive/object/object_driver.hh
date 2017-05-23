#pragma once

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/future-util.hh"
#include "core/shared_mutex.hh"
#include "hive/hive_plan.hh"
#include "hive/store/hint_entry.hh"
#include "hive/metadata_service/calculate/io_planner.hh"

namespace hive{

struct object_info {
    sstring container_name = "";
    sstring container_uuid = "";
    sstring last_extent_group_id = "";
    uint64_t size = 0;
};

class object_driver {
private:
    io_planner _planner;
    seastar::shared_mutex _driver_lock;

    std::unordered_map<sstring, object_info> _object_cache;

    future<object_info> get_object_info(sstring object_id);

    allocate_item allocate_from_new_extent_group(sstring container_name, sstring container_uuid, sstring object_id, sstring extent_id);
    allocate_item allocate_from_last_extent_group(sstring container_name, sstring object_id, sstring extent_group_id, sstring extent_id);
    allocate_item allocate_from_exist_extent_group(sstring container_name, sstring object_id, sstring extent_group_id, sstring extent_id);

public:
    object_driver();
    ~object_driver();

    future<write_plan> get_write_plan(sstring object_id, uint64_t offset, uint64_t length);
    future<read_plan> get_read_plan(sstring object_id, uint64_t offset, uint64_t length);

    future<> commit_create_group(sstring object_id, std::vector<create_action> actions);
    future<> commit_for_write_object(std::vector<hint_entry> hints);
    future<uint64_t> commit_write_object(sstring object_id, uint64_t offset, uint64_t length);
}; //class object_driver

}//namespace hive
