#ifndef __GURU_KIT__H__
#define __GURU_KIT__H__

#include <map>
#include <memory>
#include "seastar/core/sstring.hh"
#include "seastar/core/future.hh"
#include "seastar/core/distributed.hh"

#include "log.hh"
#include "hive/http/seawreck.hh"

#include "hive/http/json11.hh"

#include "hive/hive_plan.hh"
#include "hive/metadata_service/entity/volume_entity.hh"
#include "hive/metadata_service/entity/object_entity.hh"

namespace hive {
                                                        
//////////////////////////////////
// ++ class guru_kit 
//////////////////////////////////
class guru_kit {
public:
    guru_kit();
    ~guru_kit();
 
public:
    sstring  build_request_body_for_get_write_plan(sstring volume_id, uint64_t offset, uint64_t length);
    sstring  build_request_body_for_get_read_plan(sstring volume_id, uint64_t offset, uint64_t length);
    sstring  build_request_body_for_commit_create_group(std::vector<create_action> create_actions);
    sstring  build_request_body_for_commit_write_plan(write_plan wplan);
    sstring  build_request_body_for_allocate_disks(sstring container_uuid);
    sstring  build_request_body_for_allocate_disk_for_extent_group(sstring volume_id, sstring extent_group_id, std::vector<sstring> disks, sstring error_disk_id);

    sstring  build_request_body_for_get_volume_entity(sstring volume_id);

    write_plan parse_write_plan(sstring write_plan_json);
    read_plan parse_read_plan(sstring read_plan_json);
    volume_entity parse_volume_entity(sstring volume_string);
    object_entity parse_object_entity(sstring object_string);
    std::vector<sstring> parse_allocate_disks(sstring disks_json);
    sstring parse_allocate_disk_for_extent_group(sstring disk_json);

    future<write_plan> get_write_plan(sstring url, sstring volume_id, uint64_t offset, uint64_t length);
    future<> commit_write_plan(sstring url, write_plan wplan);
    future<> commit_create_group(sstring url, std::vector<create_action> create_actions);

    future<read_plan> get_read_plan(sstring url, sstring volume_id, uint64_t offset, uint64_t length);

    future<object_entity> get_object_entity(sstring url, sstring object_id);

    future<volume_entity> get_volume_entity(sstring url, sstring volume_id);
    future<std::vector<sstring>> allocate_write_disks(sstring url, sstring volume_id);

    future<sstring> allocate_disk_for_extent_group(sstring url, sstring volume_id, sstring extent_group_id, std::vector<sstring> disk_ids,  sstring error_disk_id);

    future<sstring> request_from_guru(sstring url, sstring body);

    //for auto test
    future<volume_entity> get_volume_entity_for_test(sstring volume_id);
    future<std::vector<sstring>> allocate_write_disks_for_test(sstring volume_id);
    future<sstring> allocate_disk_for_extent_group_for_test(std::vector<sstring> disk_ids, sstring error_disk_id);

};

} //namespacehive
#endif // __GURU_KIT_H__
