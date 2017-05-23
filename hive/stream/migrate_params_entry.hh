#pragma once

#include "core/sstring.hh"
#include <ostream> 

namespace hive {

enum class migrate_type:int{
    UNKNOWN_TYPE = 0,
    MIGRATE_EXTENT_GROUP,
    MIGRATE_EXTENT_JOURNAL,
    REPLICATE_EXTENT_GROUP,
};

enum class migrate_scene:int{
    UNKNOWN_SCENE = 0,
    PRIMARY_TO_PRIMARY,
    PRIMARY_TO_SECONDARY,
    SECONDARY_TO_SECONDARY,
    SECONDARY_TO_PRIMARY,
};

class migrate_params_entry {
public:
    //idl for serialize can not use inherit, so use type for the moment
    migrate_type  type  = migrate_type::UNKNOWN_TYPE; 
    migrate_scene scene = migrate_scene::UNKNOWN_SCENE;

    sstring driver_node_ip = "";
    sstring container_name = "";
    sstring intent_id      = "";

    sstring volume_id       = "";
    sstring extent_group_id = "";
    sstring src_disk_id     = "";
    sstring dst_disk_id     = "";
    uint64_t offset = 0;
    uint64_t length = 0;
    sstring src_file_path = "";
    sstring dst_file_path = "";
    
    // append for migrate extent_journal 
    sstring src_node_ip = "";
    sstring dst_node_ip = "";
    sstring commitlog_file_name = "";
    sstring commitlog_file_path = "";

    //method
    migrate_params_entry() = default;

    //for serialize
    migrate_params_entry(migrate_type type_
                       , migrate_scene scene_
                       , sstring driver_node_ip_
                       , sstring container_name_
                       , sstring intent_id_
                       , sstring volume_id_
                       , sstring extent_group_id_
                       , sstring src_disk_id_
                       , sstring dst_disk_id_
                       , uint64_t offset_
                       , uint64_t length_
                       , sstring src_file_path_ 
                       , sstring dst_file_path_ 
                       , sstring src_node_ip_
                       , sstring dst_node_ip_
                       , sstring commitlog_file_name_
                       , sstring commitlog_file_path_)
                           : type(type_)
                           , scene(scene_)
                           , driver_node_ip(driver_node_ip_)
                           , container_name(container_name_) 
                           , intent_id(intent_id_)
                           , volume_id(volume_id_)
                           , extent_group_id(extent_group_id_)
                           , src_disk_id(src_disk_id_)
                           , dst_disk_id(dst_disk_id_)
                           , offset(offset_)
                           , length(length_)
                           , src_file_path(src_file_path_)
                           , dst_file_path(dst_file_path_)
                           , src_node_ip(src_node_ip_)
                           , dst_node_ip(dst_node_ip_)
                           , commitlog_file_name(commitlog_file_name_)
                           , commitlog_file_path(commitlog_file_path_)
                           {}

    //for migrate extent group
    migrate_params_entry(sstring driver_node_ip_
                       , sstring container_name_
                       , sstring intent_id_
                       , sstring volume_id_
                       , sstring extent_group_id_
                       , sstring src_disk_id_
                       , sstring dst_disk_id_
                       , uint64_t offset_
                       , uint64_t length_)
                           : type(migrate_type::MIGRATE_EXTENT_GROUP)
                           , driver_node_ip(driver_node_ip_) 
                           , container_name(container_name_)
                           , intent_id(intent_id_)
                           , volume_id(volume_id_)
                           , extent_group_id(extent_group_id_)
                           , src_disk_id(src_disk_id_)
                           , dst_disk_id(dst_disk_id_)
                           , offset(offset_)
                           , length(length_)
                           , src_file_path("")
                           , dst_file_path("")
                           {}

    //for migrate extent journal 
    migrate_params_entry(migrate_scene scene_
                       , sstring volume_id_
                       , sstring src_node_ip_
                       , sstring dst_node_ip_)
                           : type(migrate_type::MIGRATE_EXTENT_JOURNAL)
                           , scene(scene_)
                           , volume_id(volume_id_)
                           , src_node_ip(src_node_ip_)
                           , dst_node_ip(dst_node_ip_)
                           {}


    //for replicate extent group
    migrate_params_entry(sstring volume_id_
                       , sstring extent_group_id_
                       , sstring src_disk_id_
                       , sstring dst_disk_id_
                       , uint64_t offset_
                       , uint64_t length_)
                           : type(migrate_type::REPLICATE_EXTENT_GROUP)
                           , volume_id(volume_id_)
                           , extent_group_id(extent_group_id_)
                           , src_disk_id(src_disk_id_)
                           , dst_disk_id(dst_disk_id_)
                           , offset(offset_)
                           , length(length_){}

};

std::ostream& operator<<(std::ostream& os, const migrate_params_entry& params);

} // namespace hive
