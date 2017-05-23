#pragma once

#include "utils/UUID.hh"
#include "migrate_params_entry.hh"
#include <ostream>

namespace hive {

/**
 * Summary of streaming.
 */
class stream_summary {
public:
    migrate_type  type  = migrate_type::UNKNOWN_TYPE;
    migrate_scene scene = migrate_scene::UNKNOWN_SCENE;

    sstring  extent_group_id = "";
    sstring  dst_disk_id = "";
    uint64_t total_size = 0;

    //append for migrate extent journal
    sstring commitlog_file_name = ""; 

public:
    stream_summary() = default;
    
    //for serialize 
    stream_summary(migrate_type type_
                 , migrate_scene scene_
                 , sstring extent_group_id_
                 , sstring dst_disk_id_
                 , uint64_t total_size_
                 , sstring commitlog_file_name_)
        : type(type_)
        , scene(scene_)
        , extent_group_id(extent_group_id_)
        , dst_disk_id(dst_disk_id_)
        , total_size(total_size_) 
        , commitlog_file_name(commitlog_file_name_) 
        {}

    //for migrate extent group 
    stream_summary(sstring extent_group_id_
                 , sstring dst_disk_id_
                 , uint64_t total_size_)
        : type(migrate_type::MIGRATE_EXTENT_GROUP)
        , extent_group_id(extent_group_id_)
        , dst_disk_id(dst_disk_id_)
        , total_size(total_size_) 
        {}

    //for migrate extent journal
    stream_summary(migrate_scene scene_
        , sstring commitlog_file_name_
        , uint64_t total_size_)
        : type(migrate_type::MIGRATE_EXTENT_JOURNAL)
        , scene(scene_)
        , total_size(total_size_)
        , commitlog_file_name(commitlog_file_name_)
        {}


    friend std::ostream& operator<<(std::ostream& os, const stream_summary& summary);
};

} // namespace hive 
