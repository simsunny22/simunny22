#pragma once

#include <iostream>
#include <functional>
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include <ostream>
#include "migrate_params_entry.hh"


namespace hive {

class migrate_chunk {
public:
    migrate_type  type  = migrate_type::UNKNOWN_TYPE;
    migrate_scene scene = migrate_scene::UNKNOWN_SCENE; 

    sstring  volume_id;
    sstring  extent_group_id;
    uint64_t offset;
    uint64_t length;
    bytes    data;
    sstring  src_disk_id;
    sstring  dst_disk_id;

    //append for migrate extent journal
    sstring  commitlog_file_name;

public:
    migrate_chunk() = default;


    //for migrate serialize 
    migrate_chunk(migrate_type type_
                , migrate_scene scene_
                , sstring volume_id_
                , sstring extent_group_id_
                , uint64_t offset_
                , uint64_t length_
                , bytes data_
                , sstring src_disk_id_
                , sstring dst_disk_id_
                , sstring commitlog_file_name_)
                    : type(type_)
                    , scene(scene_)
                    , volume_id(volume_id_)
                    , extent_group_id(extent_group_id_)
                    , offset(offset_)
                    , length(length_)
                    , data(std::move(data_))
                    , src_disk_id(src_disk_id_)
                    , dst_disk_id(dst_disk_id_)
                    , commitlog_file_name(commitlog_file_name_)
                    {}

    //for migrate extent group
    migrate_chunk(sstring volume_id_
                , sstring extent_group_id_
                , uint64_t offset_
                , uint64_t length_
                , bytes data_
                , sstring src_disk_id_
                , sstring dst_disk_id_)
                    : type(migrate_type::MIGRATE_EXTENT_GROUP)
                    , volume_id(volume_id_)
                    , extent_group_id(extent_group_id_)
                    , offset(offset_)
                    , length(length_)
                    , data(std::move(data_))
                    , src_disk_id(src_disk_id_)
                    , dst_disk_id(dst_disk_id_)
                    , commitlog_file_name("")
                    {}

    //for migrate extent journal 
    migrate_chunk(migrate_scene scene_
                , sstring volume_id_
                , uint64_t offset_
                , uint64_t length_
                , bytes data_
                , sstring commitlog_file_name_)
                    : type(migrate_type::MIGRATE_EXTENT_JOURNAL)
                    , scene(scene_)
                    , volume_id(volume_id_)
                    , extent_group_id("")
                    , offset(offset_)
                    , length(length_)
                    , data(std::move(data_))
                    , src_disk_id("")
                    , dst_disk_id("")
                    , commitlog_file_name(commitlog_file_name_)
                    {}

};

std::ostream& operator<<(std::ostream& os, const migrate_chunk& chunk);

} //namespace hive

