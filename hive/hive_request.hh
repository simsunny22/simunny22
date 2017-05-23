#pragma once

#include <experimental/optional>
#include "core/sstring.hh"
#include "enum_set.hh"
#include "gms/inet_address.hh"

namespace hive { 

class hive_write_command {
public:
    sstring  owner_id; //volume_id or object_id
    uint64_t offset;
    uint64_t length;
    bytes data;

    hive_write_command(
        sstring owner_id,
        uint64_t offset,
        uint64_t length,
        bytes    data)
        : owner_id(owner_id)
        , offset(offset)
        , length(length)
        , data(std::move(data))
    {}

    friend std::ostream& operator<<(std::ostream& out, const hive_write_command& write_cmd);
};

class hive_read_command {
public:
    sstring  owner_id; //volume_id or object_id
    uint64_t offset;
    uint64_t length;

    hive_read_command(
        sstring owner_id,
        uint64_t offset,
        uint64_t length)
        : owner_id(owner_id)
        , offset(offset)
        , length(length)
    {}

    friend std::ostream& operator<<(std::ostream& out, const hive_read_command& read_cmd);
};

class hive_read_subcommand {
public:
    sstring  owner_id; //volume_id or object_id
    sstring  extent_group_id;
    sstring  extent_id;
    uint64_t extent_offset_in_group;
    uint64_t data_offset_in_extent;
    uint64_t length;
    sstring  disk_ids;
    sstring  md5;
    sstring  options; //TODO:red need delete

    hive_read_subcommand(
        sstring owner_id,
        sstring extent_group_id,
        sstring extent_id,
        uint64_t extent_offset_in_group,
        uint64_t data_offset_in_extent,
        uint64_t length,
        sstring disk_ids,
        sstring md5,
        sstring options)
        : owner_id(owner_id)
        , extent_group_id(extent_group_id) 
        , extent_id(extent_id)
        , extent_offset_in_group(extent_offset_in_group)
        , data_offset_in_extent(data_offset_in_extent)
        , length(length)
        , disk_ids(disk_ids)
        , md5(md5)
        , options(options)
    {}
    
    friend std::ostream& operator<<(std::ostream& out, const hive_read_subcommand& read_cmd);
};


#if 0
class hive_read_command {
public:
    sstring  owner_id; //volume_id or object_id
    sstring  extent_group_id;
    sstring  extent_id;
    uint64_t extent_offset_in_group;
    uint64_t data_offset_in_extent;
    uint64_t length;
    sstring  disk_ids;
    sstring  datum_md5;
    sstring  options;
public:
    hive_read_command(
        sstring owner_id,
        sstring extent_group_id,
        sstring extent_id,
        uint64_t  extent_offset_in_group,
        uint64_t  data_offset_in_extent,
        uint64_t  length,
        sstring disk_ids,
        sstring datum_md5,
        sstring options)
        : owner_id(owner_id)
        , extent_group_id(extent_group_id)
        , extent_id(extent_id)
        , extent_offset_in_group(extent_offset_in_group)
        , data_offset_in_extent(data_offset_in_extent)
        , length(length)
        , disk_ids(disk_ids)
        , datum_md5(datum_md5)
        , options(options)
    { }

    friend std::ostream& operator<<(std::ostream& out, const hive_read_command& read_cmd);
};
#endif

class hive_access_trail_command{
public:
    sstring disk_id;
    sstring node_id;

    hive_access_trail_command(sstring disk_id,
                              sstring node_id)
            : disk_id(disk_id)
            , node_id(node_id){}
   
    friend std::ostream& operator<<(std::ostream& out, const hive_access_trail_command& read_cmd);
};


}  //namespace hive 
