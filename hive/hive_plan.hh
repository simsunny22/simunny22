#pragma once

#include <experimental/optional>
#include "core/sstring.hh"
#include "enum_set.hh"
#include "gms/inet_address.hh"
#include <set> 

namespace hive { 

struct create_action {
    sstring extent_group_id;
    sstring disk_ids;
};

struct rw_split_item {
    rw_split_item(){
        extent_id = "";
        data_offset_in_extent = 0;
	      extent_offset_in_volume = 0;
        length = 0;
        data_offset = 0;
    }

    sstring extent_id;
    uint64_t extent_offset_in_volume;
    uint64_t data_offset_in_extent;
    uint64_t length;
    uint64_t data_offset;
};

struct allocate_item {
    sstring  volume_id = "";
    sstring  extent_id = "";
    sstring  extent_group_id = "undefined";
    uint64_t extent_offset_in_group = 0;
    uint64_t version = 0;
    bool     need_create = false;
    std::vector<sstring> disks;
};

//TODO: write_volume_action, write_object_action
struct write_action {
    write_action(){
        type = "write";
        
        volume_id = "";
        data_offset_in_volume = 0;

        extent_id = "";
        data_offset_in_extent = 0;

        std::shared_ptr<bytes> buffer;
        uint64_t offset_in_buffer;
        length = 0;
    }

    sstring type;

    sstring  volume_id;
    uint64_t data_offset_in_volume;
    sstring  extent_id;
    uint64_t data_offset_in_extent;

    std::shared_ptr<bytes> buffer;
    uint64_t offset_in_buffer
    uint64_t length;
};


struct read_action {
    read_action(){
        type = "read",
        volume_id = "";

		    extent_id = "";
		    data_offset_in_extent = 0;

	      disk_ids = "";
		    extent_group_id = "";
		    extent_offset_in_group = 0;

		    length = 0;
		    md5 = "";
	 }

   sstring type;
   sstring volume_id;

   // params for read journal
   sstring extent_id;
   uint64_t data_offset_in_extent;

   // params for read extent_group
   sstring disk_ids;
   sstring extent_group_id;
   uint64_t extent_offset_in_group;

   uint64_t length;
   sstring md5;
};

class write_plan {
public:
    sstring owner_id; //volume_id or object_id
    std::vector<write_action>   write_actions;
    friend std::ostream& operator << (std::ostream& out, const write_plan& plan);
};

class read_plan {
public:
    sstring owner_id; //volume_id or object_id
    std::vector<read_action>   read_actions;
    friend std::ostream& operator << (std::ostream& out, const read_plan& plan);
};

}  //namespace hive 
