#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"
#include "extent_entity.hh"

#include <stdlib.h>
#include<msgpack.hpp>

namespace hive {

class extent_group_entity {
public:
    std::string                 _extent_group_id;
    std::string                 _disk_id;
    std::vector<extent_entity>  _extents;
    std::string                 _owner_id;
    bool                        _created;
    uint64_t                    _version;
    uint64_t                    _mtime;

    MSGPACK_DEFINE(_extent_group_id, _disk_id, _extents, _owner_id, _created, _version, _mtime);

public:
    extent_group_entity(){
        _extent_group_id = "undefined";
        _disk_id         = "";
        _created         = false;
    }
    extent_group_entity(sstring extent_group_id
                       , sstring disk_id
		       , sstring owner_id
                       , uint64_t version)
                       :_extent_group_id(extent_group_id)
                       , _disk_id(disk_id)
		       , _owner_id(owner_id)
                       ,_version(version){}

    sstring get_entity_string(){ 
        hive::Json body_json = hive::Json::object {
          {"extent_group_id", _extent_group_id.c_str()},
          {"disk_id", _disk_id.c_str()},
          {"version", to_sstring(_version).c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_extent_group_id(){return _extent_group_id;}
    sstring get_disk_id(){return _disk_id;}
    std::vector<extent_entity> get_extents(){return _extents;}
    uint64_t get_version(){return _version;}
    uint64_t get_mtime(){return _mtime;}

};

} //namespace hive
