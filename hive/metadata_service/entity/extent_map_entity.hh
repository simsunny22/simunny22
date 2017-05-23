#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive {

class extent_map_entity {
public:
    sstring           _extent_id;
    sstring           _extent_group_id;
    uint64_t          _mtime;

public:
    extent_map_entity(){}
    extent_map_entity(sstring extent_id, sstring extent_group_id, uint64_t mtime)
                       :_extent_id(extent_id)
                       ,_extent_group_id(extent_group_id)
                       ,_mtime(mtime){}

    extent_map_entity(extent_map_entity& entity){
        _extent_id       = entity._extent_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
    }

    extent_map_entity(const extent_map_entity& entity){
        _extent_id       = entity._extent_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
    }

    extent_map_entity& operator=(extent_map_entity& entity) noexcept  {
        _extent_id       = entity._extent_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
        return *this;
    }
    
    extent_map_entity& operator=(const extent_map_entity& entity) noexcept  {
        _extent_id       = entity._extent_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
        return *this;
    }

    extent_map_entity(extent_map_entity&& entity) noexcept  {
        if(this != &entity){
            _extent_id       = entity._extent_id;
            _extent_group_id = entity._extent_group_id;
            _mtime           = entity._mtime;
        }
    }

    extent_map_entity& operator=(extent_map_entity&& entity) noexcept  {
        if(this != &entity){
            _extent_id       = entity._extent_id;
            _extent_group_id = entity._extent_group_id;
            _mtime           = entity._mtime;
        }
        return *this;
    }

    sstring get_entity_string(){ 
        hive::Json body_json = hive::Json::object {
          {"extent_id", _extent_id.c_str()},
          {"extent_group_id", _extent_group_id.c_str()},
          {"mtime", to_sstring(_mtime).c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_extent_id(){return _extent_id;}
    sstring get_extent_group_id(){return _extent_group_id;}
    uint64_t get_mtime(){return _mtime;}

};

} //namespace hive
