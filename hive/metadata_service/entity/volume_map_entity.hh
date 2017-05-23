#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive {

class volume_map_entity {
public:
    sstring           _volume_id;
    uint64_t          _block_id;
    sstring           _extent_group_id;
    uint64_t          _mtime;

public:
    volume_map_entity(){
      _extent_group_id = "undefined";
    }
    volume_map_entity(sstring volume_id, uint64_t block_id, sstring extent_group_id, uint64_t mtime)
                       :_volume_id(volume_id)
                       ,_block_id(block_id)
                       ,_extent_group_id(extent_group_id)
                       ,_mtime(mtime){
    }

    volume_map_entity(volume_map_entity& entity){
        _volume_id       = entity._volume_id;
        _block_id        = entity._block_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
    }

    volume_map_entity(const volume_map_entity& entity){
        _volume_id       = entity._volume_id;
        _block_id        = entity._block_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
    }

    volume_map_entity& operator=(volume_map_entity& entity) noexcept  {
        _volume_id       = entity._volume_id;
        _block_id        = entity._block_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
        return *this;
    }
    
    volume_map_entity& operator=(const volume_map_entity& entity) noexcept  {
        _volume_id       = entity._volume_id;
        _block_id        = entity._block_id;
        _extent_group_id = entity._extent_group_id;
        _mtime           = entity._mtime;
        return *this;
    }

    volume_map_entity(volume_map_entity&& entity) noexcept  {
        if(this != &entity){
            _volume_id       = entity._volume_id;
            _block_id        = entity._block_id;
            _extent_group_id = entity._extent_group_id;
            _mtime           = entity._mtime;
        }
    }

    volume_map_entity& operator=(volume_map_entity&& entity) noexcept  {
        if(this != &entity){
            _volume_id       = entity._volume_id;
            _block_id        = entity._block_id;
            _extent_group_id = entity._extent_group_id;
            _mtime           = entity._mtime;
        }
        return *this;
    }

    sstring get_entity_string(){ 
        hive::Json body_json = hive::Json::object {
          {"volume_id", _volume_id.c_str()},
          {"block_id", to_sstring(_block_id).c_str()},
          {"extent_group_id", _extent_group_id.c_str()},
          {"mtime", to_sstring(_mtime).c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_volume_id(){return _volume_id;}
    uint64_t get_block_id(){return _block_id;}
    sstring get_extent_group_id(){return _extent_group_id;}
    uint64_t get_mtime(){return _mtime;}

};

} //namespace hive
