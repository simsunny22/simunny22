#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive {

class storage_map_entity {
public:
    sstring           _disk_id;
    int64_t           _hash;
    sstring           _extent_group_id;
    sstring           _container_name;

public:
    storage_map_entity(){
      _extent_group_id = "undefined";
    }
    storage_map_entity(sstring disk_id, int64_t hash, sstring extent_group_id, sstring container_name)
                       :_disk_id(disk_id)
                       ,_hash(hash)
                       ,_extent_group_id(extent_group_id)
                       ,_container_name(container_name){
    }

    storage_map_entity(storage_map_entity& entity){
        _disk_id         = entity._disk_id;
        _hash            = entity._hash;
        _extent_group_id = entity._extent_group_id;
        _container_name  = entity._container_name;
    }

    storage_map_entity(const storage_map_entity& entity){
        _disk_id         = entity._disk_id;
        _hash            = entity._hash;
        _extent_group_id = entity._extent_group_id;
        _container_name  = entity._container_name;
    }

    storage_map_entity& operator=(storage_map_entity& entity) noexcept  {
        _disk_id         = entity._disk_id;
        _hash            = entity._hash;
        _extent_group_id = entity._extent_group_id;
        _container_name  = entity._container_name;
        return *this;
    }
    
    storage_map_entity& operator=(const storage_map_entity& entity) noexcept  {
        _disk_id         = entity._disk_id;
        _hash            = entity._hash;
        _extent_group_id = entity._extent_group_id;
        _container_name  = entity._container_name;
        return *this;
    }

    storage_map_entity(storage_map_entity&& entity) noexcept  {
        if(this != &entity){
            _disk_id         = entity._disk_id;
            _hash            = entity._hash;
            _extent_group_id = entity._extent_group_id;
            _container_name  = entity._container_name;
        }
    }

    storage_map_entity& operator=(storage_map_entity&& entity) noexcept  {
        if(this != &entity){
            _disk_id         = entity._disk_id;
            _hash            = entity._hash;
            _extent_group_id = entity._extent_group_id;
            _container_name  = entity._container_name;
        }
        return *this;
    }

    sstring get_entity_string(){ 
        hive::Json body_json = hive::Json::object {
          {"disk_id", _disk_id.c_str()},
          {"hash", to_sstring(_hash).c_str()},
          {"extent_group_id", _extent_group_id.c_str()},
          {"container_name", to_sstring(_container_name).c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_disk_id(){return _disk_id;}
    int64_t get_hash(){return _hash;}
    sstring get_extent_group_id(){return _extent_group_id;}
    sstring get_container_name(){return _container_name;}

};

} //namespace hive
