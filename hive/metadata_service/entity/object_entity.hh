#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive {

class object_entity {
public:
    sstring _object_id = "";
    sstring _cluster_uuid = "";
    sstring _storage_pool_name = "";
    sstring _storage_pool_uuid = "";
    sstring _container_name = "";
    sstring _container_uuid = "";
    sstring _name  = "";
    uint64_t _size = 0;
    uint64_t _vclock = 0;
    bool     _marked_for_removal = false;

public:
    object_entity() {}
    object_entity(sstring object_id
               , sstring cluster_uuid
               , sstring storage_pool_name
               , sstring storage_pool_uuid
               , sstring container_name
               , sstring container_uuid
	       , sstring name
               , uint64_t size
               , uint64_t vclock
	       , bool     marked_for_removal
               )
                   : _object_id(object_id)
                   , _cluster_uuid(cluster_uuid)
                   , _storage_pool_name(storage_pool_name)
                   , _storage_pool_uuid(storage_pool_uuid)
                   , _container_name(container_name)
                   , _container_uuid(container_uuid)
		   , _name(name)
                   , _size(size)
                   , _vclock(vclock)
		   , _marked_for_removal(marked_for_removal)
    {}

    ~object_entity(){}

    object_entity(object_entity& entry) {
        _object_id     = entry._object_id;     
        _cluster_uuid  = entry._cluster_uuid;
        _storage_pool_name = entry._storage_pool_name;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _name                 = entry._name;
	_size          = entry._size;
        _vclock        = entry._vclock;
        _marked_for_removal   = entry._marked_for_removal;
    }

    object_entity& operator=(object_entity& entry) noexcept  {
        _object_id     = entry._object_id;     
        _cluster_uuid  = entry._cluster_uuid;
        _storage_pool_name = entry._storage_pool_name;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _name                 = entry._name;
	_size          = entry._size;
        _vclock        = entry._vclock;
        _marked_for_removal   = entry._marked_for_removal;
        return *this;
    }
    
    object_entity& operator=(const object_entity& entry) noexcept  {
        _object_id     = entry._object_id;     
        _cluster_uuid  = entry._cluster_uuid;
        _storage_pool_name = entry._storage_pool_name;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _name                 = entry._name;
	_size          = entry._size;
        _vclock        = entry._vclock;
        _marked_for_removal   = entry._marked_for_removal;
        return *this;
    }
 
    object_entity(const object_entity& entry) {
        _object_id     = entry._object_id;     
        _cluster_uuid  = entry._cluster_uuid;
        _storage_pool_name = entry._storage_pool_name;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _name                 = entry._name;
	_size          = entry._size;
        _vclock        = entry._vclock;
        _marked_for_removal   = entry._marked_for_removal;
    }

    //for move method
    object_entity(object_entity&& entry) noexcept  {
        if(this != &entry){
            _object_id     = entry._object_id;     
            _cluster_uuid  = entry._cluster_uuid;
            _storage_pool_name = entry._storage_pool_name;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _name                 = entry._name;
	    _size          = entry._size;
            _vclock        = entry._vclock;
            _marked_for_removal   = entry._marked_for_removal;
        }
    }

    object_entity& operator=(object_entity&& entry) noexcept  {
        if(this != &entry){
            _object_id     = entry._object_id;     
            _cluster_uuid  = entry._cluster_uuid;
            _storage_pool_name = entry._storage_pool_name;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _name                 = entry._name;
	    _size          = entry._size;
            _vclock        = entry._vclock;
            _marked_for_removal   = entry._marked_for_removal;
        }
        return *this;
    }


}; 

} //namespace hive
