#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive {

class volume_entity {
public:
    sstring _volume_id;
    sstring _driver_node_id;
    std::vector<sstring> _journal_nodes;
    int64_t _vclock;
    sstring _cluster_uuid;
    sstring _storage_pool_uuid;
    sstring _container_name;
    sstring _container_uuid;

    sstring _last_extent_group_id;
    bool    _marked_for_removal;
    uint64_t _max_capacity_bytes;
    sstring  _name;
    bool     _on_disk_dedup;
    sstring  _parent_volume;
    bool     _snapshot;
    std::vector<sstring> _snapshots;

public:
    volume_entity() {}
    volume_entity(sstring volume_id
               , sstring  driver_node
               , std::vector<sstring> journal_nodes
               , int64_t vclock
               , sstring cluster_uuid
               , sstring storage_pool_uuid
               , sstring container_name
               , sstring container_uuid
               , sstring last_extent_group_id
               , bool    marked_for_removal
               , uint64_t max_capacity_bytes
               , sstring name
               , bool dedup
               , sstring parent_volume
               , bool snapshot
               , std::vector<sstring> snapshots
               )
                   : _volume_id(volume_id)
                   , _driver_node_id(driver_node)
                   , _journal_nodes(std::move(journal_nodes))
                   , _vclock(vclock)
                   , _cluster_uuid(cluster_uuid)
                   , _storage_pool_uuid(storage_pool_uuid)
                   , _container_name(container_name)
                   , _container_uuid(container_uuid)
                   , _last_extent_group_id(last_extent_group_id)
                   , _marked_for_removal(marked_for_removal)
                   , _name(name)
                   , _on_disk_dedup(dedup)
                   , _parent_volume(parent_volume)
                   , _snapshot(snapshot)
                   , _snapshots(std::move(snapshots))
    {}

    ~volume_entity(){}

    volume_entity(volume_entity& entry) {
        _volume_id     = entry._volume_id;     
        _driver_node_id   = entry._driver_node_id;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        _marked_for_removal   = entry._marked_for_removal;
        _name                 = entry._name;
        _on_disk_dedup        = entry._on_disk_dedup;
        _parent_volume        = entry._parent_volume;
        _snapshot             = entry._snapshot;
        _snapshots            = entry._snapshots;
    }

    volume_entity& operator=(volume_entity& entry) noexcept  {
        _volume_id     = entry._volume_id;     
        _driver_node_id   = entry._driver_node_id;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        _marked_for_removal   = entry._marked_for_removal;
        _name                 = entry._name;
        _on_disk_dedup        = entry._on_disk_dedup;
        _parent_volume        = entry._parent_volume;
        _snapshot             = entry._snapshot;
        _snapshots            = entry._snapshots;
        return *this;
    }
    
    volume_entity& operator=(const volume_entity& entry) noexcept  {
        _volume_id     = entry._volume_id;     
        _driver_node_id   = entry._driver_node_id;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        _marked_for_removal   = entry._marked_for_removal;
        _name                 = entry._name;
        _on_disk_dedup        = entry._on_disk_dedup;
        _parent_volume        = entry._parent_volume;
        _snapshot             = entry._snapshot;
        _snapshots            = entry._snapshots;
        return *this;
    }
 
    volume_entity(const volume_entity& entry) {
        _volume_id     = entry._volume_id;     
        _driver_node_id   = entry._driver_node_id;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        _marked_for_removal   = entry._marked_for_removal;
        _name                 = entry._name;
        _on_disk_dedup        = entry._on_disk_dedup;
        _parent_volume        = entry._parent_volume;
        _snapshot             = entry._snapshot;
        _snapshots            = entry._snapshots;
    }

    //for move method
    volume_entity(volume_entity&& entry) noexcept  {
        if(this != &entry){
            _volume_id = entry._volume_id;     
            _driver_node_id = std::move(entry._driver_node_id);     
            _journal_nodes = std::move(entry._journal_nodes);     
            _vclock = entry._vclock;
            _cluster_uuid = entry._cluster_uuid;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _last_extent_group_id = entry._last_extent_group_id;
            _marked_for_removal   = entry._marked_for_removal;
            _name                 = entry._name;
            _on_disk_dedup        = entry._on_disk_dedup;
            _parent_volume        = entry._parent_volume;
            _snapshot             = entry._snapshot;
            _snapshots            = entry._snapshots;
        }
    }

    volume_entity& operator=(volume_entity&& entry) noexcept  {
        if(this != &entry){
            _volume_id = entry._volume_id;     
            _driver_node_id = std::move(entry._driver_node_id);     
            _journal_nodes = std::move(entry._journal_nodes);     
            _vclock = entry._vclock;
            _cluster_uuid = entry._cluster_uuid;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _last_extent_group_id = entry._last_extent_group_id;
            _marked_for_removal   = entry._marked_for_removal;
            _name                 = entry._name;
            _on_disk_dedup        = entry._on_disk_dedup;
            _parent_volume        = entry._parent_volume;
            _snapshot             = entry._snapshot;
            _snapshots            = entry._snapshots;
        }
        return *this;
    }
    
}; 

} //namespace hive
