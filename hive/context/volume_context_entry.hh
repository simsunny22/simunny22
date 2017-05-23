#pragma once

#include "core/sstring.hh"
#include <vector>
#include "context_entry.hh"
#include "hive/http/json11.hh"

namespace hive {

class node_context_entry{
private:
    sstring _id;
    sstring _ip;
public:
    node_context_entry(){}
    node_context_entry(sstring id, sstring ip):_id(id), _ip(ip){}


    node_context_entry(node_context_entry& entry) {
        _id = entry._id;
        _ip = entry._ip;
    }   
        
    node_context_entry(const node_context_entry& entry) {
        _id = entry._id;
        _ip = entry._ip;
    }   
        
    node_context_entry& operator=(node_context_entry& entry) noexcept  {
        _id = entry._id;
        _ip = entry._ip;
        return *this;
    }   
        
    node_context_entry& operator=(const node_context_entry& entry) noexcept  {
        _id = entry._id;
        _ip = entry._ip;
        return *this;
    }   
        
    node_context_entry(node_context_entry&& entry) noexcept  {
        if(this != &entry){
            _id = entry._id;
            _ip = entry._ip;
        }   
    }   

    node_context_entry& operator=(node_context_entry&& entry) noexcept  {
        if(this != &entry){
            _id = entry._id;
            _ip = entry._ip;
        }   
        return *this;
    }   

    sstring get_id(){return _id;}
    sstring get_ip(){return _ip;}
    sstring get_entry_string(){
        hive::Json body_json = hive::Json::object {
          {"id", _id.c_str()},
          {"ip", _ip.c_str()},
        };  
        auto body = body_json.dump();
        return body;
    }   
};


class volume_context_entry : public context_entry{
private:
    sstring _volume_id;
    node_context_entry _driver_node;
    std::vector<node_context_entry> _journal_nodes;
    uint64_t _vclock;
    //int32_t _replication_factor= 2;//replication_factor
    sstring _cluster_uuid;
    sstring _storage_pool_uuid;
    sstring _container_name;
    sstring _container_uuid;
    sstring _last_extent_group_id;
public:
    volume_context_entry() {}
    volume_context_entry(sstring volume_id 
               , node_context_entry driver_node 
               , std::vector<node_context_entry> journal_nodes
               , uint64_t vclock
               //, int32_t rf=2
               , sstring cluster_uuid = "default_cluster"
               , sstring storage_pool_uuid = "default_storage_pool"
               , sstring container_name = "default_container_name"
               , sstring container_uuid = "default_container"
               , sstring last_extent_group_id= ""
               )
                   : _volume_id(volume_id)
                   , _driver_node(std::move(driver_node))
                   , _journal_nodes(std::move(journal_nodes))
                   , _vclock(vclock)
                   //, _replication_factor(rf)
                   , _cluster_uuid(cluster_uuid)
                   , _storage_pool_uuid(storage_pool_uuid)
                   , _container_name(container_name)
                   , _container_uuid(container_uuid)
                   , _last_extent_group_id(last_extent_group_id)
    {}

    ~volume_context_entry(){}

    volume_context_entry(volume_context_entry& entry) {
        _volume_id     = entry._volume_id;     
        _driver_node   = entry._driver_node;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        //_replication_factor = entry._replication_factor;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
    }

    volume_context_entry& operator=(volume_context_entry& entry) noexcept  {
        _volume_id     = entry._volume_id;     
        _driver_node   = entry._driver_node;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        //_replication_factor = entry._replication_factor;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        return *this;
    }
    
    volume_context_entry& operator=(const volume_context_entry& entry) noexcept  {
        _volume_id     = entry._volume_id;     
        _driver_node   = entry._driver_node;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        //_replication_factor = entry._replication_factor;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
        return *this;
    }
 
    volume_context_entry(const volume_context_entry& entry) {
        _volume_id     = entry._volume_id;     
        _driver_node   = entry._driver_node;     
        _journal_nodes = entry._journal_nodes;     
        _vclock        = entry._vclock;
        //_replication_factor = entry._replication_factor;
        _cluster_uuid = entry._cluster_uuid;
        _storage_pool_uuid = entry._storage_pool_uuid;
        _container_name = entry._container_name;
        _container_uuid = entry._container_uuid;
        _last_extent_group_id = entry._last_extent_group_id;
    }

    //for move method
    volume_context_entry(volume_context_entry&& entry) noexcept  {
        if(this != &entry){
            _volume_id = entry._volume_id;     
            _driver_node = std::move(entry._driver_node);     
            _journal_nodes = std::move(entry._journal_nodes);     
            _vclock = entry._vclock;
            //_replication_factor = entry._replication_factor;
            _cluster_uuid = entry._cluster_uuid;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _last_extent_group_id = entry._last_extent_group_id;
        }
    }

    volume_context_entry& operator=(volume_context_entry&& entry) noexcept  {
        if(this != &entry){
            _volume_id = entry._volume_id;     
            _driver_node = std::move(entry._driver_node);     
            _journal_nodes = std::move(entry._journal_nodes);     
            _vclock = entry._vclock;
            //_replication_factor = entry._replication_factor;
            _cluster_uuid = entry._cluster_uuid;
            _storage_pool_uuid = entry._storage_pool_uuid;
            _container_name = entry._container_name;
            _container_uuid = entry._container_uuid;
            _last_extent_group_id = entry._last_extent_group_id;
        }
        return *this;
    }
    
    sstring get_volume_id(){return _volume_id;}
    node_context_entry& get_driver_node(){return _driver_node;}
    std::vector<node_context_entry>& get_journal_nodes(){return _journal_nodes;} 
    uint64_t get_vclock(){return _vclock;}
    //int32_t get_rf(){return _replication_factor;}
    sstring get_cluster_uuid(){return _cluster_uuid;}
    sstring get_storage_pool_uuid(){return _storage_pool_uuid;}
    sstring get_container_name(){return _container_name;}
    sstring get_container_uuid(){return _container_uuid;}
    sstring get_last_extent_group_id(){return _last_extent_group_id;}

    context_entry* get_context_ptr(){
       return this;
    }

    sstring get_entry_string() {
        sstring volume_stirng = "volume_id:" + _volume_id;
        sstring driver_node_string = "driver_node:" + _driver_node.get_entry_string();
        sstring journal_nodes_string = ""; 
        for(auto it : _journal_nodes) {
            if(journal_nodes_string == "") {
                journal_nodes_string = it.get_entry_string();
            } else{
                journal_nodes_string = it.get_entry_string() + ", " + journal_nodes_string;
            }
        }
        sstring vclock_string = "vclock:" + to_sstring(_vclock);
        //sstring replication_factor_string = "replication_factor:" + to_sstring(_replication_factor);
        sstring cluster_uuid_string = "cluster_uuid_string:" + _cluster_uuid;
        sstring storage_pool_uuid_string = "storage_pool_uuid:" + _storage_pool_uuid;
        sstring container_name_string = "container_name:" + _container_name;
        sstring container_uuid_string = "container_uuid:" + _container_uuid;
        sstring last_extent_group_id_string = "last_extent_group_id:" + _last_extent_group_id;
        return "{" + volume_stirng + "; " 
                   + driver_node_string + "; journal_nodes:[" 
                   + journal_nodes_string + "]; " 
                   + vclock_string + "; " 
                   //+ replication_factor_string + "; " 
                   + cluster_uuid_string + "; " 
                   + storage_pool_uuid_string + "; " 
                   + container_name_string + "; " 
                   + container_uuid_string + ";"
                   + last_extent_group_id_string+ "}"; 

    }
}; 

} //namespace hive

