#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "utils/histogram.hh"
#include "seastar/core/shared_ptr.hh"
#include "db/consistency_level.hh"                                                                                      
#include "db/write_type.hh"
#include "database.hh"
#include "db/config.hh"
#include "log.hh"

#include "entity/volume_entity.hh"
#include "entity/volume_map_entity.hh"
#include "entity/extent_map_entity.hh"
#include "entity/extent_group_entity.hh"
#include "entity/extent_entity.hh"


#include "calculate/io_planner.hh"
#include "model/volume_helper.hh"
#include "model/volume_map_helper.hh"
#include "model/extent_map_helper.hh"
#include "model/extent_group_map_helper.hh"
#include "model/object_helper.hh"
#include "model/storage_map_helper.hh"

#include <stdlib.h>
#include <msgpack.hpp>

class database;
namespace hive{

extern logging::logger logger_v;
//static logging::logger logger("metadata_service");

// from config ?
const static uint32_t metadata_cache_ttl = 300 ; // ttl: 5 minutes
const static uint32_t metadata_cache_item_size = 800; // 900 + 80 < 1024
const static uint64_t metadata_cache_magic_header = 1001; //

struct commit_create_params {
    sstring container_name  = "";
    sstring volume_id       = "";
    sstring extent_group_id = "";
    sstring disk_id         = "";
    bool    created         = false;

    commit_create_params(
        sstring container_name
        , sstring volume_id
        , sstring extent_group_id
        , sstring disk_id
        , bool created)
        : container_name(container_name)
        , volume_id(volume_id)
        , extent_group_id(extent_group_id)
        , disk_id(disk_id)
        , created(created)
    {}
};

struct commit_write_params {
    sstring  container_name  = "";
    sstring  volume_id       = "";
    sstring  extent_group_id = "";
    sstring  disk_id         = "";
    uint64_t version         = 0;

    commit_write_params(
        sstring container_name
        , sstring volume_id
        , sstring extent_group_id
        , sstring disk_id
        , uint64_t version)
        : container_name(container_name)
        , volume_id(volume_id)
        , extent_group_id(extent_group_id)
        , disk_id(disk_id)
        , version(version)
    {}
};

struct extent_item {
    std::string extent_id = "";
    uint64_t offset = 0;
    MSGPACK_DEFINE(extent_id, offset);
};

class metadata_cache_item {
public:
    uint64_t magic = 0;
    uint64_t version = 0;
    uint64_t extents_num = 0;
    bool     created = false;
    std::string  extent_group_id = "undefined";
    std::string  disks = "";
    std::vector<extent_item> extents;

    temporary_buffer<char> serialize();
    void deserialize(temporary_buffer<char>&& buf, size_t size);

    temporary_buffer<char> pack();
    void unpack(temporary_buffer<char>&& buf, size_t size);
    MSGPACK_DEFINE(magic, version, extents_num, created, extent_group_id, disks, extents);
};

class metadata_service: public seastar::async_sharded_service<metadata_service> {
    using clock_type = std::chrono::steady_clock;
private:
    lw_shared_ptr<db::config> _config;
    extent_group_map_helper _extent_group_map_helper;
    extent_map_helper       _extent_map_helper;
    volume_map_helper       _volume_map_helper;
    volume_helper           _volume_helper;
    object_helper           _object_helper;
    storage_map_helper      _storage_map_helper;
    // cache : <extent_group_id, extent_group_entity>
    //std::unordered_map<sstring, extent_group_entity> _metadata_cache;

    seastar::shared_mutex _metadata_lock;

public:
    metadata_service();
    metadata_service(lw_shared_ptr<db::config> config);
    ~metadata_service();
    
    future<> stop();
   
    future<> print_memory();

    // volumes operation
    volume_entity load_volume_metadata(sstring volume_id);
    future<> update_volume_last_extent_group(sstring volume_id, sstring extent_group_id);
    future<> update_volume_vclock(sstring volume_id, int64_t vclock);

    // volume map operation
    future<sstring> get_extent_group_id(sstring container_name, sstring volume_id, uint64_t block_id);
    future<sstring> get_extent_group_id(sstring container_name, sstring volume_id, sstring extent_id);

    future<> create_volume_map(sstring container_name, sstring volume_id, uint64_t block_id, sstring extent_group_id);
    future<> create_volume_map(sstring container_name, sstring volume_id, sstring extent_id, sstring extent_group_id);
    future<> create_volume_map(sstring container_name, volume_map_entity entity);

    future<> remove_volume_map(sstring container_name, sstring volume_id);
    future<> remove_volume_map(sstring container_name, sstring volume_id, uint64_t block_id);
    future<> remove_volume_map(sstring container_name, sstring volume_id, sstring extent_id);

    // extent operation
    extent_map_entity load_extent_metadata(sstring container_name, sstring extent_id);
    future<> create_extent(sstring container_name, extent_map_entity extent_map);
    future<> remove_extent(sstring container_name, sstring extent_id);

    // extent_group operation
    extent_entity find_extent(std::vector<extent_entity> extents, sstring extent_id);
    extent_item   find_extent_from_cache_item(std::vector<extent_item>   extents, sstring extent_id);
    future <extent_group_entity> get_extent_group_entity(sstring container_name, sstring extent_group_id, sstring disk_id);
    future <extent_group_entity> get_extent_group_entity(sstring container_name, sstring extent_group_id);
    future<> add_extent_to_group(sstring container_name, sstring extent_group_id, sstring disk_ids, extent_entity extent);
    future<> add_extents_to_group(sstring container_name, sstring extent_group_id, sstring disk_ids, std::vector<extent_entity> extents);
    future<> update_extent_group_version(sstring container_name, sstring extent_group_id, sstring disk_id, uint64_t version);
    future<> update_extent_group_created(sstring container_name, sstring extent_group_id, sstring disk_ids, bool created);
    future <std::vector<sstring>> get_extent_group_disks(sstring container_name, sstring extent_group_id);
    future <bool> get_created(sstring container_name, sstring extent_group_id);
    future <uint64_t> get_version(sstring container_name, sstring extent_group_id);
    future<> create_extent_group(sstring container_name, extent_group_entity entity);
    future<> remove_extent_group(sstring container_name, sstring extent_group_id, sstring disk_id);
    future<uint64_t> get_extent_offset_in_group(sstring container_name, sstring extent_group_id, sstring extent_id);
    future<uint64_t> get_extents_num(sstring container_name, sstring extent_group_id);
    future<> create_extent_group_new_replica(sstring container_name, sstring volume_id, sstring extent_group_id, sstring src_disk_id, sstring dst_disk_id);

    //cache operation
    future<metadata_cache_item> get_extent_group_from_cache(sstring extent_group_id);
    future<bool> set_extent_group_to_cache(metadata_cache_item item);
    metadata_cache_item make_cache_item(extent_group_entity entity);

    //commit drain operation
    future<> commit_create_groups(std::vector<commit_create_params> create_params);
    future<> commit_write_groups(std::vector<commit_write_params>   write_params);
    future<std::vector<volume_map_entity>> get_volume_metadata(sstring container_name, sstring volume_id);

    //commit object size
    future<> update_object_size(sstring object_id, uint64_t new_size);


    //storage_map operate
    future<> create_storage_map(std::vector<sstring> disks, sstring extent_group_id, sstring container_name);

    unsigned shard_of(const sstring volume_id);
};

extern distributed<metadata_service> _the_metadata_service;

inline distributed<metadata_service>& get_metadata_service() {
    return _the_metadata_service;
}

inline metadata_service& get_local_metadata_service() {
    return _the_metadata_service.local();
}

inline shared_ptr<metadata_service> get_local_shared_metadata_service() {
    return _the_metadata_service.local_shared();
}

}//namespace hive
