#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "exceptions/exceptions.hh"

#include "locator/snitch_base.hh"
#include "log.hh"
#include "to_string.hh"
#include <seastar/core/thread.hh>
#include <sstream>
#include <algorithm>
#include "unimplemented.hh"
#include <sys/time.h>

#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/enum.hh>
#include <seastar/net/tls.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>

#include "service/storage_proxy.hh"
#include "db/consistency_level_type.hh"
#include "partition_slice_builder.hh"
#include "query-result-reader.hh"
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "atomic_cell.hh"

#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"

#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include "bytes.hh"
#include <chrono>
#include <functional>
#include <cstdint>
#include "hive/memcache/memcached_service.hh"

#include "metadata_service.hh"
#include "hive/volume_service.hh"

using namespace std::chrono_literals;
namespace hive {
static logging::logger logger("metadata_service");

distributed<metadata_service> _the_metadata_service;
using namespace exceptions;


temporary_buffer<char> metadata_cache_item::serialize() {
    temporary_buffer<char> buffer = temporary_buffer<char>(metadata_cache_item_size);
    memset(buffer.get_write(), 0, metadata_cache_item_size);

    size_t pos = 0;
    //magic header
    magic = metadata_cache_magic_header;
    memcpy(buffer.get_write(), &magic, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //extent_group_id length
    uint64_t g_id_len = extent_group_id.length();
    memcpy(buffer.get_write()+pos, &g_id_len, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //extent_group_id 
    memcpy(buffer.get_write()+pos, extent_group_id.c_str(), g_id_len);
    pos += g_id_len;
    //disks length
    uint64_t disks_len = disks.length();
    memcpy(buffer.get_write()+pos, &disks_len, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //disks
    memcpy(buffer.get_write()+pos, disks.c_str(), disks_len);
    pos += disks_len;
    //created
    memcpy(buffer.get_write()+pos, &created, sizeof(bool));
    pos += sizeof(bool);
    //version
    memcpy(buffer.get_write()+pos, &version, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //extents_num
    extents_num = extents.size();
    memcpy(buffer.get_write()+pos, &extents_num, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //extents content
    for(auto extent : extents) {
        uint64_t e_id_len = extent.extent_id.length();
        memcpy(buffer.get_write()+pos, &e_id_len, sizeof(uint64_t));
        pos += sizeof(uint64_t);
        memcpy(buffer.get_write()+pos, extent.extent_id.c_str(), e_id_len);
        pos += e_id_len;
        memcpy(buffer.get_write()+pos, &extent.offset, sizeof(uint64_t));
        pos += sizeof(uint64_t);
    }

    return std::move(buffer);
}

void metadata_cache_item::deserialize(temporary_buffer<char>&& buf, size_t size) {
    size_t pos = 0;
    memcpy(&magic, buf.begin(), sizeof(uint64_t));
    if(magic != metadata_cache_magic_header) {
        logger.error("[{}] magic:{} size:{}", __func__, magic, size);
        throw hive::metadata_cache_error_exception("deserialize failed");
    };
    pos += sizeof(uint64_t);
    uint64_t g_id_len = 0;
    memcpy(&g_id_len, buf.begin() + pos, sizeof(uint64_t));

    pos += sizeof(uint64_t);
    temporary_buffer<char> g_tmp(g_id_len+1);
    memset(g_tmp.get_write(), 0, g_id_len+1);
    std::copy_n(buf.begin()+pos, g_id_len, g_tmp.get_write());
    extent_group_id = to_sstring(g_tmp.get());
    pos += g_id_len;
    uint64_t disks_len = 0;
    memcpy(&disks_len, buf.begin() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    
    temporary_buffer<char> d_tmp(disks_len+1);
    memset(d_tmp.get_write(), 0, disks_len+1);
    std::copy_n(buf.begin()+pos, disks_len, d_tmp.get_write());
    disks = to_sstring(d_tmp.get());

    pos += disks_len;
    memcpy(&created, buf.begin() + pos, sizeof(bool));
    pos += sizeof(bool);
    memcpy(&version, buf.begin() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&extents_num, buf.begin() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    for(uint64_t i = 0; i<extents_num; i++){
        extent_item item;
        uint64_t extent_id_len = 0;
        memcpy(&extent_id_len, buf.begin() + pos, sizeof(uint64_t));
        pos += sizeof(uint64_t);
        
        temporary_buffer<char> e_tmp(extent_id_len+1);
        memset(e_tmp.get_write(), 0, extent_id_len+1);
        std::copy_n(buf.begin()+pos, extent_id_len, e_tmp.get_write());
        item.extent_id = to_sstring(e_tmp.get());

        pos += extent_id_len;
        memcpy(&item.offset, buf.begin() + pos, sizeof(uint64_t));
        pos += sizeof(uint64_t);

        extents.push_back(item);
    }
}

metadata_service::metadata_service(lw_shared_ptr<db::config> config) 
        : _config(config)
{
    logger.info("constructor start");
}

metadata_service::metadata_service() 
{
    logger.info("constructor start");
}

metadata_service::~metadata_service() 
{}

unsigned metadata_service::shard_of(const sstring volume_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(volume_id)));
    return dht::shard_of(token);
}

future<> metadata_service::stop() {
    return make_ready_future<>();
}

//unsigned metadata_service::shard_of(const sstring extent_id){
//    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_id)));
//    return dht::shard_of(token);
//}

future<> metadata_service::print_memory(){
    logalloc::shard_tracker().print_memory_info();
    return make_ready_future<>();
}


volume_entity metadata_service::load_volume_metadata(sstring volume_id){
    volume_entity volumeEntity;
    return volumeEntity;
}

future<> metadata_service::update_volume_last_extent_group(sstring volume_id, sstring extent_group_id){
    return _volume_helper.update_volume_last_extent_group(volume_id, extent_group_id);
}

future<> metadata_service::update_volume_vclock(sstring volume_id, int64_t vclock){
    return _volume_helper.update_volume_vclock(volume_id, vclock);
}


future<> metadata_service::create_extent(sstring container_name, extent_map_entity extent_map){
    return _extent_map_helper.create(container_name, extent_map._extent_id, extent_map._extent_group_id);
}
future<> metadata_service::remove_extent(sstring container_name, sstring extent_id){
    return _extent_map_helper.remove(container_name, extent_id);
}

future<sstring> metadata_service::get_extent_group_id(sstring container_name, sstring volume_id, sstring extent_id){
    auto splits = hive_tools::split_to_vector(extent_id, "#");
    uint64_t block_id = hive_tools::str_to_int64(splits.at(1));
    return _volume_map_helper.find(container_name, volume_id, block_id).then([](auto entity){
        if (entity._extent_group_id == "undefined") {                                                       
            return make_ready_future<sstring>("undefined"); 
        }
        return make_ready_future<sstring>(std::move(entity._extent_group_id)); 
    });
}

future<sstring> metadata_service::get_extent_group_id(sstring container_name, sstring volume_id, uint64_t block_id){
    return _volume_map_helper.find(container_name, volume_id, block_id).then([](auto entity){
        if (entity._extent_group_id == "undefined") {                                                       
            return make_ready_future<sstring>("undefined"); 
        }
        return make_ready_future<sstring>(std::move(entity._extent_group_id)); 
    });
}

future<> metadata_service::create_volume_map(sstring container_name, sstring volume_id, uint64_t block_id, sstring extent_group_id){
    return _volume_map_helper.create(container_name, volume_id, block_id, extent_group_id); 
}

future<> metadata_service::create_volume_map(sstring container_name, sstring volume_id, sstring extent_id, sstring extent_group_id){
    auto splits = hive_tools::split_to_vector(extent_id, "#");
    uint64_t block_id = hive_tools::str_to_int64(splits.at(1));
    return _volume_map_helper.create(container_name, volume_id, block_id, extent_group_id); 
}

future<> metadata_service::create_volume_map(sstring container_name, volume_map_entity entity){
    return _volume_map_helper.create(container_name, entity._volume_id, entity._block_id, entity._extent_group_id); 
}

future<> metadata_service::remove_volume_map(sstring container_name, sstring volume_id){
    return _volume_map_helper.remove(container_name, volume_id); 
}

future<> metadata_service::remove_volume_map(sstring container_name, sstring volume_id, uint64_t block_id){
    return _volume_map_helper.remove(container_name, volume_id, block_id); 
}

future<> metadata_service::remove_volume_map(sstring container_name, sstring volume_id, sstring extent_id){
    auto splits = hive_tools::split_to_vector(extent_id, "#");
    uint64_t block_id = hive_tools::str_to_int64(splits.at(1));
    return _volume_map_helper.remove(container_name, volume_id, block_id); 
}

metadata_cache_item metadata_service::make_cache_item(extent_group_entity entity) {
    metadata_cache_item cache_item;
    cache_item.extent_group_id = entity._extent_group_id;
    cache_item.version         = entity._version;
    cache_item.extents_num     = entity._extents.size();
    cache_item.created         = entity._created;
    cache_item.disks           = entity._disk_id;

    for(auto& extent : entity._extents){
        extent_item e_item;
        e_item.extent_id = extent._extent_id;
        e_item.offset    = extent._offset;

        cache_item.extents.push_back(e_item);
    }

    return std::move(cache_item);
}

future <extent_group_entity> metadata_service::get_extent_group_entity(sstring container_name, sstring extent_group_id, sstring disk_id){
    return _extent_group_map_helper.find(container_name, extent_group_id, disk_id).then([this, extent_group_id](auto group_entity){
        return make_ready_future<extent_group_entity>(group_entity);
    });
}


future <extent_group_entity> metadata_service::get_extent_group_entity(sstring container_name, sstring extent_group_id){
    return _extent_group_map_helper.find(container_name, extent_group_id).then([this, extent_group_id](auto group_entity){
        return make_ready_future<extent_group_entity>(group_entity);
    });
}

future<> metadata_service::add_extent_to_group(sstring container_name, sstring extent_group_id, sstring disk_ids, extent_entity extent){
    //1. load group from db to cache
    return get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id, disk_ids, extent](auto entity){
        entity._extents.push_back(extent);
        auto cache_item = this->make_cache_item(entity);
        return this->set_extent_group_to_cache(std::move(cache_item)).then([this, container_name, extent_group_id, disk_ids, extent, entity](auto success){
            logger.debug("[{}] extent_group_id:{}, disk_ids:{}, cache extents size:{}", __func__, extent_group_id, disk_ids, entity._extents.size());

            auto disks = hive_tools::split_to_vector(disk_ids, ":");
            std::vector<future<>> futures;
            for(auto disk_id : disks) {
              auto f = _extent_group_map_helper.update_extents(container_name, extent_group_id, disk_id, entity._extents);
              futures.push_back(std::move(f));
            }
            return when_all(futures.begin(), futures.end()).discard_result();
        });
    });
}

future<> metadata_service::add_extents_to_group(sstring container_name, sstring extent_group_id, sstring disk_ids, std::vector<extent_entity> extents){
    //1. load group from db to cache
    return get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id, disk_ids, extents](auto entity){
        for(auto extent:extents){
            entity._extents.push_back(extent);
        }
        auto cache_item = this->make_cache_item(entity);
        return this->set_extent_group_to_cache(std::move(cache_item)).then([this, container_name, extent_group_id, disk_ids, entity](auto success){
            logger.debug("[{}] extent_group_id:{}, disk_ids:{}, cache extents size:{}", __func__, extent_group_id, disk_ids, entity._extents.size());

            auto disks = hive_tools::split_to_vector(disk_ids, ":");
            std::vector<future<>> futures;
            for(auto disk_id : disks) {
              auto f = _extent_group_map_helper.update_extents(container_name, extent_group_id, disk_id, entity._extents);
              futures.push_back(std::move(f));
            }
            return when_all(futures.begin(), futures.end()).discard_result();
        });
    });
}


future <std::vector<sstring>> metadata_service::get_extent_group_disks(sstring container_name, sstring extent_group_id){
    return get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id](auto cache_item){
        if (cache_item.extent_group_id != "undefined"){
            auto disks_string = cache_item.disks;
	    if (disks_string == "") {
                return  _extent_group_map_helper.get_extent_group_disks(container_name, extent_group_id).then([this, disks_string, extent_group_id, cache_item](auto disk_ids){
                    auto new_cache_item = cache_item;
            logger.debug("[{}] extent_group_id:{}, disk_ids:{}", __func__, extent_group_id, disk_ids);
		    new_cache_item.disks = hive_tools::join(disk_ids, ":");
                    return this->set_extent_group_to_cache(std::move(new_cache_item)).then([disk_ids](auto success){
                        return make_ready_future<std::vector<sstring>>(std::move(disk_ids));
		    });
                });
	    }else{
                auto disks = hive_tools::split_to_vector(disks_string, ":");
                return make_ready_future<std::vector<sstring>>(std::move(disks));
	    }
        }else{
            //1. load group from db to cache
            return this->get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id](auto entity_){
                return  _extent_group_map_helper.get_extent_group_disks(container_name, extent_group_id).then([this, extent_group_id, entity_](auto disk_ids){
                    auto cache_item = this->make_cache_item(entity_);
            logger.debug("[{}] extent_group_id:{}, disk_ids:{}", __func__, extent_group_id, disk_ids);
		    cache_item.disks = hive_tools::join(disk_ids, ":");
                    return this->set_extent_group_to_cache(std::move(cache_item)).then([disk_ids](auto success){
                        return make_ready_future<std::vector<sstring>>(disk_ids);
		    });
                });
            });
        }
    });
}


future <bool> metadata_service::get_created(sstring container_name, sstring extent_group_id){
    return get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id](auto cache_item){
        if (cache_item.extent_group_id != "undefined"){
            auto create = cache_item.created;
            return make_ready_future<bool>(create);
        }else{
            //1. load group from db to cache
            return this->get_extent_group_entity(container_name, extent_group_id).then([this](auto entity_){
                auto cache_item = this->make_cache_item(entity_);
                return this->set_extent_group_to_cache(std::move(cache_item)).then([this, entity_](auto success){
                    return make_ready_future<bool>(entity_._created);
		});
            });
        }
    });
}

future <uint64_t> metadata_service::get_version(sstring container_name, sstring extent_group_id){
    return get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id](auto cache_item){
        if (cache_item.extent_group_id != "undefined"){
            auto version = cache_item.version;
            return make_ready_future<uint64_t>(version);
        }else{
            //1. load group from db to cache
            return this->get_extent_group_entity(container_name, extent_group_id).then([this](auto entity_){
                auto cache_item = this->make_cache_item(entity_);
                return this->set_extent_group_to_cache(std::move(cache_item)).then([this, entity_](auto success){
                    return make_ready_future<uint64_t>(entity_._version);
		});
            });
        }
    });
}

future<> metadata_service::update_extent_group_version(sstring container_name, sstring extent_group_id, sstring disk_id, uint64_t version){
    return _extent_group_map_helper.update_version(container_name, extent_group_id, disk_id, version);
}

future<> metadata_service::update_extent_group_created(sstring container_name, sstring extent_group_id, sstring disk_ids, bool created){
logger.debug("[{}] start container_name:{}, extent_group_id:{}, disk_ids:{}, created:{}",__func__, container_name, extent_group_id, disk_ids, created);
    //1. load group from db to cache
    return get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id, disk_ids, created](auto entity){
        //2. update cache
        entity._created = created;
        auto cache_item = this->make_cache_item(entity);
        return this->set_extent_group_to_cache(std::move(cache_item)).then([this, container_name, extent_group_id, disk_ids, created](auto success){
            std::vector<future<>> futures;
            auto disks = hive_tools::split_to_vector(disk_ids, ":");
            for(auto disk_id: disks){
                auto f = _extent_group_map_helper.update_created(container_name, extent_group_id, disk_id, created); 
                futures.push_back(std::move(f));
            }
            return when_all(futures.begin(), futures.end()).discard_result();
        });
    });
}

future<> metadata_service::create_extent_group(sstring container_name, extent_group_entity entity){
  logger.debug("[{}] start container_name:{}",__func__, container_name);
    //1. set cache
    auto cache_item = make_cache_item(entity);
    return set_extent_group_to_cache(std::move(cache_item)).then([this, container_name, entity](auto success){
        logger.debug("[{}] start container_name:{} success:{}",__func__, container_name, success);
        std::vector<future<>> futures;
        auto disks = hive_tools::split_to_vector(entity._disk_id, ":");
        for(auto disk_id: disks){
            auto f = _extent_group_map_helper.create(container_name, entity._owner_id, entity._extent_group_id, disk_id);
            futures.push_back(std::move(f));
        }
        return when_all(futures.begin(), futures.end()).discard_result();
    });

}

future<> metadata_service::create_extent_group_new_replica(sstring container_name, sstring volume_id, sstring extent_group_id, sstring src_disk_id, sstring dst_disk_id){
  logger.debug("[{}] start container_name:{}",__func__, container_name);
    //1. create new replica
    return _extent_group_map_helper.create(container_name, volume_id, extent_group_id, dst_disk_id).then([this, container_name, volume_id, extent_group_id, src_disk_id, dst_disk_id](){
        //2. copy extents to new replica 
	return this->get_extent_group_entity(container_name, extent_group_id, src_disk_id).then([this, container_name, extent_group_id, dst_disk_id](auto entity){
            return _extent_group_map_helper.update_extents(container_name, extent_group_id, dst_disk_id, entity._extents);
        });
    }).then([this, extent_group_id, src_disk_id, dst_disk_id](){
        //3. update cache disks
        return this->get_extent_group_from_cache(extent_group_id).then([this, src_disk_id, dst_disk_id](auto cache_item)mutable{
            auto old_disks = hive_tools::split_to_vector(cache_item.disks, ":");
	    //filter src_disk_id
            auto filter_disks = hive_tools::filter(old_disks, src_disk_id);
            filter_disks.push_back(dst_disk_id);
  logger.debug("[{}] create_extent_group_new_replica old_disks:{} filter_disks{} src_disk_id:{} dst_disk_id:{} ",__func__, old_disks, filter_disks, src_disk_id, dst_disk_id);
            auto new_disks = hive_tools::deduplicate(filter_disks);
            auto new_disks_string  = hive_tools::join(new_disks, ":");
            cache_item.disks = new_disks_string;
	    return this->set_extent_group_to_cache(cache_item);
        });
    }).then([](auto success){
        return make_ready_future<>();
    });
}

future<> metadata_service::remove_extent_group(sstring container_name, sstring extent_group_id, sstring disk_id){
    //1.remove cache
    //2.remove db
    return _extent_group_map_helper.remove(container_name, extent_group_id, disk_id);
}

future<uint64_t> metadata_service::get_extent_offset_in_group(sstring container_name, sstring extent_group_id, sstring extent_id) {
    //1. query from metadata cache
    return get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id, extent_id](auto cache_item){
        if (cache_item.extent_group_id != "undefined"){
            auto extent  = this->find_extent_from_cache_item(cache_item.extents, extent_id);
            uint64_t offset = extent.offset;
            return make_ready_future<uint64_t>(offset);
        }else{
            //2. query from scylla db and set cache
            return this->get_extent_group_entity(container_name, extent_group_id).then([this, extent_id](auto entity_){
                auto cache_item = this->make_cache_item(entity_);
                return this->set_extent_group_to_cache(std::move(cache_item)).then([this, extent_id, entity_](auto success){
                    auto extents = entity_._extents;
                    auto extent  = this->find_extent(extents, extent_id);
                    return make_ready_future<uint64_t>(extent._offset);
		});
            });
        }
    });
}

future<uint64_t> metadata_service::get_extents_num(sstring container_name, sstring extent_group_id) {
    //1. query from metadata cache
    return get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id](auto cache_item){
        if (cache_item.extent_group_id != "undefined"){
            auto num = cache_item.extents_num;
logger.debug("[{}] extent_group_id:{} from cache num:{}", __func__, extent_group_id, num);
            return make_ready_future<uint64_t>(num);
        }else{
            //2. query from scylla db and set cache
            return this->get_extent_group_entity(container_name, extent_group_id).then([this, extent_group_id](auto entity_){
                auto cache_item = this->make_cache_item(entity_);
                return this->set_extent_group_to_cache(std::move(cache_item)).then([this, extent_group_id, entity_](auto success){
                    auto num = entity_._extents.size();
logger.debug("[{}] extent_group_id:{} from db num:{}", __func__, extent_group_id, num);
                    return make_ready_future<uint64_t>(num);
		});
            });
        }
    });
}

extent_item metadata_service::find_extent_from_cache_item(std::vector<extent_item> extents, sstring extent_id) {
  for(auto& extent : extents) {
      if(to_sstring(extent.extent_id) == extent_id){
        return extent;
      }else{
        continue;
      }
  }

  //TODO:need throw error
  extent_item item;
  item.extent_id = "undefined";
  return item;
}


extent_entity metadata_service::find_extent(std::vector<extent_entity> extents, sstring extent_id) {
  for(auto& extent : extents) {
      if(to_sstring(extent._extent_id) == extent_id){
        return extent;
      }else{
        continue;
      }
  }
  //TODO:need throw error
  extent_entity entity;
  entity._extent_id = "undefined";
  return entity;
}

future<metadata_cache_item> metadata_service::get_extent_group_from_cache(sstring extent_group_id) {
logger.debug("[{}] extent_group_id:{}", __func__, extent_group_id);
    auto& memcached = memcache::get_local_memcached_service();
    memcache::item_key key(extent_group_id);
    return memcached.get(key).then([extent_group_id](auto entity_ptr)mutable{
        if (!entity_ptr) {
logger.debug("[{}] not find extent_group_id:{}", __func__, extent_group_id);
            metadata_cache_item item;
            return make_ready_future<metadata_cache_item>(std::move(item));
        }else{
            //unpack sstring
            auto value = temporary_buffer<char>(entity_ptr->value().get(), entity_ptr->value().size());
	    metadata_cache_item item;
	    //item.deserialize(std::move(value), value.size());

      msgpack::object_handle oh1 = msgpack::unpack(value.get(), (std::size_t)value.size());
      msgpack::object const& obj1 = oh1.get();
      obj1.convert(item);

logger.debug("[{}] found extent_group_id:{}, size:{} disks:{}", __func__, extent_group_id, entity_ptr->value().size(), item.disks);
            return make_ready_future<metadata_cache_item>(std::move(item));
        }
    });
}

future<bool> metadata_service::set_extent_group_to_cache(metadata_cache_item item) {
   //return make_ready_future<bool>(true);
logger.debug("[{}] extent_group_id:{},disk_id:{},extent_size:{}", __func__, item.extent_group_id,item.disks,item.extents.size()); 
    auto& memcached = memcache::get_local_memcached_service();
    auto extent_group_id = item.extent_group_id;
    memcache::item_insertion_data insertion;
    memcache::item_key key(extent_group_id);

    
    //pack entity
    //auto buffer = item.serialize();
    std::stringstream buffer;
    msgpack::pack(buffer, item);
    insertion.key = std::move(key);
    //insertion.data = std::move(buffer);//temporary_buffer<char>(buffer.get(), buffer.size());//std::move(buffer);//
    insertion.data = temporary_buffer<char>(buffer.str().data(), buffer.str().size());//std::move(buffer);//

logger.error("[{}]  2 extent_group_id:{},disk_id:{},extent_size:{} insertion.data.size:{}", __func__, item.extent_group_id,item.disks,item.extents.size(),insertion.data.size()); 

    //set value
    return memcached.set(std::move(insertion));
}


future<> metadata_service::commit_create_groups(std::vector<commit_create_params> create_params){
    if(create_params.empty()){
        return make_ready_future<>();
    }
    auto volume_id = create_params.at(0).volume_id;
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [this, create_params](auto& volume_service) mutable{
        std::vector<future<>> futures;
        // 1.update db
        for (auto create_parameter : create_params) {
            auto container_name = create_parameter.container_name;
            auto extent_group_id = create_parameter.extent_group_id;
            auto disk_id = create_parameter.disk_id;
            auto created = create_parameter.created;
            //1.update db
            auto f = _extent_group_map_helper.update_created(container_name, extent_group_id, disk_id, created).then([this, container_name, extent_group_id, disk_id, created](){
                //2.update cache
                return this->get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id,disk_id, created](auto item){
                    if (item.extent_group_id != "undefined") {
                        item.created = created;
                        return this->set_extent_group_to_cache(item).then([](auto success){
                            return make_ready_future<>();
                        });
                    } else {
                        //load db to cache
                        return this->get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id](auto entity_){
                            return  _extent_group_map_helper.get_extent_group_disks(container_name, extent_group_id).then([this, entity_](auto disk_ids){
                                auto cache_item = this->make_cache_item(entity_);
                                return this->set_extent_group_to_cache(std::move(cache_item)).then([](auto success){
                                    return make_ready_future<>();
                                });
                            });
                        });
                    }
                });
            });
            futures.push_back(std::move(f));
        }
        return when_all(futures.begin(), futures.end()).discard_result();
    });
}

future<> metadata_service::commit_write_groups(std::vector<commit_write_params>   write_params){
    if(write_params.empty()){
        return make_ready_future<>();
    }

    auto volume_id = write_params.at(0).volume_id;
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [this, write_params](auto& volume_service) mutable{
        std::vector<future<>> futures;
        // 1.update db
        for (auto write_parameter : write_params) {
            auto container_name = write_parameter.container_name;
            auto extent_group_id = write_parameter.extent_group_id;
            auto disk_id = write_parameter.disk_id;
            auto version = write_parameter.version;
            //1.update db
            auto f = _extent_group_map_helper.update_version(container_name, extent_group_id, disk_id, version).then([this, container_name, extent_group_id, disk_id, version](){
                //2.update cache
                return this->get_extent_group_from_cache(extent_group_id).then([this, container_name, extent_group_id,disk_id, version](auto item){
                    if (item.extent_group_id != "undefined") {
                        item.version= version;
                        return this->set_extent_group_to_cache(item).then([](auto success){
                            return make_ready_future<>();
                        });
                    } else {
                        //load db to cache
                        return this->get_extent_group_entity(container_name, extent_group_id).then([this, container_name, extent_group_id](auto entity_){
                            return  _extent_group_map_helper.get_extent_group_disks(container_name, extent_group_id).then([this, entity_](auto disk_ids){
                                auto cache_item = this->make_cache_item(entity_);
                                return this->set_extent_group_to_cache(std::move(cache_item)).then([](auto success){
                                    return make_ready_future<>();
                                });
                            });
                        });
                    }
                });
            }); 
            futures.push_back(std::move(f));
        }
        return when_all(futures.begin(), futures.end()).discard_result();
    });
}

future<> metadata_service::update_object_size(sstring object_id, uint64_t new_size){
    return _object_helper.update_object_size(object_id, new_size);
}

future<std::vector<volume_map_entity>> metadata_service::get_volume_metadata(sstring container_name, sstring volume_id){

    return _volume_map_helper.find_by_volume_id(container_name, volume_id);
}

future<> metadata_service::create_storage_map(std::vector<sstring> disks, sstring extent_group_id, sstring container_name){
    std::vector<future<>> futures;
    for(auto disk_id : disks) {
        auto f = _storage_map_helper.create(disk_id, extent_group_id, container_name);
        futures.push_back(std::move(f));
    }
    return when_all(futures.begin(), futures.end()).discard_result();
}


} //namespace hive
