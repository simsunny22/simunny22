#include "hive/cache/cache.hh" 

#include <iostream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/align.hh"
#include <unistd.h>
#include <boost/intrusive_ptr.hpp>

#include "db/config.hh"
#include "log.hh"

namespace hive {

distributed<cache> _cache;
//static std::string make_cache_key(sstring volume_id, uint64_t offset) {
//    std::string off_str = boost::lexical_cast<std::string>(offset);
//    std::string key_str = std::string(volume_id.begin(), volume_id.size()) + ":" + off_str;
//    return key_str;
//}

cache::cache(const db::config& cfg)
: _cfg(std::make_unique<db::config>(cfg)){
    _mem_cache = new memory_cache(cfg);
}

cache::~cache(){
    if(nullptr != _mem_cache){
        delete _mem_cache;    
    }
}

future<> cache::init(){
    return _mem_cache->init();
}

future<> cache::stop() {
    return _mem_cache->stop();
}

future<> cache::flush_all() {
    return _mem_cache->flush_all();
}

size_t cache::size(){
    return _mem_cache->size();
}

future<bool> cache::set(item_insertion_data& insertion) {
    auto& rw_lock = get_rw_lock(insertion.key);
    return with_lock(rw_lock, [this, &insertion](){
        return _mem_cache->set(insertion);
    });
}

future<item_ptr> cache::get(const item_key& key) {
    auto& rw_lock = get_rw_lock(key);
    return with_shared(rw_lock, [this, &key](){
        return _mem_cache->get(key);
    });
}

future<bool> cache::remove(const item_key key) {
    auto& rw_lock = get_rw_lock(key);
    return with_lock(rw_lock, [this, key](){
        return _mem_cache->remove(key);
    });
}

future<> cache::remove(std::vector<sstring> keys) {
  return parallel_for_each(keys, [this] (auto key_) {
      item_key key{key_};
      return this->remove(key).then([key] (auto res) {
          //tododl::red how to use this res?
          return make_ready_future<>();
      });
  });
}

/*
* public functions
*/


} //namespace hive 

