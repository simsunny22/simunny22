#ifndef __HIVE_CACHE__
#define __HIVE_CACHE__

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
#include "core/shared_mutex.hh"
#include "utils/histogram.hh"
#include <unistd.h>
#include <boost/intrusive_ptr.hpp>

#include "hive/cache/memory_cache.hh"
#include "hive/cache/item_key.hh"

namespace db {
    class config;
}

namespace hive {

class cache: public seastar::async_sharded_service<cache> {
private:
    memory_cache* _mem_cache = nullptr;
    std::unique_ptr<db::config> _cfg;     

    static const size_t max_lock_count = 128;
    std::unordered_map<size_t, seastar::shared_mutex> _rw_locks;

    inline unsigned get_cpu(const item_key& key) {
        return std::hash<item_key>()(key) % smp::count;
    }

    inline seastar::shared_mutex& get_rw_lock(const item_key& key){
        size_t lock_key = key.hash() % cache::max_lock_count;
        auto itor = _rw_locks.find(lock_key);
        if(itor != _rw_locks.end()){
            return itor->second; 
        }else{
            auto ret = _rw_locks.insert(std::make_pair(lock_key, seastar::shared_mutex()));
            if(ret.second){
                return ret.first->second;
            }else{
                throw std::runtime_error("[cache] insert _rw_locks failed");    
            }
        }
    }

public:
    cache(const db::config& cfg);
    ~cache();

    future<> init();
    future<> stop();
    future<> flush_all();
    db::config& get_config(){ return *_cfg; }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> set(item_insertion_data& insertion);

    // The caller must keep @key live until the resulting future resolves.
    future<item_ptr> get(const item_key& key);

    // The caller must keep @key live until the resulting future resolves.
    future<bool> remove(const item_key key);

    future<> remove(std::vector<sstring> keys);
    size_t size();

}; //class cache

extern distributed<cache> _cache;

inline distributed<cache>& get_cache() {
    return _cache;
}

inline cache& get_local_cache() {
    return _cache.local();
}

inline shared_ptr<cache> get_local_shared_cache() {
    return _cache.local_shared();
}

} //namespace hive 

#endif
