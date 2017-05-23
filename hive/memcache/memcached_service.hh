#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "core/shared_ptr.hh"
#include "utils/histogram.hh"
#include "log.hh"
#include "memcache.hh"

namespace memcache {

class memcached_service : public seastar::async_sharded_service<memcached_service> {
private:
    lw_shared_ptr<cache> _cache;

    inline
    unsigned get_cpu(item_key& key) {
        return std::hash<item_key>()(key) % smp::count;
    }
public:
    memcached_service();
    
    memcached_service& operator=(memcached_service&&) = delete;
    memcached_service& operator=(const memcached_service&) = delete;

    ~memcached_service();

    future<> stop();

    // interfaces
    future<> flush_all();

    future<> flush_at(uint32_t time);

    clock_type::duration get_wc_to_clock_type_delta();

    future<bool> set(item_insertion_data insertion);

    future<bool> add(item_insertion_data insertion);

    future<bool> replace(item_insertion_data insertion);

    future<bool> remove(item_key key);

    future<item_ptr> get(item_key key);

    future<cas_result> cas(item_insertion_data insertion, item::version_type version);

    future<std::pair<item_ptr, bool>> incr(item_key key, uint64_t delta);

    future<std::pair<item_ptr, bool>> decr(item_key key, uint64_t delta);


}; //class memcached_service


extern distributed<memcached_service> _the_memcached_service;

inline distributed<memcached_service>& get_memcached_service() {
    return _the_memcached_service;
}

inline memcached_service& get_local_memcached_service() {
    return _the_memcached_service.local();
}

inline shared_ptr<memcached_service> get_local_shared_memcached_service() {
    return _the_memcached_service.local_shared();
}

}//namespace memcache
