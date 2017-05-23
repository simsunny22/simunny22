#ifndef __HIVE_STREAM_CACHE__
#define __HIVE_STREAM_CACHE__

#include <iostream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"

#include "hive/cache/datum.hh"
#include "hive/cache/datum_differ.hh"

namespace db {
    class config;
}

namespace hive {

class stream_cache: public seastar::async_sharded_service<stream_cache> {
private:
    uint64_t seq = 0;
    std::unique_ptr<db::config> _cfg;  

    future<lw_shared_ptr<datum>> do_cache_get(sstring key);
    future<bool> do_cache_set(sstring key, lw_shared_ptr<datum> data);
    future<bool> do_cache_del(sstring key);

public:
    stream_cache(const db::config& cfg);
    ~stream_cache();

    future<> stop();
    db::config& get_config(){ return *_cfg; }

    future<lw_shared_ptr<datum>> get(sstring key);
    future<bool> patch(sstring key, lw_shared_ptr<datum_differ> differ);
    future<bool> set(sstring key, lw_shared_ptr<datum> data);
 
}; //class stream_cache

extern distributed<stream_cache> _stream_cache;

inline distributed<stream_cache>& get_stream_cache() {
    return _stream_cache;
}

inline stream_cache& get_local_stream_cache() {
    return _stream_cache.local();
}

inline shared_ptr<stream_cache> get_local_shared_stream_cache() {
    return _stream_cache.local_shared();
}

} //namespace hive 

#endif
