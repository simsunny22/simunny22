#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "db/config.hh"
#include "core/shared_ptr.hh"
#include "log.hh"

#include "hive/object/object_driver.hh"
#include "hive/object/object_stream.hh"
#include "hive/hive_shared_mutex.hh"

namespace hive{

class object_service: public seastar::async_sharded_service<object_service> {
    using clock_type = std::chrono::steady_clock;
private:
    lw_shared_ptr<object_driver> _object_driver_ptr = nullptr;
    lw_shared_ptr<object_stream> _object_stream_ptr = nullptr;
    hive_shared_mutex _create_driver_lock;
    hive_shared_mutex _create_stream_lock;

    future<lw_shared_ptr<object_driver>> find_or_create_driver();
    future<lw_shared_ptr<object_stream>> find_or_create_stream();
public:
    object_service();
    ~object_service();
    
    future<> stop();

    unsigned shard_of(const sstring object_id);

    future<lw_shared_ptr<object_driver>> bind_object_driver();
    future<lw_shared_ptr<object_stream>> bind_object_stream();

    future<lw_shared_ptr<object_driver>> get_object_driver();
    future<lw_shared_ptr<object_stream>> get_object_stream();
};

extern distributed<object_service> _the_object_service;

inline distributed<object_service>& get_object_service() {
    return _the_object_service;
}

inline object_service& get_local_object_service() {
    return _the_object_service.local();
}

inline shared_ptr<object_service> get_local_shared_object_service() {
    return _the_object_service.local_shared();
}

}//namespace hive
