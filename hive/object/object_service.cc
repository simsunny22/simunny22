#include "log.hh"
#include "database.hh"
#include "core/distributed.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"

#include "hive/object/object_service.hh"
#include "hive/context/context_service.hh"

using namespace std::chrono_literals;

namespace hive {
static logging::logger logger("object_service");
distributed<object_service> _the_object_service;

object_service::object_service(){}
object_service::~object_service() {}

unsigned object_service::shard_of(const sstring object_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(object_id)));
    return dht::shard_of(token);
}

future<lw_shared_ptr<object_driver>> object_service::find_or_create_driver(){
    return with_hive_lock(_create_driver_lock, [this](){
        if(!_object_driver_ptr){
            _object_driver_ptr = make_lw_shared<object_driver>();
        }
        return make_ready_future<lw_shared_ptr<object_driver>>(_object_driver_ptr);
    });
}

future<lw_shared_ptr<object_stream>> object_service::find_or_create_stream(){
    return with_hive_lock(_create_stream_lock, [this](){
        if(!_object_stream_ptr){
            _object_stream_ptr = make_lw_shared<object_stream>();
        }
        return make_ready_future<lw_shared_ptr<object_stream>>(_object_stream_ptr);
    });
}

future<lw_shared_ptr<object_driver>> object_service::bind_object_driver(){
    return find_or_create_driver();
}

future<lw_shared_ptr<object_stream>> object_service::bind_object_stream(){
    return find_or_create_stream();
}

future<lw_shared_ptr<object_driver>> object_service::get_object_driver(){
    return make_ready_future<lw_shared_ptr<object_driver>>(_object_driver_ptr);
}

future<lw_shared_ptr<object_stream>> object_service::get_object_stream(){
    return make_ready_future<lw_shared_ptr<object_stream>>(_object_stream_ptr);
}

future<> object_service::stop() {
    return make_ready_future<>();
}

} //namespace hive
