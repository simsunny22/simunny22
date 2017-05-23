#include "hive/trail/trail_service.hh"
#include "hive/trail/abstract_trail.hh"
#include "hive/trail/access_trail.hh"
#include "hive/hive_service.hh"
#include "hive/hive_tools.hh"

#include "database.hh"
#include "service/storage_proxy.hh"
#include "service/storage_service.hh"
#include "service/migration_manager.hh"
#include "log.hh"

namespace hive{
static logging::logger logger("trail_service");

distributed<trail_service> _the_trail_service;

// ==============================================================
// for static of trail_service
// ==============================================================
const sstring trail_service::keyspace_name = "vega_trail";
static const sstring access_trail_name     = "access_trail";

future<> trail_service::create_keyspace(){
    logger.debug("[{}] start", __func__);

    std::map<sstring, sstring> opts;
    opts["replication_factor"] = "1";
    bool durable_writes = false;  //not enable committrail we must set false

    auto ksm = keyspace_metadata::new_keyspace(
        trail_service::keyspace_name,
        "org.apache.cassandra.locator.SimpleStrategy",
        opts,
        durable_writes
    );

    try {
        auto& local_migration = service::get_local_migration_manager();
        return local_migration.announce_new_keyspace(ksm, false);
    }catch(const exceptions::already_exists_exception& ex){
        logger.warn("[create_keyspace] warn, {}", ex.what());  
    }catch(...){
        throw; 
    }

    return make_ready_future<>();
}

vega_trail_type trail_service::get_trail_type(sstring table_name){
    if(table_name == access_trail_name){
        return vega_trail_type::ACCESS_TRAIL; 
    }else {
        auto exception_info = sprint("unknow table name:%s, when trail_service::get_trail_type", table_name);
        throw exceptions::invalid_request_exception(exception_info);
    }
}

sstring trail_service::get_trail_name(vega_trail_type trail_type){
    switch (trail_type){
        case vega_trail_type::ACCESS_TRAIL:
            return access_trail_name;
        default:
            throw exceptions::invalid_request_exception(
                sprint("unknow trail type in trail_service::get_trail_name"));
    } 
}

future<> trail_service::init(db::config config){
    //if(config.vega_log_enable()){
        return seastar::async([config](){
            // 1. start trail_service
            auto& trail_service = hive::get_trail_service();
            trail_service.start(config).get();

            // 2. create_keyspace 
            trail_service::create_keyspace().get();

            auto& local_trail_service = hive::get_local_trail_service();

            // 3. create 'access_map' table
            auto access_trail = local_trail_service.get_trail(vega_trail_type::ACCESS_TRAIL);
            access_trail->create_trail().get();

        });
    //}
    return make_ready_future<>();
}

// ==============================================================
// for class trail_service
// ==============================================================
trail_service::trail_service(db::config config): _config(config){
    //std::cout << "trail_servie start ... cpu id:" << engine().cpu_id() << std::endl;
}

trail_service::~trail_service(){
}

future<> trail_service::stop(){
    return make_ready_future<>();
}

shared_ptr<abstract_trail> trail_service::get_trail(vega_trail_type trail_type){
    auto it = ptr_map.find(trail_type);
    if(it != ptr_map.end()){
        return it->second;
    }

    shared_ptr<abstract_trail> trail_ptr;
    switch (trail_type){
        case vega_trail_type::ACCESS_TRAIL:
            trail_ptr = make_shared<access_trail>(_config);
            break;
        default:
            throw exceptions::invalid_request_exception(
                sprint("unknow trail type in trail_service::get_trail"));
    } 

    auto ret = ptr_map.insert(std::make_pair(trail_type, trail_ptr));
    if(ret.second){
        return trail_ptr;
    }else{
        throw exceptions::invalid_request_exception(
            sprint("std::map insert failed in trail_service::get_trail"));
    }
}

shared_ptr<abstract_trail> trail_service::get_trail(sstring table_name) {
    auto trail_type = trail_service::get_trail_type(table_name);
    return get_trail(trail_type);
}

future<> trail_service::trace(udp_datagram dgram){
    if (_config.vega_log_enable()){
        auto data = get_trail_data(std::move(dgram));
        //for(auto& pair : params){
        //    logger.error("{}:{}", pair.first, pair.second);
        //}
        
        auto trail_ptr = get_trail(data["table_name"]);
        return trail_ptr->collect(std::move(data));
    }
    return make_ready_future<>();
}

future<> trail_service::trace(trail_data data){
    if (_config.vega_log_enable()){
        //for(auto& pair : params){
        //    logger.error("{}:{}", pair.first, pair.second);
        //}
        auto trail_ptr = get_trail(data["table_name"]);
        return trail_ptr->collect(std::move(data));
    }
    return make_ready_future<>();
}

// =====================================================
// private func
// =====================================================
trail_data trail_service::get_trail_data(udp_datagram dgram){
    trail_data data;
    packet& p = dgram.get_data();

    msgpack::unpacked unpack = msgpack::unpack(p.frag(0).base, p.frag(0).size);
    msgpack::object obj = unpack.get();

    obj.convert(data);
    return data;
}

}//hive

