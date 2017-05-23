#pragma once
#include "core/future.hh"
#include "core/distributed.hh"
#include "net/api.hh"
#include "db/config.hh"
#include <msgpack.hpp>
#include <map>

using namespace net;

namespace hive{

class abstract_trail;
class access_trail;

enum class vega_trail_type{
    UNKNOWN_LOG = 0,
    ACCESS_TRAIL,
};

using trail_data = std::unordered_map<std::string, std::string>;
using trail_ptr_map = std::map<vega_trail_type, shared_ptr<abstract_trail>>;

class trail_service : public seastar::async_sharded_service<trail_service> {
private:
    db::config _config; 
    trail_ptr_map ptr_map; 

    trail_data get_trail_data(udp_datagram dgram);
public:
    //static members
    static const sstring keyspace_name;
    static future<> init(db::config config);
    static future<> create_keyspace();
    static vega_trail_type get_trail_type(sstring table_name);
    static sstring         get_trail_name(vega_trail_type trail_type);
public:
    trail_service(db::config config);
    ~trail_service();
    future<> stop();

    future<> trace(udp_datagram dgram);
    future<> trace(trail_data  data);
    shared_ptr<abstract_trail> get_trail(vega_trail_type trail_type);
    shared_ptr<abstract_trail> get_trail(sstring table_name);
};


extern distributed<trail_service> _the_trail_service;

inline distributed<trail_service>& get_trail_service(){
    return _the_trail_service;
}

inline trail_service& get_local_trail_service(){
    return _the_trail_service.local();
}

}//namespace hive
