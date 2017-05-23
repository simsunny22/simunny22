#pragma once
#include "core/future.hh"
#include "core/distributed.hh"
#include "db/config.hh"
#include "log.hh"
#include "system_time.hh"


namespace hive{

class system_service : public seastar::async_sharded_service<system_service> {
private:
    lw_shared_ptr<db::config> _conf = nullptr;
    lw_shared_ptr<system_time> _time_ptr = nullptr;

public:
    system_service(lw_shared_ptr<db::config> conf);
    ~system_service();
    future<> stop();
    lw_shared_ptr<system_time> get_system_time_ptr();

private:
    void start();
};


extern distributed<system_service> _the_system_service;

inline distributed<system_service>& get_system_service(){
    return _the_system_service;
}

inline system_service& get_local_system_service(){
    return _the_system_service.local();
}

}//namespace hive
