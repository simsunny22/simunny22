#include "system_service.hh"

namespace hive{
static logging::logger logger("system_service");

distributed<system_service> _the_system_service;

// ===========================================
// public 
// ===========================================
system_service::system_service(lw_shared_ptr<db::config> conf):_conf(conf){
   start();
}

system_service::~system_service(){
}

future<> system_service::stop(){
    return make_ready_future<>();
}

lw_shared_ptr<system_time> system_service::get_system_time_ptr(){
    return _time_ptr;
}
// ===========================================
// private 
// ===========================================
void system_service::start(){
    _time_ptr = make_lw_shared<system_time>(_conf);
}

}//hive
