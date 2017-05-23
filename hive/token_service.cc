#include "hive/token_service.hh"
#include "log.hh"

#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"

namespace hive {

static logging::logger logger("token_service");

distributed<token_service> _the_token_service;
using namespace exceptions;

token_service::~token_service() {}
token_service::token_service(lw_shared_ptr<db::config> config) 
: _config(config)
, _memory_size(0)
, _memory_sem(0)
, _volume_balance_enable(false)
{
    logger.info("constructor start, cpu_id:{}", engine().cpu_id());
    init();
}

void token_service::init() {
    auto memtable_space = 0;
    auto _volume_balance_enable = _config->enable_volume_balance();
    auto volume_token_timeout_in_s = _config->volume_token_timeout_in_s();
    auto _volume_token_timeout = std::chrono::seconds(volume_token_timeout_in_s);
    auto config_memtable_space = _config->token_size_per_cpu_in_mb() << 20;
    assert(config_memtable_space > 0);   
    
    auto total_memory = memory::stats().total_memory();
    memtable_space = std::min(config_memtable_space, uint64_t(total_memory*0.8));

    auto reserved_token_size = memtable_space * _config->reserved_token_size_ratio();

    auto print_info = sprint("==> shard:%d, volume balance enable:%s, ultimate memtable space:%ld, the config is:%ld, reserved space:%ld\n"
        , engine().cpu_id(), _volume_balance_enable ? "true":"false", memtable_space, config_memtable_space, (uint64_t)reserved_token_size);
    std::cout << print_info;
    if(_volume_balance_enable){
        _memory_size = memtable_space - reserved_token_size;
    } else {
        _memory_size = memtable_space;
    }
    _memory_sem.signal(memtable_space);
    set_timer();
}

//whoops
void token_service::set_timer(){
    // 1. token_wipe
    uint64_t periodic_token_check = _config->periodic_token_check_in_ms();
    _token_check_timer.set_callback(std::bind(&token_service::unregister_expired_volume, this));
    _token_check_timer.arm(lowres_clock::now(), std::experimental::optional<lowres_clock::duration>{
        std::chrono::milliseconds(periodic_token_check)});

    // 2. print_token_count
    uint64_t periodic_print_token_count = _config->periodic_print_token_count_in_s();
    if (periodic_print_token_count > 0) {
        _token_print_timer.set_callback(std::bind(&token_service::print_token_count, this));
        _token_print_timer.arm(lowres_clock::now(), std::experimental::optional<lowres_clock::duration>{
            std::chrono::seconds(periodic_print_token_count)});
    }
}

future<> token_service::stop() {
    return make_ready_future<>();
}

unsigned token_service::shard_of(const sstring volume_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(volume_id)));
    return dht::shard_of(token);
}

void token_service::unregister_expired_volume(){
    for(auto itor = _volume_registry.begin(); itor != _volume_registry.end(); itor++){
        if(0 == itor->second.allocated){
            _volume_registry.erase(itor);
            if(_volume_registry.size() > 0){
                uint64_t ulimit = _memory_size - (_volume_registry.size()-1) * ( _memory_size/_volume_registry.size() );
                adjust_ulimit(false);
                _volume_registry.begin()->second.ulimit = ulimit;
            }
        }
    }
}

void token_service::print_token_count(){
    logger.error("[{}] periodic_print_token_count, current count : {}", __func__,  _memory_sem.current());
}

uint64_t check_size(uint64_t size){
    return size <= 0 ? 0 : size;
}

token_service::volume_mem_usage 
token_service::make_volume_mem_usage(uint64_t ulimit, uint64_t allocated, uint64_t timestamp){
    token_service::volume_mem_usage usage = {ulimit, allocated, timestamp};
    return usage;
}

bool token_service::is_registered(sstring volume_id){
    auto itor = _volume_registry.find(volume_id);
    return itor != _volume_registry.end();
}

void token_service::adjust_ulimit(bool is_register){
    auto new_ulimit = is_register ? _memory_size/(_volume_registry.size()+1) : _memory_size/(_volume_registry.size());
    for(auto iter = _volume_registry.begin(); iter != _volume_registry.end(); iter++){
        iter->second.ulimit = new_ulimit;
    }
}

void token_service::register_volume(sstring volume_id){
    uint64_t ulimit = _memory_size - _volume_registry.size()*( _memory_size/(_volume_registry.size() + 1) );
    auto ilimit = _config->inferior_limit_token_size_per_volume_in_mb() << 20;
    if(ulimit < ilimit){
        sstring warn_info = "error:insufficient memory, shard:" + to_sstring(engine().cpu_id()) 
                           + " upper limit is lower than inferior limit "
                           + " volume_id:" + volume_id 
                           + " ulimit:" + to_sstring(ulimit)
                           + " ilimit:" + to_sstring(ilimit);
        logger.warn(warn_info.c_str());
        throw memory_insufficient_exception(warn_info.c_str());
    }
    adjust_ulimit(true);
    auto usage = make_volume_mem_usage(ulimit, 0, hive_tools::get_current_time_in_ms());
    auto ret = _volume_registry.insert(std::make_pair(volume_id, usage));
    if (!ret.second){
        throw std::runtime_error("token_service::register_volume _volume_registry insert failed");
    }
}

void token_service::volume_balance_when_throttle(sstring volume_id, uint64_t size){
    if(!is_registered(volume_id)){
        register_volume(volume_id);
    }
    auto& usage = _volume_registry.find(volume_id)->second; 
    if(usage.allocated + size > usage.ulimit){
        sstring warn_info = "error:insufficient memory, shard:" + to_sstring(engine().cpu_id()) 
                           + " volume_id:" + volume_id 
                           + " size:" + to_sstring(size)
                           + " ulimit:" + to_sstring(usage.ulimit)
                           + " allocated:" + to_sstring(usage.allocated);
        logger.warn(warn_info.c_str());
        throw memory_insufficient_exception(warn_info.c_str());
    }else{
        usage.timestamp = hive_tools::get_current_time_in_ms();
        usage.allocated += size;
    }
}

future<bool> token_service::throttle_memory(sstring volume_id, uint64_t size){
    auto claim_size = check_size(size);
    if(_volume_balance_enable){
        volume_balance_when_throttle(volume_id, claim_size);
    }
    return _memory_sem.wait(_volume_token_timeout, claim_size).then([this, volume_id, claim_size]{
        logger.debug("token_service, throttle_memory, leave...., _memory_sem:{}", _memory_sem.current());
        return make_ready_future<bool>(true);
    });
}

void token_service::volume_balance_when_unthrottle(sstring volume_id, uint64_t reclaim_size){
    if(!is_registered(volume_id)){
        sstring error_info = "error:unregistered volume, shard:" 
                            + to_sstring(engine().cpu_id()) 
                            + " volume_id:" + volume_id 
                            + " size:" + to_sstring(reclaim_size);
        logger.error(error_info.c_str());
        return;
    }
    auto& usage = _volume_registry.find(volume_id)->second; 

    usage.allocated = usage.allocated - reclaim_size;
    usage.timestamp = hive_tools::get_current_time_in_ms();
}

void token_service::unthrottle_memory(sstring volume_id, uint64_t size) {
    auto reclaim_size = check_size(size);
    if(_volume_balance_enable){
        volume_balance_when_unthrottle(volume_id, reclaim_size);
    }
    logger.debug("token_service, unthrottle_memory, current_sem_num:{}, reclaim:{}", _memory_sem.current(), reclaim_size);
    _memory_sem.signal(reclaim_size);
}
//whoops

} //namespace hive
