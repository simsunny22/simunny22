#pragma once
#include "core/semaphore.hh"
#include "core/distributed.hh"
#include "utils/histogram.hh"
#include "seastar/core/shared_ptr.hh"
#include "log.hh"
#include <mutex> 
#include <unordered_map>
#include <stdexcept>

#include "hive/journal/primary_journal.hh"
#include "hive/commitlog/hive_commitlog.hh"
#include "hive/volume_driver.hh"
#include "hive/drain/drain_manager.hh"
#include "hive/hive_shared_mutex.hh"

namespace hive{

class memory_insufficient_exception:public std::runtime_error{
public:
    explicit memory_insufficient_exception(const std::string &s) : std::runtime_error(s) {}
};

class token_service: public seastar::async_sharded_service<token_service> {
private:
    lw_shared_ptr<db::config> _config;
    uint64_t _memory_size;
    semaphore _memory_sem;
    bool _volume_balance_enable;
    timer<>::duration  _volume_token_timeout;

    //whoops for volume memory balance
    timer<lowres_clock> _token_check_timer;
    timer<lowres_clock> _token_print_timer;
    struct volume_mem_usage{
        uint64_t ulimit;
        uint64_t allocated;
        uint64_t timestamp;
    };
    std::map<sstring, volume_mem_usage> _volume_registry;

    hive_shared_mutex _mutex;

    void volume_balance_when_throttle(sstring volume_id, uint64_t claim_size);
    void volume_balance_when_unthrottle(sstring volume_id, uint64_t reclaim_size);
    bool is_registered(sstring volume_id);
    void register_volume(sstring volume_id);
    volume_mem_usage make_volume_mem_usage(uint64_t ulimit, uint64_t allocated, uint64_t timestamp); 
    void adjust_ulimit(bool is_register);
    void set_timer();
    void unregister_expired_volume();
    void print_token_count();
    //whoops

    void init();
public:
    token_service(lw_shared_ptr<db::config> config);
    ~token_service();

    future<> stop();

    unsigned shard_of(const sstring volume_id);

    future<bool> throttle_memory(sstring volume_id, uint64_t size);
    void unthrottle_memory(sstring volume_id, uint64_t size);
    
}; //class token_service

extern distributed<token_service> _the_token_service;

inline distributed<token_service>& get_token_service() {
    return _the_token_service;
}

inline token_service& get_local_token_service() {
    return _the_token_service.local();
}

}//namespace hive
