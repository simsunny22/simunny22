#pragma once

#include "database.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "utils/histogram.hh"

#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/hive_shared_mutex.hh"
#include "hive/extent_revision_set.hh"
#include "hive/stream/migrate_params_entry.hh"

#include "hive/response_handler.hh"
#include "hive/extent_store.hh"
#include "hive/context/context_service.hh"

namespace hive{

class extent_store_proxy : public seastar::async_sharded_service<extent_store_proxy> {
    using clock_type = std::chrono::steady_clock;
    struct rh_entry {
        std::unique_ptr<common_response_handler> handler;
        timer<> expire_timer;
        rh_entry(std::unique_ptr<common_response_handler>&& h, std::function<void()>&& cb);
    };

    //tododl:yellow maybe we not need this handler??
    struct unique_response_handler {
        uint64_t id;
        extent_store_proxy& p;
        unique_response_handler(extent_store_proxy& p_, uint64_t id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        uint64_t release();
    };

public:
    struct stats {
        //create_extent_group 
        uint64_t received_creates         = 0;
        uint64_t forwarded_creates        = 0;
        uint64_t forwarding_create_errors = 0;

        //delete_extent_group 
        uint64_t received_deletes         = 0;
        uint64_t forwarded_deletes        = 0;
        uint64_t forwarding_delete_errors = 0;

        //rwrite_extent_group 
        uint64_t received_rwrites         = 0;
        uint64_t forwarded_rwrites        = 0;
        uint64_t forwarding_rwrite_errors = 0;
        
        //migrate_extent_group 
        uint64_t received_migrates         = 0;
        uint64_t forwarded_migrates        = 0;
        uint64_t forwarding_migrate_errors = 0;

        //hit 
        uint64_t read_requests        = 0;
        uint64_t local_ssd_hit_count  = 0;
        uint64_t local_hdd_hit_count  = 0;
        uint64_t remote_ssd_hit_count = 0;
        uint64_t remote_hdd_hit_count = 0;
    };
private:
    std::unique_ptr<db::config> _config;
    distributed<extent_store>& _extent_store;
    uint64_t _next_response_id = 1; // 0 is reserved for unique_response_handler??
    std::unordered_map<uint64_t, rh_entry> _response_handlers;
    stats _stats;

    //for monad on extent_group
    std::map<sstring, hive_shared_mutex> _monad_queue;
    timer<lowres_clock> _timer;

private:
    void on_timer();
    void wash_monad_queue();

    void init_messaging_service();
    void uninit_messaging_service();

    uint64_t register_response_handler(std::unique_ptr<common_response_handler>&& h);
    common_response_handler& get_response_handler(uint64_t id);
    void remove_response_handler(uint64_t response_id);
    void got_response(uint64_t response_id, gms::inet_address from);

    future<response_entry> response_wait(uint64_t response_id, clock_type::time_point timeout);
    future<response_entry> response_end(future<response_entry> result, utils::latency_counter lc);

    void cancel_response_handler_timeout(uint64_t response_id);

    future<disk_context_entry> filter_local_disk_context(std::vector<sstring> disk_ids);
    future<disk_context_entry> check_disk_belong_to_me(sstring disk_id); 

    future<gms::inet_address> get_addr_by_disk_id(sstring disk_id);

    //create extent group
    uint64_t build_create_extent_group_response_handler(
        smd_create_extent_group create_data
        , std::unordered_set<gms::inet_address> targets);

    void create_extent_group_on_all_nodes(
        uint64_t response_id
        , clock_type::time_point timeout);

    future<> create_extent_group_locally(smd_create_extent_group& create_data);

    //delete extent group
    uint64_t build_delete_extent_group_response_handler(
        smd_delete_extent_group delete_data
        , std::unordered_set<gms::inet_address> targets);

    void delete_extent_group_on_all_nodes(
        uint64_t response_id
        , clock_type::time_point timeout);

    future<> delete_extent_group_locally(smd_delete_extent_group& delete_data);

    //rwrite extent group
    uint64_t build_rwrite_extent_group_response_handler(
        smd_rwrite_extent_group rwrite_data
        , std::unordered_set<gms::inet_address> targets);

    void rwrite_extent_group_on_all_nodes(
        uint64_t response_id
        , clock_type::time_point timeout);

    future<> rwrite_extent_group_locally(smd_rwrite_extent_group& rwrite_data);

    //read extent group
    future<foreign_ptr<lw_shared_ptr<rmd_read_extent_group>>>
    read_extent_group_locally(smd_read_extent_group read_data); 

    //migrate extent group
    uint64_t build_migrate_extent_group_response_handler(
        smd_migrate_extent_group migrate_data
        , std::unordered_set<gms::inet_address> targets);

    void migrate_extent_group_on_all_nodes(
        uint64_t response_id
        , clock_type::time_point timeout);

    future<> migrate_extent_group_locally(smd_migrate_extent_group& migrate_data);


    void hit_rate(sstring hit_disk_id);

    //get extent group
    future<sstring> get_local_disk_mount_path(sstring volume_id, sstring extent_group_id);
    
    future<foreign_ptr<lw_shared_ptr<rmd_get_extent_group>>>
    get_extent_group_locally(smd_get_extent_group smd_data);

public:
    extent_store_proxy(const db::config config, distributed<extent_store>& extent_store);
    ~extent_store_proxy();
    future<> stop();

    std::unique_ptr<db::config>& get_config() {
        return _config;
    }

    distributed<extent_store>& get_extent_store() {
        return _extent_store;
    }
    
    const stats& get_stats() const {
        return _stats;
    }
    
    unsigned shard_of(const sstring extent_group_id);

    //for monad
    hive_shared_mutex& get_or_create_mutex(sstring extent_group_id);
    size_t get_monad_waiters_count(sstring extent_group_id);

    template <typename Func>
    futurize_t<std::result_of_t<Func ()>> with_monad(sstring extent_group_id, Func&& func){
        auto& mutex = get_or_create_mutex(extent_group_id);
        return with_hive_lock(mutex, std::forward<Func>(func));
    }

    //create
    future<response_entry> 
    create_extent_group(smd_create_extent_group create_data, std::unordered_set<gms::inet_address> targets);
    
    //delete
    future<response_entry> 
    delete_extent_group(smd_delete_extent_group delete_data, std::unordered_set<gms::inet_address> targets);
    
    //rwrite
    future<response_entry> 
    rwrite_extent_group(smd_rwrite_extent_group rwrite_data, std::unordered_set<gms::inet_address> targets);

    //read
    future<foreign_ptr<lw_shared_ptr<rmd_read_extent_group>>>
    read_extent_group(hive_read_subcommand read_subcmd, bool read_object = false);

    //migrate
    future<response_entry> migrate_extent_group(smd_migrate_extent_group migrate_data, std::unordered_set<gms::inet_address> targets);

    //for drain add
    future<rmd_get_extent_group>
    get_extent_group(smd_get_extent_group request_data, gms::inet_address target);

    friend class common_response_handler;
};//class extent_store_proxy

extern distributed<extent_store_proxy> _the_extent_store_proxy;

inline distributed<extent_store_proxy>& get_extent_store_proxy() {
    return _the_extent_store_proxy;
}

inline extent_store_proxy& get_local_extent_store_proxy() {
    return _the_extent_store_proxy.local();
}

inline shared_ptr<extent_store_proxy> get_local_shared_extent_store_proxy() {
    return _the_extent_store_proxy.local_shared();
}

}//namespace hive
