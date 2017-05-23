#pragma once

#include "database.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "utils/histogram.hh"

#include "hive/stream/migrate_params_entry.hh"

namespace hive{
class migrate_forward_response_handler;

class migrate_forward_proxy : public seastar::async_sharded_service<migrate_forward_proxy> /*implements StorageProxyMBean*/ {
    using clock_type = std::chrono::steady_clock;
    struct rh_entry {
        std::unique_ptr<migrate_forward_response_handler> handler;
        timer<> expire_timer;
        rh_entry(std::unique_ptr<migrate_forward_response_handler>&& h, std::function<void()>&& cb);
    };

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        migrate_forward_proxy& p;
        unique_response_handler(migrate_forward_proxy& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        response_id_type release();
    };

public:
    struct stats {
        uint64_t migrate_count = 0;
    };
private:
    std::unique_ptr<db::config> _config;
    response_id_type _next_response_id = 1; // 0 is reserved for unique_response_handler
    std::unordered_map<response_id_type, rh_entry> _response_handlers;
    stats _stats;

private:
    void init_messaging_service();
    void uninit_messaging_service();

    response_id_type register_response_handler(std::unique_ptr<migrate_forward_response_handler>&& h);
    void             remove_response_handler(response_id_type id);
    
    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    migrate_forward_response_handler&   get_response_handler(migrate_forward_proxy::response_id_type id);
    void got_response(response_id_type response_handler_id, gms::inet_address reply_from);
    
    void trigger_exception(response_id_type response_handler_id
                         , gms::inet_address reply_from
                         , sstring exception);

    future<> do_action_prepare(migrate_params_entry migrate_params
        , uint32_t timeout_in_ms
        , std::function<void (migrate_forward_proxy::response_id_type, clock_type::time_point)> action_fun);

    future<> do_action_done(future<> result_fut, utils::latency_counter lc);

    void do_migrate_forward(migrate_forward_proxy::response_id_type response_id
        , clock_type::time_point timeout
        , migrate_params_entry migrate_params);

    future<> migrate_forward_extent_group_locally(migrate_params_entry migrate_params);
    future<> migrate_forward_extent_journal_locally(migrate_params_entry migrate_params);
    future<bool> is_driver_node(sstring volume_id);
public:
    migrate_forward_proxy(db::config config);
    ~migrate_forward_proxy();
    future<> stop();

    std::unique_ptr<db::config>& get_config() {
        return _config;
    }
    
    const stats& get_stats() const {
        return _stats;
    }
    
    unsigned shard_of(const sstring extent_group_id);

    future<> migrate_forward(migrate_params_entry migrate_params);
    future<> migrate_forward_locally(migrate_params_entry migrate_params);

    friend class migrate_forward_response_handler;
};

extern distributed<migrate_forward_proxy> _the_migrate_forward_proxy;

inline distributed<migrate_forward_proxy>& get_migrate_forward_proxy() {
    return _the_migrate_forward_proxy;
}

inline migrate_forward_proxy& get_local_migrate_forward_proxy() {
    return _the_migrate_forward_proxy.local();
}

inline shared_ptr<migrate_forward_proxy> get_local_shared_migrate_forward_proxy() {
    return _the_migrate_forward_proxy.local_shared();
}

}//namespace hive
