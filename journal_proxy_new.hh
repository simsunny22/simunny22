#ifndef __HIVE_JOURNAL_PROXY_NEW__
#define __HIVE_JOURNAL_PROXY_NEW__
#include "database.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "core/distributed.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "utils/histogram.hh"
//#include "sstables/estimated_histogram.hh"

#include "hive/commitlog/replay_position.hh"
#include "hive/hive_request.hh"
#include "hive/context/volume_context_entry.hh"

namespace hive {

class abstract_journal_response_handler;
//class abstract_journal_data;
class smd_init_commitlog;
class smd_sync_commitlog;
class smd_discard_commitlog;

class journal_abstract_read_executor;
class journal_abstract_executor;

class journal_proxy_new : public seastar::async_sharded_service<journal_proxy_new> {
    using clock_type = std::chrono::steady_clock;
    struct rh_entry {
        std::unique_ptr<abstract_journal_response_handler> handler;
        timer<> expire_timer;
        rh_entry(std::unique_ptr<abstract_journal_response_handler>&& h, std::function<void()>&& cb);
    };

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        journal_proxy_new& p;
        unique_response_handler(journal_proxy_new& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        response_id_type release();
    };

public:
    struct stats {
        //create commitlog
        uint64_t received_creates         = 0;
        uint64_t forwarded_creates        = 0;
        uint64_t forwarding_create_errors = 0;

        //sync commitlog
        uint64_t received_syncs         = 0;
        uint64_t forwarded_syncs        = 0;
        uint64_t forwarding_sync_errors = 0;
        
        //discard commitlog
        uint64_t received_discards         = 0;
        uint64_t forwarded_discards        = 0;
        uint64_t forwarding_discard_errors = 0;
    };
private:
    response_id_type _next_response_id = 1; // 0 is reserved for unique_response_handler
    std::unordered_map<response_id_type, rh_entry> _response_handlers;

    stats _stats;

private:
    void init_messaging_service();
    void uninit_messaging_service();
    
    response_id_type register_response_handler(std::unique_ptr<abstract_journal_response_handler>&& h);
    abstract_journal_response_handler& get_response_handler(journal_proxy_new::response_id_type id);
    void remove_response_handler(response_id_type id);

    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    future<> response_end(future<> result, utils::latency_counter lc);
    void got_response(response_id_type id, gms::inet_address from);

    //init commitlog
    response_id_type create_init_commitlog_response_handler(
        smd_init_commitlog data, 
        std::unordered_set<gms::inet_address> targets);
    
    void init_commitlog_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> init_commitlog_locally(gms::inet_address from, smd_init_commitlog& data);

    //sync commitlog
    response_id_type create_sync_commitlog_response_handler(
        smd_sync_commitlog data, 
        std::unordered_set<gms::inet_address> targets);
    
    void sync_commitlog_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> sync_commitlog_locally(gms::inet_address from, smd_sync_commitlog& data);

    //discard commitlog
    response_id_type create_discard_commitlog_response_handler(
        smd_discard_commitlog data,
        std::unordered_set<gms::inet_address> targets);

    void discard_commitlog_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> discard_secondary_commitlog_locally(gms::inet_address from, smd_discard_commitlog& data);

public:
    journal_proxy_new();
    ~journal_proxy_new();

    future<> stop();
    const stats& get_stats() const {
        return _stats;
    }

    future<> init_commitlog(smd_init_commitlog data, std::unordered_set<gms::inet_address> targets); 
    future<> sync_commitlog(smd_sync_commitlog data, std::unordered_set<gms::inet_address> targets); 
    future<> discard_commitlog(smd_discard_commitlog data, std::unordered_set<gms::inet_address> targets);

    //future<std::vector<extent_revision>> query(hive_read_command read_cmd);

    friend class abstract_journal_response_handler;

}; //journal_proxy_new

extern distributed<journal_proxy_new> _the_journal_proxy_new;

inline distributed<journal_proxy_new>& get_journal_proxy_new() {
    return _the_journal_proxy_new;
}

inline journal_proxy_new& get_local_journal_proxy_new() {
    return _the_journal_proxy_new.local();
}

inline shared_ptr<journal_proxy_new> get_local_shared_journal_proxy_new() {
    return _the_journal_proxy_new.local_shared();
}

}//namespace hive

#endif 

