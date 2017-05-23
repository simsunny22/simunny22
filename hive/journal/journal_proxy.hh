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
#include "hive/hive_request.hh"
#include "hive/context/volume_context_entry.hh"
#include "hive/response_handler.hh"
#include "hive/journal/volume_revision.hh"
#include "hive/journal/revision_data.hh"

namespace hive {

//class abstract_journal_response_handler;
//class abstract_journal_data;
class smd_init_segment;
class smd_sync_segment;
class smd_discard_segment;
class smd_get_journal_data;
class rmd_get_journal_data;

class journal_abstract_read_executor;
class journal_abstract_executor;

class journal_proxy : public seastar::async_sharded_service<journal_proxy> {
    using clock_type = std::chrono::steady_clock;
    struct rh_entry {
        //std::unique_ptr<abstract_journal_response_handler> handler;
        std::unique_ptr<common_response_handler> handler;
        timer<> expire_timer;
        //rh_entry(std::unique_ptr<abstract_journal_response_handler>&& h, std::function<void()>&& cb);
        rh_entry(std::unique_ptr<common_response_handler>&& h, std::function<void()>&& cb);
    };

    using response_id_type = uint64_t;
    struct unique_response_handler {
        response_id_type id;
        journal_proxy& p;
        unique_response_handler(journal_proxy& p_, response_id_type id_);
        unique_response_handler(const unique_response_handler&) = delete;
        unique_response_handler& operator=(const unique_response_handler&) = delete;
        unique_response_handler(unique_response_handler&& x);
        ~unique_response_handler();
        response_id_type release();
    };

public:
    struct stats {
        //create segment
        uint64_t received_creates         = 0;
        uint64_t forwarded_creates        = 0;
        uint64_t forwarding_create_errors = 0;

        //sync segment
        uint64_t received_syncs         = 0;
        uint64_t forwarded_syncs        = 0;
        uint64_t forwarding_sync_errors = 0;
        
        //discard segment
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
    
    //response_id_type register_response_handler(std::unique_ptr<abstract_journal_response_handler>&& h);
    response_id_type register_response_handler(std::unique_ptr<common_response_handler>&& h);
    //abstract_journal_response_handler& get_response_handler(journal_proxy::response_id_type id);
    common_response_handler& get_response_handler(journal_proxy::response_id_type id);
    void remove_response_handler(response_id_type id);

    future<> response_wait(response_id_type id, clock_type::time_point timeout);
    future<> response_end(future<> result, utils::latency_counter lc);
    void got_response(response_id_type id, gms::inet_address from);

    //init segment
    response_id_type create_init_segment_response_handler(
        smd_init_segment data, 
        std::unordered_set<gms::inet_address> targets);
    
    void init_segment_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> init_segment_locally(gms::inet_address from, smd_init_segment& data);

    //sync segment
    response_id_type create_sync_segment_response_handler(
        smd_sync_segment data, 
        std::unordered_set<gms::inet_address> targets);
    
    void sync_segment_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> sync_segment_locally(gms::inet_address from, smd_sync_segment& data);

    //discard segment
    response_id_type create_discard_segment_response_handler(
        smd_discard_segment data,
        std::unordered_set<gms::inet_address> targets);

    void discard_segment_to_live_endpoints(response_id_type response_id, clock_type::time_point timeout);
    future<> discard_secondary_segment_locally(gms::inet_address from, smd_discard_segment& data);

    //get segment data
    future<foreign_ptr<lw_shared_ptr<rmd_get_journal_data>>>
    get_journal_data_locally(smd_get_journal_data smd_data);

    future<std::map<sstring, std::vector<revision_data>>>
    do_get_journal_data(
        sstring volume_id
        , sstring segment_id
        , uint64_t shard
        , std::map<sstring, std::vector<extent_group_revision>> revisions);
    


public:
    journal_proxy();
    ~journal_proxy();

    future<> stop();
    const stats& get_stats() const {
        return _stats;
    }

    future<> init_segment(smd_init_segment data, std::unordered_set<gms::inet_address> targets); 
    future<> sync_segment(smd_sync_segment data, std::unordered_set<gms::inet_address> targets); 
    future<> discard_segment(smd_discard_segment data, std::unordered_set<gms::inet_address> targets);

    future<rmd_get_journal_data>
    get_journal_data(smd_get_journal_data request_data, gms::inet_address target);


    friend class abstract_journal_response_handler;

}; //journal_proxy

extern distributed<journal_proxy> _the_journal_proxy;

inline distributed<journal_proxy>& get_journal_proxy() {
    return _the_journal_proxy;
}

inline journal_proxy& get_local_journal_proxy() {
    return _the_journal_proxy.local();
}

inline shared_ptr<journal_proxy> get_local_shared_journal_proxy() {
    return _the_journal_proxy.local_shared();
}

}//namespace hive

#endif 

