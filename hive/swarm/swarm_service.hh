#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "utils/histogram.hh"
#include "seastar/core/shared_ptr.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/message_data_type.hh"
#include "log.hh"
#include "drain_minion.hh"


namespace hive{

class swarm_service: public seastar::async_sharded_service<swarm_service> {
    using clock_type = std::chrono::steady_clock;
private:
    lw_shared_ptr<drain_minion> _drain_minion = nullptr; 
public:
    swarm_service();
    ~swarm_service();

    future<> stop();

    // drain task
    future<smd_drain_task_group>
      perform_drain(smd_drain_task_group task_group);

}; //class swarm_service


extern distributed<swarm_service> _the_swarm_service;

inline distributed<swarm_service>& get_swarm_service() {
    return _the_swarm_service;
}

inline swarm_service& get_local_swarm_service() {
    return _the_swarm_service.local();
}

inline shared_ptr<swarm_service> get_local_shared_swarm_service() {
    return _the_swarm_service.local_shared();
}

}//namespace hive
