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

#include "hive/extent_store.hh"
#include "hive/context/context_service.hh"
#include "hive/message_data_type.hh"

namespace hive{

using clock_type = std::chrono::steady_clock;

class swarm_proxy : public seastar::async_sharded_service<swarm_proxy> {
public:
    struct stats {
        //drain tasks
        uint64_t received_drain_tasks          = 0;
        uint64_t forwarded_drain_tasks         = 0;
        uint64_t forwarding_drain_tasks_errors = 0;
    };
private:
    stats _stats;
    timer<lowres_clock> _timer;

private:
    void init_messaging_service();
    void uninit_messaging_service();

    future<smd_drain_task_group> perform_drain_locally(smd_drain_task_group task_group);

    future<> drain_task_group_done(smd_drain_task_group result);

public:
    swarm_proxy();
    ~swarm_proxy();
    future<> stop();

    const stats& get_stats() const {
        return _stats;
    }

    //drain tasks
    future<>
      perform_drain(smd_drain_task_group task, gms::inet_address target, clock_type::time_point timeout);
};//class swarm_proxy

extern distributed<swarm_proxy> _the_swarm_proxy;

inline distributed<swarm_proxy>& get_swarm_proxy() {
    return _the_swarm_proxy;
}

inline swarm_proxy& get_local_swarm_proxy() {
    return _the_swarm_proxy.local();
}

inline shared_ptr<swarm_proxy> get_local_shared_swarm_proxy() {
    return _the_swarm_proxy.local_shared();
}

}//namespace hive
