#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "utils/histogram.hh"
#include "seastar/core/shared_ptr.hh"
#include "log.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/message_data_type.hh"


namespace hive{

using clock_type = std::chrono::steady_clock;

class job_coordinator_service: public seastar::async_sharded_service<job_coordinator_service> {
private:
public:
    job_coordinator_service();
    ~job_coordinator_service();
    
    future<> stop();

    future<> perform_drain(drain_task_group task_group);

    future<> drain_task_group_done(smd_drain_task_group task_group);

}; //class job_coordinator_service


extern distributed<job_coordinator_service> _the_job_coordinator_service;

inline distributed<job_coordinator_service>& get_job_coordinator_service() {
    return _the_job_coordinator_service;
}

inline job_coordinator_service& get_local_job_coordinator_service() {
    return _the_job_coordinator_service.local();
}

inline shared_ptr<job_coordinator_service> get_local_shared_job_coordinator_service() {
    return _the_job_coordinator_service.local_shared();
}

}//namespace hive
