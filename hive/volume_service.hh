#pragma once
#include "core/distributed.hh"
#include "core/shared_mutex.hh"
#include "utils/histogram.hh"
#include "seastar/core/shared_ptr.hh"
#include "log.hh"

#include "hive/volume_driver.hh"
#include "hive/drain/drain_manager.hh"
#include "hive/drain/drain_plan.hh"
#include "hive/drain/drain_task_group.hh"

namespace hive{

class volume_service: public seastar::async_sharded_service<volume_service> {
private:
    std::map<sstring, lw_shared_ptr<volume_driver>> _drivers;
    seastar::shared_mutex _create_driver_lock; 

    future<lw_shared_ptr<volume_driver>> find_or_create_volume_driver(sstring volume_id);
public:
    volume_service();
    ~volume_service();
    
    future<> stop();
   
    unsigned shard_of(const sstring volume_id);
    
    future<> recover_commitlog();

    future<driver_info> bind_volume_driver(sstring volume_id);

    lw_shared_ptr<volume_driver> get_volume_driver(sstring volume_id);

    future<> rebuild_volume_driver(sstring volume_id);
    future<> stop_volume_driver(sstring volume_id);

    future<uint64_t> get_vclock(sstring volume_id, uint64_t count=1);
    future<write_plan> get_write_plan(sstring volume_id, uint64_t offset, uint64_t length, lw_shared_ptr<bytes> buffer);
    future<> commit_create_group(sstring volume_id, std::vector<create_action> actions);
    future<> commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks);
    future<read_plan> get_read_plan(sstring volume_id, uint64_t offset, uint64_t length);
    
    future<> drain_volumes();
    future<drain_plan> make_drain_plan(std::vector<volume_revision_view> volume_revision_views);
    future<drain_plan> rescue_task_group(drain_task_group task_group);
};

extern distributed<volume_service> _the_volume_service;

inline distributed<volume_service>& get_volume_service() {
    return _the_volume_service;
}

inline volume_service& get_local_volume_service() {
    return _the_volume_service.local();
}

inline shared_ptr<volume_service> get_local_shared_volume_service() {
    return _the_volume_service.local_shared();
}

}//namespace hive
