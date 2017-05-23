#pragma once
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "keys.hh"
#include <list>
#include "utils/exponential_backoff_retry.hh"
#include "utils/histogram.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/rwlock.hh>
#include "core/shared_mutex.hh"
#include <mutex>

#include "hive/journal/primary_journal.hh"
#include "hive/journal/journal_segment.hh"
#include "hive/drain/drain_manager.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/drain/drain_plan.hh"
#include "hive/drain/drain_job.hh"
#include "hive/drain/drain_response_handler.hh"

using clock_type = std::chrono::steady_clock;

namespace hive {

struct drainer_stats {
    int64_t force_pending_drains = 0;
    int64_t pending_drains = 0;
    bool _drain_manager_queued = false;
    int64_t retry_task_times = 0;
    int64_t retry_drain_times = 0;
    uint64_t generation = 0;
    uint64_t group_id_generator = 0;
    uint64_t job_id_generator = 0;
};
struct hive_stats;

class volume_drainer{
private:
    sstring _volume_id;
    sstring _segment_id;
    std::unique_ptr<db::config> _config;
    hive::drain_manager& _drain_manager;
    primary_journal& _primary_journal;
    hive::hive_stats* _hive_stats = nullptr;
    drainer_stats _stats;
    lw_shared_ptr<drain_response_handler> _response_handler = nullptr;
    lw_shared_ptr<timer<>> _expire_timer_ptr;

    bool _force_drain = false;
    promise<> _force_drain_promise;

private:
    future<> do_drain_execution(lw_shared_ptr<journal_segment> segment); 
    future<> do_drain_execution_(lw_shared_ptr<journal_segment> segment, utils::latency_counter& lc); 
    future<drain_plan> make_drain_plan(lw_shared_ptr<journal_segment> segment);
    future<drain_plan> rescue_drain_plan(drain_task_group task_group);
    future<drain_job> make_drain_job(drain_task_group_type type, drain_plan plan);
    future<> run_job(drain_job job);
    future<> rescue_task_group(drain_task_group task_group);
    future<> remove_journal_segment(lw_shared_ptr<journal_segment> segment);

    uint64_t generate_task_group_id(){
       return _stats.group_id_generator++;
    }

    uint64_t generate_job_id(){
       return _stats.job_id_generator++;
    }

    void start_drain_timeout_timer();
    void cancel_drain_timeout_timer();

    
    future<drain_plan> make_drain_plan_for_test();

public:
    volume_drainer(
        sstring volume_id
        , const db::config& config
        , drain_manager& drain_manager
        , primary_journal& primary_journal
        , hive_stats* hive_stats
        );

    ~volume_drainer();
    sstring get_volume_id(){
        return _volume_id;
    }
    bool is_stopping_drainer();

    void drain(uint64_t size =1);
    future<> run_drain();
    future<> drain_task_group_done(drain_task_group task_group);

    bool drain_manager_queued() const;
    void set_drain_manager_queued(bool drain_manager_queued);
    int get_pending_drains();
    bool pending_drains() const;
    future<bool> check_drain_task_over();
    
    void reset_try_drain_times();
    void add_try_drian_times(sstring err_info);

    bool get_force_drain_stats();
    future<> waiting_force_drain();
    void maybe_force_drain_done();
};

} //namespace hive
