#pragma once

#include "core/semaphore.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/gate.hh"
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <deque>
#include <vector>
#include <functional>
#include "core/timer.hh"



namespace hive {

class volume_drainer;

// drain manager is a feature used to manage drain jobs from multiple
// primary_journal pertaining to the same database.
// For each drain job handler, there will be one fiber that will check for
// jobs, and if any, run it. FIFO ordering is implemented here.
class drain_manager {
public:
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of compaction going on.
    };
private:
    struct task {
        future<> drain_done = make_ready_future<>();
        semaphore drain_sem = semaphore(0);
        seastar::gate drain_gate;
        exponential_backoff_retry drain_retry = exponential_backoff_retry(std::chrono::seconds(2), std::chrono::seconds(5));
        volume_drainer* cur_volume_drainer= nullptr;
        bool stopping = false;
    };

    std::vector<lw_shared_ptr<task>> _tasks;
    std::deque<volume_drainer*> _volumes_to_drain;

    bool _stopped = true;
    stats _stats;

    timer<lowres_clock> _timer;
    void on_timer();
    
    void     task_start(lw_shared_ptr<task>& task);
    future<> task_stop(lw_shared_ptr<task>& task);

    bool can_submit();
    void add_volume_drainer(volume_drainer* drainer);
    void signal_less_busy_task();
public:
    drain_manager();
    ~drain_manager();

    void     start(int task_nr = 1);
    future<> stop();

    void     submit(volume_drainer* drainer);
    future<> remove(volume_drainer* drainer);
    void     stop_drain(sstring type); //Stops ongoing drain of a given type.
    future<bool> check_drain_task_over(volume_drainer* drainer);

    const stats& get_stats() const {
        return _stats;
    }
};

} //namespace hive
