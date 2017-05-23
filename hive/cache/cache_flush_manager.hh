#pragma once

#include "core/semaphore.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/gate.hh"
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <boost/intrusive_ptr.hpp>
#include <deque>
#include <vector>
#include <functional>
#include <unordered_map>

#include "hive/cache/memory_cache.hh"
#include "hive/cache/item_key.hh"

namespace bi = boost::intrusive;
namespace hive {

using item_ptr = boost::intrusive_ptr<item>;

class cache_flush_manager {
public:
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of flush going on.
    };
private:
    memory_cache* _mem_cache;

    struct task {
        future<> flush_done = make_ready_future<>();
        semaphore flush_sem = semaphore(0);
        seastar::gate flush_gate;
        exponential_backoff_retry flush_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(60));
        // item being currently flushed.
        lw_shared_ptr<item_insertion_data> flushing_item = nullptr;
        bool stopping = false;
    };

    // flush manager may have N fibers to allow parallel flush per shard.
    std::vector<lw_shared_ptr<task>> _tasks;

    // Queue shared among all tasks containing all items to be flushed.
    std::deque<lw_shared_ptr<item_insertion_data>> _item_to_flush;

    // Used to assert that cache_flush_manager was explicitly stopped, if started.
    bool _stopped = true;

    stats _stats;
    std::vector<scollectd::registration> _registrations;

private:
    void task_start(lw_shared_ptr<task>& task);
    future<> task_stop(lw_shared_ptr<task>& task);

    void add_item(lw_shared_ptr<item_insertion_data> itemptr);
    // Signal the flush task with the lowest amount of pending jobs.
    // This function is called when a cf is submitted for flush and we need
    // to wake up a handler.
    void signal_less_busy_task();
    // Returns if this flush manager is accepting new requests.
    // It will not accept new requests in case the manager was stopped and/or there
    // is no task to handle them.
    bool can_submit();
public:
    cache_flush_manager(memory_cache* c);
    ~cache_flush_manager();

    void register_collectd_metrics();

    // Creates N fibers that will allow N flush jobs to run in parallel.
    // Defaults to only three fibers.
    void start(int task_nr = 3);

    // Stop all fibers. Ongoing flushs will be waited.
    future<> stop();

    // Submit a item to be flushed.
    void submit(item_ptr&& itemptr);

    // Remove oldest item if item more than default_flush_queue_count
    void maybe_remove_old_item();

    // Remove a item from the flush manager.
    // Cancel requests on item and wait for a possible ongoing flush on item.
    future<> remove(item_key key);

    const stats& get_stats() const {
        return _stats;
    }

    // Stops ongoing flush of a given type.
    void stop_flush(sstring type);

}; //class cache_flush_manager

} //namespace hive 
