#include "hive/cache/cache_flush_manager.hh"
#include "core/scollectd.hh"

namespace hive {
static logging::logger logger("cache_flush_manager");

using version_type = uint64_t;

static uint64_t default_flush_queue_count = 80; //tododl:yellow why?

void cache_flush_manager::task_start(lw_shared_ptr<cache_flush_manager::task>& task) {
    logger.debug("[{}] start, this:{}, task:{}", __func__, this, &task);

    task->flush_done = keep_doing([this, task] {
        return task->flush_sem.wait().then([this, task] {
            return seastar::with_gate(task->flush_gate, [this, task] {
                if (_stopped || _item_to_flush.empty()) {
                    return make_ready_future<>();
                }

                task->flushing_item = _item_to_flush.front();
                _item_to_flush.pop_front();

                _stats.pending_tasks--;
                _stats.active_tasks++;

                auto it = task->flushing_item;
                future<> operation = make_ready_future<>();
                operation = _mem_cache->run_flush(*it);

                return operation.then([this, task] {
                    _stats.completed_tasks++;
                    task->flush_retry.reset();
                    
                    _mem_cache->set_cache_flush_manager_queued(task->flushing_item->key, false);
                    task->flushing_item = nullptr;
                    return make_ready_future<>();
                }).finally([this] {
                    _stats.active_tasks--;
                });
            });//with_gate
        }).then_wrapped([this, task] (future<> f) { //flush_sem.wait()
            bool retry = false;
            // seastar::gate_closed_exception is used for regular termination of the fiber.
            try {
                f.get();
            } catch (seastar::gate_closed_exception& e) {
                task->flushing_item = nullptr;
                throw;
            } catch (...) {
                retry = true;
            }

            // We shouldn't retry flush if task was asked to stop.
            if (!task->stopping && retry) {
                return task->flush_retry.retry().then([this, task] {
                    if (!task->flushing_item) {
                        return make_ready_future<>();
                    }
                    // pushing item to the back, so if the error is persistent,
                    // at least the others get a chance.
                    add_item(std::move(task->flushing_item));
                    task->flushing_item = nullptr;

                    // after sleeping, signal semaphore for the next flush attempt.
                    task->flush_sem.signal();
                    return make_ready_future<>();
                });
            }
            return make_ready_future<>();
        });
    }).then_wrapped([] (future<> f) { //keep_doing
        try {
            f.get();
        } catch (seastar::gate_closed_exception& e){
            logger.warn("[cache_flush_task] warn, exception:{}", e.what() );
        } catch (...){
            // this shouldn't happen, let's log it anyway.
            std::ostringstream out;
            out << "[cache_flush_task] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
    });
}

future<> cache_flush_manager::task_stop(lw_shared_ptr<cache_flush_manager::task>& task) {
    task->stopping = true;
    return task->flush_gate.close().then([task] {
        // NOTE: Signalling semaphore because we want task to finish with the
        // gate_closed_exception exception.
        task->flush_sem.signal();
        return task->flush_done.then([task] {
            task->flush_gate = seastar::gate();
            task->stopping = false;
            return make_ready_future<>();
        });
    });
}

void cache_flush_manager::add_item(lw_shared_ptr<item_insertion_data> it) {
    _item_to_flush.push_back(it);
    _stats.pending_tasks++;
}

void cache_flush_manager::maybe_remove_old_item() {
    if (_item_to_flush.size() >= default_flush_queue_count) {
        // reset the flsuh flag, let it can be added again.
        auto it = _item_to_flush.front();
        _mem_cache->set_cache_flush_manager_queued(it->key, false);
        _item_to_flush.pop_front();
        _stats.pending_tasks--;
    }
}

cache_flush_manager::cache_flush_manager(memory_cache* cache) {
    _mem_cache = cache;
}

cache_flush_manager::~cache_flush_manager() {
    // Assert that flush manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is destroyed.
    assert(_stopped == true);
}

void cache_flush_manager::register_collectd_metrics() {
    auto add = [this] (auto type_name, auto name, auto data_type, auto func) {
        _registrations.push_back(
            scollectd::add_polled_metric(scollectd::type_instance_id("cache_flush_manager",
                scollectd::per_cpu_plugin_instance,
                type_name, name),
                scollectd::make_typed(data_type, func)));
    };

    add("objects", "flushs", scollectd::data_type::GAUGE, [&] { return _stats.active_tasks; });
}

void cache_flush_manager::start(int task_nr) {
    _stopped = false;
    _tasks.reserve(task_nr);
    register_collectd_metrics();
    for (int i = 0; i < task_nr; i++) {
        auto task = make_lw_shared<cache_flush_manager::task>();
        task_start(task);
        _tasks.push_back(task);
    }
}

future<> cache_flush_manager::stop() {
    if (_stopped) {
        return make_ready_future<>();
    }
    _stopped = true;
    _registrations.clear();
    // Wait for each task handler to stop.
    return do_for_each(_tasks, [this] (auto& task) {
        return this->task_stop(task);
    }).then([this] {
        for (auto& it : _item_to_flush) {
            _mem_cache->set_cache_flush_manager_queued(it->key, false);
        }
        _item_to_flush.clear();
        //logger.info("Stopped");
        return make_ready_future<>();
    });
}

void cache_flush_manager::signal_less_busy_task() {
    auto result = std::min_element(std::begin(_tasks), std::end(_tasks), [] (auto& i, auto& j) {
        return i->flush_sem.current() < j->flush_sem.current();
    });
    (*result)->flush_sem.signal();
}

bool cache_flush_manager::can_submit() {
    return !_stopped && !_tasks.empty();
}

void cache_flush_manager::submit(item_ptr&& it) {
    if (!can_submit()) {
        return;
    }
    // To avoid having two or more entries of the same item stored in the queue.
    if (it->cache_flush_manager_queued()) {
        return;
    }
    it->set_cache_flush_manager_queued(true);
    // If the _item_to_flush count more than default_flush_queue_count, remove the oldest
    maybe_remove_old_item();

    // constructor item_insertion_data
    auto itemptr = make_lw_shared<item_insertion_data>(it->key().to_string(), it->version(), std::move(it->datum()));

    add_item(std::move(itemptr));
    signal_less_busy_task();
}

future<> cache_flush_manager::remove(item_key key) {
    // Remove every reference to it from _item_to_flush.
    _item_to_flush.erase(
        std::remove_if(_item_to_flush.begin(), _item_to_flush.end(), [key] (lw_shared_ptr<item_insertion_data> entry) {
            return key == entry->key;
        }),
        _item_to_flush.end());
    _stats.pending_tasks = _item_to_flush.size();

    _mem_cache->set_cache_flush_manager_queued(key, false);

    // We need to guarantee that a task being stopped will not re-queue the
    // item being removed.
    auto tasks_to_stop = make_lw_shared<std::vector<lw_shared_ptr<task>>>();
    for (auto& task : _tasks) {
        if (task->flushing_item) {
          if (task->flushing_item->key == key) {
              tasks_to_stop->push_back(task);
              task->stopping = true;
          }
        }
    }

    // Wait for the termination of an ongoing flush on it, if any.
    return do_for_each(*tasks_to_stop, [this, key] (auto& task) {
      if (task->flushing_item) {
        if (!(task->flushing_item->key == key)) {
            // There is no need to stop a task that is no longer flushing
            // the item being removed.
            task->stopping = false;
            return make_ready_future<>();
        }
      }
      return this->task_stop(task).then([this, &task] {
          // assert that task finished successfully.
          assert(!task->flushing_item);
          this->task_start(task);
          return make_ready_future<>();
      });
    }).then([tasks_to_stop] {});
}

} // namespace hive 
