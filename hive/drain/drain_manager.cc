#include "hive/drain/drain_manager.hh"
#include "database.hh"
#include "core/scollectd.hh"
#include "hive/hive_service.hh"
#include "hive/drain/volume_drainer.hh"
//#include "exceptions.hh"

namespace hive {

static logging::logger logger("drain_manager");

void drain_manager::task_start(lw_shared_ptr<drain_manager::task>& task) {
    // When it's time to shutdown, we need to prevent any new drain 
    // from starting and wait for a possible ongoing drain.
    // That's possible by closing gate, busting semaphore and waiting for
    // the future drain_done to resolve.

    task->drain_done = keep_doing([this, task] {
        return task->drain_sem.wait().then([this, task] {
            return seastar::with_gate(task->drain_gate, [this, task] {
                if (_stopped || _volumes_to_drain.empty()) {
                    return make_ready_future<>();
                }
                
                task->cur_volume_drainer= _volumes_to_drain.front();
                _volumes_to_drain.pop_front();

                _stats.pending_tasks--;
                _stats.active_tasks++;

                future<> drain_fut = make_ready_future<>();
                volume_drainer& drainer= *task->cur_volume_drainer;
                drain_fut = drainer.run_drain();

                return drain_fut.then([this, task] {
                    _stats.completed_tasks++;
                    // If drain completed successfully, let's reset
                    // sleep time of drain_retry.
                    task->drain_retry.reset();
                    task->cur_volume_drainer->reset_try_drain_times();

                    // Re-schedule drain for cur_volume_drainer, if needed.
                    if (!task->stopping && task->cur_volume_drainer->pending_drains()) {
                        // If there are pending drains for primary_journal,
                        // push it into the back of the queue.
                        add_volume_drainer(task->cur_volume_drainer);
                        task->drain_sem.signal();
                    } else {
                        // If so, primary_journal is no longer queued by drain_manager.
                        task->cur_volume_drainer->set_drain_manager_queued(false);
                    }
                    task->cur_volume_drainer= nullptr;

                    return make_ready_future<>();
                }).finally([this] {
                    _stats.active_tasks--;
                });
            }); //seastar::with_gate
        }).then_wrapped([this, task] (future<> f) { //task->drain_sem.wait()
            bool retry = false;
            sstring error_info;
            // seastar::gate_closed_exception is used for regular termination of the fiber.
            try {
                f.get();
            } catch (seastar::gate_closed_exception& e) {
                task->cur_volume_drainer = nullptr;
                logger.debug("drain task handler stopped due to shutdown");
                throw;
            }catch(...){
                 std::ostringstream out;
                 out << "error_info:" << std::current_exception();
                 error_info = out.str();
                 logger.error("error, drain catch unknown exception:{}", std::current_exception());
                 retry = true;
            }
            // We shouldn't retry drain if task was asked to stop.
            if (!task->stopping && retry) {
                logger.info("drain task handler sleeping for {} seconds",
                std::chrono::duration_cast<std::chrono::seconds>(task->drain_retry.sleep_time()).count());
                return task->drain_retry.retry().then([this, task, error_info] {
                    if (!task->cur_volume_drainer) {
                        return make_ready_future<>();
                    }
                    // pushing primary_journal to the back, so if the error is persistent,
                    // at least the others get a chance.
                    add_volume_drainer(task->cur_volume_drainer);
                    task->cur_volume_drainer->add_try_drian_times(error_info);
                    task->cur_volume_drainer = nullptr;

                    // after sleeping, signal semaphore for the next drain attempt.
                    task->drain_sem.signal();
                    return make_ready_future<>();
                });
            }
            return make_ready_future<>();
        });
    }).then_wrapped([] (future<> f) { //keep_doing
        try {
            f.get();
        } catch (seastar::gate_closed_exception& e) {
            // exception logged in keep_doing.
        } catch (...) {
            // this shouldn't happen, let's log it anyway.
            logger.error("error drain task: unexpected error:{}", std::current_exception());
        }
    });
}

drain_manager::drain_manager() = default;
drain_manager::~drain_manager() {
    // Assert that drain manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is destroyed.
    assert(_stopped == true);
}

void drain_manager::start(int task_nr) {
    logger.info("drain_manager start, task count:{}", task_nr);
    _stopped = false;
    _tasks.reserve(task_nr);
    for (int i = 0; i < task_nr; i++) {
        auto task = make_lw_shared<drain_manager::task>();
        task_start(task);
        _tasks.push_back(task);
    }

    //tododl: open can print drain stats
    uint64_t periodic_print_drain_task_count = hive::get_local_hive_service().get_hive_config()->periodic_print_drain_task_count_in_s();
    if (periodic_print_drain_task_count > 0) {
        _timer.set_callback(std::bind(&drain_manager::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(1)
                 , std::experimental::optional<lowres_clock::duration> {std::chrono::seconds(periodic_print_drain_task_count)});
    }
}

void drain_manager::on_timer() {
    logger.error("-->on_timer,stats ========== current drain pending tasks: {}", _stats.pending_tasks);
    logger.error("-->on_timer,stats ========== current drain active tasks: {}", _stats.active_tasks);
    logger.error("-->on_timer,stats ========== current completed tasks: {}", _stats.completed_tasks);
}

future<> drain_manager::task_stop(lw_shared_ptr<drain_manager::task>& task) {
    task->stopping = true;
    return task->drain_gate.close().then([task] {
        // NOTE: Signalling semaphore because we want task to finish with the
        // gate_closed_exception exception.
        task->drain_sem.signal();
        return task->drain_done.then([task] {
            task->drain_gate = seastar::gate();
            task->stopping = false;
            return make_ready_future<>();
        });
    });
}

void drain_manager::add_volume_drainer(volume_drainer* drainer) {
    auto stopping_drainer = drainer->is_stopping_drainer();
    if (stopping_drainer)
        return;
    _volumes_to_drain.push_back(drainer);
    _stats.pending_tasks++;
}

future<> drain_manager::stop() {
    logger.info("Asked to stop");
    if (_stopped) {
        return make_ready_future<>();
    }
    _stopped = true;
    // Wait for each task handler to stop.
    return do_for_each(_tasks, [this] (auto& task) {
        return this->task_stop(task);
    }).then([this] {
        for (auto& volume_drainer: _volumes_to_drain) {
            volume_drainer->set_drain_manager_queued(false);
        }
        _volumes_to_drain.clear();
        logger.info("Stopped");
        return make_ready_future<>();
    });
}

void drain_manager::signal_less_busy_task() {
    auto result = std::min_element(std::begin(_tasks), std::end(_tasks), [] (auto& i, auto& j) {
        return i->drain_sem.current() < j->drain_sem.current();
    });
    (*result)->drain_sem.signal();
}

bool drain_manager::can_submit() {
    return !_stopped && !_tasks.empty();
}

void drain_manager::submit(volume_drainer* drainer) {
    if (!can_submit()) {
        return;
    }
    // To avoid having two or more entries of the same journal stored in the queue.
    if (drainer->drain_manager_queued()) {
        return;
    }
    drainer->set_drain_manager_queued(true);
    add_volume_drainer(drainer);
    signal_less_busy_task();
}

future<> drain_manager::remove(volume_drainer* drainer) {
    // Remove every reference to journal from _volumes_to_drain.
    _volumes_to_drain.erase(
        std::remove_if(_volumes_to_drain.begin(), _volumes_to_drain.end(), [drainer] (volume_drainer* entry) {
            return drainer== entry;
        }),
        _volumes_to_drain.end()
    );

    _stats.pending_tasks = _volumes_to_drain.size();
    drainer->set_drain_manager_queued(false);
    // We need to guarantee that a task being stopped will not re-queue the
    // primary_journal being removed.
    auto tasks_to_stop = make_lw_shared<std::vector<lw_shared_ptr<task>>>();
    for (auto& task : _tasks) {
        if (task->cur_volume_drainer == drainer) {
            tasks_to_stop->push_back(task);
            task->stopping = true;
        }
    }

    // Wait for the termination of an ongoing drain on primary_journal, if any.
    return do_for_each(*tasks_to_stop, [this, drainer] (auto& task) {
        if (task->cur_volume_drainer != drainer) {
            // There is no need to stop a task that is no longer draining 
            // the primary_journal being removed.
            task->stopping = false;
            return make_ready_future<>();
        }
        return this->task_stop(task).then([this, &task] {
            // assert that task finished successfully.
            assert(task->cur_volume_drainer == nullptr);
            this->task_start(task);
            return make_ready_future<>();
        });
    }).then([tasks_to_stop] {});
}

void drain_manager::stop_drain(sstring type) {
}

future<bool>
drain_manager::check_drain_task_over(volume_drainer* drainer){
    if(_stopped)
        return make_ready_future<bool>(true);

    auto deque_size = _volumes_to_drain.size();
    logger.debug("deque_size ==================>{}", deque_size);
    for(unsigned int i =0; i < deque_size; i ++){
        auto temp = _volumes_to_drain[i];
        if(temp == drainer){
            return make_ready_future<bool>(false);
        }
    }

    auto task_size = _tasks.size(); 
    logger.debug("task_size ==================>{}", task_size);
    for(unsigned int i =0; i < task_size; i++ ){
        auto task = _tasks[i];
        if(task->cur_volume_drainer  == drainer){
            return make_ready_future<bool>(false);
        }
    }
    return make_ready_future<bool>(true);
}

} //namespace hive
