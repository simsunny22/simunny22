#include "hive/drain/volume_drainer.hh"
#include "hive/journal/primary_journal.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "hive/journal/journal_service.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_service.hh"
#include "hive/http/json11.hh"
#include "hive/swarm/job_coordinator_service.hh"

#include "log.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "locator/simple_snitch.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "core/do_with.hh"
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "checked-file-impl.hh"
#include "disk-error-handler.hh"
#include "utils/latency.hh"

using namespace std::chrono_literals;

namespace hive {

static logging::logger logger("volume_drainer");
static uint64_t MAX_RET_DRAIN_TIMES = 3;

volume_drainer::volume_drainer(sstring volume_id
        , const db::config& config
        , drain_manager& drain_manager
        , primary_journal& primary_journal
        , hive_stats* hive_stats)
    : _volume_id(volume_id)
    , _config(std::make_unique<db::config>(config)) 
    , _drain_manager(drain_manager)
    , _primary_journal(primary_journal)
    , _hive_stats(hive_stats)
{
}

volume_drainer::~volume_drainer() {
}

bool volume_drainer::is_stopping_drainer(){
    return _primary_journal.is_stopping_driver();
}

bool volume_drainer::pending_drains() const {
    return _stats.pending_drains > 0;
}

void volume_drainer::set_drain_manager_queued(bool drain_manager_queued) {
    _stats._drain_manager_queued = drain_manager_queued;
}

bool volume_drainer::drain_manager_queued() const {
    return _stats._drain_manager_queued;
}

future<bool> volume_drainer::check_drain_task_over(){
    return _drain_manager.check_drain_task_over(this);
}

int volume_drainer::get_pending_drains(){
    logger.debug("[{}] start, shard:{}, volume_id:{}, not drain memtable:{}"
        , __func__, engine().cpu_id(), _volume_id,  _stats.pending_drains);
    return (int)_stats.pending_drains;
}


void volume_drainer::drain(uint64_t size) {
    _stats.pending_drains += size;
    _drain_manager.submit(this);

    logger.debug("[{}] drain_stats, start ===================", __func__);
    logger.debug("[{}] drain_stats, pending tasks: {}", __func__,  _volume_id, _drain_manager.get_stats().pending_tasks);
    logger.debug("[{}] drain_stats, active tasks: {}", __func__,  _volume_id, _drain_manager.get_stats().active_tasks);
    logger.debug("[{}] drain_stats, completed tasks: {}", __func__, _volume_id, _drain_manager.get_stats().completed_tasks);
    logger.debug("[{}] drain_stats, end   ===================", __func__);
}


future<> volume_drainer::run_drain(){
    assert(_stats.pending_drains > 0);
    lw_shared_ptr<journal_segment_list> segments = _primary_journal.get_segments();
    logger.debug("[{}]run_drain, pending_drains:{}", _volume_id, _stats.pending_drains);  
   
    auto old_segment= segments->front();
    return do_drain_execution(old_segment);
}

future<> volume_drainer::do_drain_execution(lw_shared_ptr<journal_segment> segment) {
    logger.debug("[{}]do_drain_execution start, segment_id:{}",_volume_id, segment->get_id());
    _stats.pending_drains--;
    auto segment_size = segment->get_occupancy();
    _hive_stats->pending_memtables_drains_count++;
    _hive_stats->pending_memtables_drains_bytes += segment_size;
    lw_shared_ptr<drain_response_handler> handler = make_lw_shared<drain_response_handler>(_stats.generation);
    _response_handler = handler;
    start_drain_timeout_timer();
    utils::latency_counter lc;
    lc.start();
    do_drain_execution_(segment, lc);
    auto f = _response_handler->wait();
    return std::move(f).then([this, segment](auto result){
        //remove cache
        return make_ready_future<>();
    }).then([this, segment]() mutable{
        logger.debug("[{}]run job done, segment_id:{}",_volume_id, segment->get_id());
        logger.debug("[{}]remove_journal_segment start, segment_id:{}",_volume_id, segment->get_id());
        return this->remove_journal_segment(segment);
    }).then_wrapped([this, lc] (auto result) mutable{
        try {
            result.get();
            this->cancel_drain_timeout_timer(); 
            _stats.generation++;
            this->maybe_force_drain_done();
            logger.debug("[{}]do_drain_excecution done, latency:{} ns, generation:{}",_volume_id, lc.stop().latency_in_nano(), _response_handler->generation());
            return make_ready_future<>();
        } catch(...) {
            _stats.generation++;
            _stats.pending_drains++;
            logger.error("[{}] do_drain_excecution exception, latency:{} ns", _volume_id, lc.stop().latency_in_nano());
            return make_exception_future<>(std::current_exception());
        } 
    });
}

future<> volume_drainer::do_drain_execution_(lw_shared_ptr<journal_segment> segment, utils::latency_counter& lc) {
    _segment_id = segment->get_id();
    logger.debug("[{}]make_drain_plan start, segment_id:{}",_volume_id, segment->get_id());
    utils::latency_counter lc_make_plan;
    lc_make_plan.start();
    return make_drain_plan(segment).then([this, segment, lc_make_plan](auto drain_plan) mutable {
        logger.debug("[{}]make_drain_plan done, segment_id:{}, latency:{} ns",_volume_id, segment->get_id(), lc_make_plan.stop().latency_in_nano());
        logger.debug("[{}]make_drain_job start, segment_id:{}",_volume_id, segment->get_id());
        return this->make_drain_job(drain_task_group_type::NORMAL_TASK_GROUP, std::move(drain_plan));
    }).then([this, segment, lc](auto job) mutable {
        logger.debug("[{}]make_drain_job done, segment_id:{}, task_group size:{}",_volume_id, segment->get_id(), job.task_groups.size());
        logger.debug("[{}]run job start, segment_id:{}, task_group size:{}",_volume_id, segment->get_id(), job.task_groups.size());
        _response_handler->add_task_groups(job.task_groups);
        return this->run_job(job);
    }).then_wrapped([this, lc] (auto result) mutable{
        try {
            result.get();
            logger.debug("[{}]run_job done, latency:{} ns, generation:{}",_volume_id, lc.stop().latency_in_nano(), _response_handler->generation());
            return make_ready_future<>();
        } catch(...) {
            logger.error("[{}] run_job exception, latency:{} ns, exception {}", _volume_id, lc.stop().latency_in_nano(), std::current_exception());
            return make_exception_future<>(std::current_exception());
        } 
    });

}

void volume_drainer::start_drain_timeout_timer(){
    _expire_timer_ptr = make_lw_shared<timer<>>([this](){
        std::ostringstream out;
        out << "do_drain_execution timeout, volume_id::=" << _volume_id << "segment_id:" << _segment_id;
        out << ", response targets: " << _response_handler->get_targets().size();
        out << ", response success: " << _response_handler->get_success().size();
        sstring error_info = out.str();
        logger.error(error_info.c_str());
        _response_handler->trigger_exception(std::runtime_error(error_info));
    });
    auto timeout_in_ms = _config->drain_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    _expire_timer_ptr->arm(timeout);
}

void volume_drainer::cancel_drain_timeout_timer(){
    _expire_timer_ptr->cancel();
}

future<drain_plan> volume_drainer::make_drain_plan(lw_shared_ptr<journal_segment> segment){
    if(_config->debug_mode()){
        return make_drain_plan_for_test();
    }
    
    return segment->scan().then([this](auto volume_revisions)mutable{
        return hive::get_local_volume_service().make_drain_plan(std::move(volume_revisions));
    });
}

future<drain_job> volume_drainer::make_drain_job(drain_task_group_type type, drain_plan plan){
    drain_job job(generate_job_id(), _stats.generation);
    std::map<sstring, drain_task_group> task_groups;
    for(auto& task : plan.tasks){
        sstring node_ip = task.node_ip;
        auto it = task_groups.find(node_ip);
        if(it != task_groups.end()){
            it->second.tasks.push_back(task);
        }else{
            uint64_t group_id = generate_task_group_id();
            drain_task_group task_group(_volume_id, job.job_id, group_id, job.job_generation, node_ip, task.journal_segment_id, engine().cpu_id(), task.journal_nodes, type);
            task_group.tasks.push_back(task);
            task_groups.insert(std::make_pair(node_ip, std::move(task_group)));
        }
    }
    for(auto it:task_groups){
       job.task_groups.push_back(it.second);
    }
 
    return make_ready_future<drain_job>(std::move(job));
}

future<> volume_drainer::run_job(drain_job job){
    logger.debug("[{}]run_job start, task_group size:{}",_volume_id,  job.task_groups.size());
    std::vector<future<>> futures;
    for(auto& task_group : job.task_groups){
        auto fut = hive::get_local_job_coordinator_service().perform_drain(std::move(task_group));
        futures.push_back(std::move(fut));
    }

    return when_all(futures.begin(), futures.end()).then_wrapped([this](auto f){
        try{
            f.get();
            logger.debug("[{}]perform drain done...",_volume_id);
            return make_ready_future<>(); 
        } catch(...) {
            std::ostringstream out;
            out << "[run job], failed";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::current_exception());
        } 
    });
}

future<> volume_drainer::drain_task_group_done(drain_task_group group){
    logger.debug("[{}] receive drain_task_group_done, task_group_id:{}, generation:{}", _volume_id, group.group_id, group.job_generation);
    if(group.success()){
        _response_handler->response(group.group_id, group.job_generation);
        return make_ready_future<>(); 
    }else{
        auto& task_groups = _response_handler->get_task_groups();
        auto itor = task_groups.find(group.group_id);
        if(itor != task_groups.end()){
            itor->second.retry_drain_num++; 
            if(itor->second.retry_drain_num > hive::MAX_RET_DRAIN_TIMES){
                if(itor->second.group_type == drain_task_group_type::NORMAL_TASK_GROUP){
                    logger.error("[{}]normal drain_task_group failed, task_group:{}, retry_try_num:{} > max_retry_times:{}, start to rescue_task_group", _volume_id, group, itor->second.retry_drain_num, hive::MAX_RET_DRAIN_TIMES);
                    return rescue_task_group(group);
                }else{
                    logger.error("[{}]rescue drain_task_group failed, task_group:{}, retry_try_num:{} > max_retry_times:{}, trigger_exception", _volume_id, group, itor->second.retry_drain_num, hive::MAX_RET_DRAIN_TIMES);
                    _response_handler->trigger_exception(std::runtime_error("rescue_task_group failed"));
                    return make_ready_future<>(); 
                }
            }else{
                logger.error("[{}]perform_drain failed, task_group:{}, retry_try_num:{}", _volume_id, itor->second, itor->second.retry_drain_num);
                return hive::get_local_job_coordinator_service().perform_drain(itor->second);
            }
        }
        return make_ready_future<>(); 
    }
}

future<> volume_drainer::rescue_task_group(drain_task_group group){
    return rescue_drain_plan(group).then([this](auto drain_plan){
         return this->make_drain_job(drain_task_group_type::RESCUE_TASK_GROUP, std::move(drain_plan));
    }).then([this, group](auto job){
        this->_response_handler->add_task_groups(job.task_groups);
        this->_response_handler->response(group.group_id, group.job_generation);
        return this->run_job(std::move(job));
        
    }); 
}

future<drain_plan> volume_drainer::rescue_drain_plan(drain_task_group group){
    if(_config->debug_mode()){
        return make_drain_plan_for_test();
    }
    return hive::get_local_volume_service().rescue_task_group(std::move(group));
}


future<> volume_drainer::remove_journal_segment(lw_shared_ptr<journal_segment> segment){
    utils::latency_counter lc;
    lc.start();
    return segment->discard_primary_segment().then([this, segment, lc = std::move(lc)]()mutable{
        auto segments = _primary_journal.get_segments();
        segments->erase(segment);
        logger.debug("[{}]remove_journal_segment done, latency:{} ns",_volume_id, lc.stop().latency_in_nano());
        return _primary_journal.release_segments_token();
    });

}


void volume_drainer::reset_try_drain_times(){
//TODO:red 
}

void volume_drainer::add_try_drian_times(sstring err_info){
//TODO:red 
}


bool volume_drainer::get_force_drain_stats(){
    return _force_drain;
}

future<> volume_drainer::waiting_force_drain(){
    if(_stats.pending_drains  <= 0){
        logger.error("volume_drainer::waiting_force_drain .... _stats.pending_drains <= 0");
        return make_ready_future<>();
    }

    assert(!_force_drain);

    _force_drain = true;
    _force_drain_promise = std::move(promise<>());
    _stats.force_pending_drains += _stats.pending_drains;
    return _force_drain_promise.get_future();
}

void volume_drainer::maybe_force_drain_done(){
  _stats.force_pending_drains ? _stats.force_pending_drains-- : _stats.force_pending_drains;
  if(_force_drain && _stats.force_pending_drains == 0){
      _force_drain = false;
      _force_drain_promise.set_value();
  }
}

future<drain_plan> volume_drainer::make_drain_plan_for_test(){
    drain_plan plan;
    //volume_id, segment_id, journal_nodes, extents, extent_group_id, need_create_extent_group, need_replicate_extent_group, version, node_ip, disk_id, replica_disk_ids, std::vector<extent_group_revision>, task_status
    drain_extent_group_task task("volume_1", "default", "segment_1", {"172.16.100.100"}, {"volume_1#0", "volume_1#1024576"}, "volume_1_extent_group_1", false, false, 0, "172.16.100.100", "1", "", {}, drain_task_status::INIT);

    plan.tasks.push_back(task);
    return make_ready_future<drain_plan>(std::move(plan)); 
}


} //namespace hive
