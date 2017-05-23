#include "hive/volume_service.hh"
#include "hive/hive_service.hh"

#include "unimplemented.hh"
#include "core/future-util.hh"
#include "exceptions/exceptions.hh"
#include "locator/snitch_base.hh"
#include "log.hh"
#include "to_string.hh"
#include <seastar/core/thread.hh>
#include <sstream>
#include <algorithm>
#include "unimplemented.hh"
#include <sys/time.h>
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/enum.hh>
#include <seastar/net/tls.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "utils/latency.hh"
#include "bytes.hh"
#include <chrono>

using namespace std::chrono_literals;
namespace hive {
static logging::logger logger("volume_service");
distributed<volume_service> _the_volume_service;

volume_service::volume_service() 
{
    logger.info("constructor start");
}

volume_service::~volume_service() 
{}

future<> volume_service::stop() {
    return make_ready_future<>();
}

unsigned volume_service::shard_of(const sstring volume_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(volume_id)));
    return dht::shard_of(token);
}

future<lw_shared_ptr<volume_driver>> volume_service::find_or_create_volume_driver(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}", __func__, volume_id);
    return with_lock(_create_driver_lock, [this, volume_id](){
        auto it = _drivers.find(volume_id);
        if(it != _drivers.end()){
            return make_ready_future<lw_shared_ptr<volume_driver>>(it->second);
        }else{
            logger.debug("[find_or_create_volume_driver] find failed will create new, volume_id:{}", volume_id);
            lw_shared_ptr<volume_driver> driver_ptr = make_lw_shared<volume_driver>(volume_id);
            return driver_ptr->init().then([this, volume_id, driver_ptr](){
                _drivers.insert(std::pair<sstring, lw_shared_ptr<volume_driver>>(volume_id, driver_ptr));
                return make_ready_future<lw_shared_ptr<volume_driver>>(driver_ptr);
            });
        }
    });
}

lw_shared_ptr<volume_driver> volume_service::get_volume_driver(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}", __func__, volume_id);
    auto it = _drivers.find(volume_id);
    if(it != _drivers.end()){
        return it->second;
    }else{
        auto error_info = sprint("get volume driver failed, volume_id:{}", volume_id);
        throw hive::not_found_exception(error_info);
    }
}

future<driver_info> volume_service::bind_volume_driver(sstring volume_id){
    // first to bind driver, second to check driver is master 
    // because we need use driver that maybe is not master to change it when always forward 
    return find_or_create_volume_driver(volume_id).then([volume_id](auto driver_ptr){
        // 1. check is master driver
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_volume_context(volume_id).then(
                [driver_ptr](auto volume_context){
            driver_info volume_ex;
            volume_ex.vclock = driver_ptr->get_vclock();
            sstring driver_node_ip = volume_context.get_driver_node().get_ip(); 
            if(!hive_tools::is_me(driver_node_ip)){
                volume_ex.need_redirect = true;     
                volume_ex.redirect_ip = driver_node_ip; 
            }
            return make_ready_future<driver_info>(std::move(volume_ex));
        });
   });
}

future<uint64_t> volume_service::get_vclock(sstring volume_id, uint64_t count){
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [this, volume_id, count]
            (auto& shard_service)mutable{
        auto driver_ptr = shard_service.get_volume_driver(volume_id);
        auto start_vclock = driver_ptr->get_vclock(count);
        return make_ready_future<uint64_t>(start_vclock);
    });
}

future<> volume_service::rebuild_volume_driver(sstring volume_id){
    auto it = _drivers.find(volume_id);
    if(it == _drivers.end()){
        lw_shared_ptr<volume_driver> driver_ptr = make_lw_shared<volume_driver>(volume_id);
        return driver_ptr->rebuild_volume_stream().then([this, volume_id, driver_ptr](){
            _drivers.insert(std::pair<sstring, lw_shared_ptr<volume_driver>>(volume_id, driver_ptr));
            return make_ready_future<>();
        });
    }else{
        sstring error_info = "volume driver has already exist" ;
        error_info += ", cpu_id:"    + engine().cpu_id();
        error_info += ", volume_id:" + volume_id;
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

future<> volume_service::stop_volume_driver(sstring volume_id){
    auto it = _drivers.find(volume_id);
    if(it != _drivers.end()){
        auto driver_ptr = it->second;
        return driver_ptr->stop_volume_stream().then([this, it](){
            _drivers.erase(it);
            return make_ready_future<>();
        });
    }else{
        sstring error_info = "volume driver do not exist" ;
        error_info += ", cpu_id:"    + engine().cpu_id();
        error_info += ", volume_id:" + volume_id;
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

future<write_plan> volume_service::get_write_plan(sstring volume_id, uint64_t offset, uint64_t length, lw_shared_ptr<bytes> buffer){
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, offset, length, buffer = std::move(buffer)]
            (auto& volume_service){
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->get_write_plan(volume_id, offset, length, std::move(buffer));
    });
}

future<> volume_service::commit_create_group(sstring volume_id, std::vector<create_action> actions){
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, actions]
            (auto& volume_service)mutable{
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->commit_create_group(actions); 
    });
}

future<> volume_service::commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks){
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, vclocks]
            (auto& volume_service)mutable{
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->commit_write_plan(volume_id, vclocks);
    });
}

future<read_plan> volume_service::get_read_plan(sstring volume_id, uint64_t offset, uint64_t length){
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, offset, length]
            (auto& volume_service)mutable{
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->get_read_plan(volume_id, offset, length); 
    });
}

future<drain_plan> volume_service::make_drain_plan(std::vector<volume_revision_view> volume_revision_views){
    if(volume_revision_views.size() == 0){
        auto error_info = sprint("volume_revision is empty!");
        throw std::runtime_error(error_info);    
    }
    sstring volume_id = volume_revision_views[0].volume_id;
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [this, volume_id, volume_revision_views](auto& volume_service) mutable{
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->make_drain_plan(std::move(volume_revision_views));
    });
}

future<drain_plan> volume_service::rescue_task_group(drain_task_group task_group){
    sstring volume_id = task_group.volume_id;
    auto shard = shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [this, volume_id, task_group] (auto& volume_service) mutable{
        auto driver_ptr = volume_service.get_volume_driver(volume_id);
        return driver_ptr->rescue_drain_plan(task_group.tasks);
    });
}

} //namespace hive
