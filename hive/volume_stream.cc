#include "hive/volume_stream.hh"
#include "hive/stream_reader.hh"
#include "hive/stream_writer.hh"
#include "hive/stream_service.hh"
#include "hive/volume_service.hh"
#include "hive/extent_datum.hh"
#include "hive/extent_store.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_request.hh"
#include "hive/journal/journal_service.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "hive/context/context_service.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/hive_plan.hh"
#include "hive/trail/trail_service.hh"

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
#include <boost/range/algorithm/find.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "atomic_cell.hh"
#include "bytes.hh"
#include <chrono>
#include <functional>
#include <cstdint>

using namespace std::chrono_literals;
namespace hive{
static logging::logger logger("volume_stream");

static inline bool is_me(gms::inet_address from){
    return from == utils::fb_utilities::get_broadcast_address();
}
    
double requests_stats::get_parameter(Option op){
    if(!requests.empty()){
        std::chrono::duration<double, std::nano> current_back_diff = get_current_time() - std::get<0>(requests.back());
        if(current_back_diff.count() > TIMEOUT){
            requests.clear();
            return 0;
        }
        std::chrono::duration<double, std::nano> current_front_diff = std::get<0>(requests.back()) - std::get<0>(requests.front());
        if(current_front_diff.count() > 0.0){
            switch(op){
                case bandwidth:{
                    int64_t sum_length = 0;
                    for(unsigned int i = 0; i < requests.size(); i++){
                        sum_length += std::get<1>(requests[i]);
                    }
                    return (sum_length / current_front_diff.count()) * NANO_TO_SECOND;
                }
                case latency:{
                    int64_t sum_latency = 0;
                    for(unsigned int i = 0; i < requests.size(); i++){
                        sum_latency += std::get<2>(requests[i]);
                    }
                    return sum_latency / requests.size();
                }
                case iops: default:
                    return (requests.size() / current_front_diff.count()) * NANO_TO_SECOND;
            }
        }
        if(1 == requests.size()){
            switch(op){
                case bandwidth:
                    return std::get<1>(requests.front());
                case latency:
                    return std::get<2>(requests.front());
                case iops: default:
                    return 1;
            }
        }
        logger.error("current_front_diff.count() {}, requests.size {}", current_front_diff.count(), requests.size());
        switch(op){
            case bandwidth:
                return MAX_BANDWIDTH;
            case latency:
                return 0;
            case iops: default:
                return MAX_IOPS;
        }
    }
    return 0;
}

volume_stream::volume_stream(const db::config& config, sstring volume_id, uint64_t requests_capacity)
    :_config(make_lw_shared<db::config>(config))
    ,_volume_id(volume_id)
    ,_read_requests(requests_capacity)
    ,_write_requests(requests_capacity)
{
    set_timer();
}

volume_stream::~volume_stream(){
}

void volume_stream::set_timer(){
    auto periodic = _config->periodic_print_performance_stats_in_s();
    if(periodic > 0) {
        _timer.set_callback(std::bind(&volume_stream::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(1)
            , std::experimental::optional<lowres_clock::duration>{std::chrono::seconds(periodic)});
    }
}

void volume_stream::on_timer(){
    print_performance_stats();
}

uint64_t volume_stream::get_iops(){
    uint64_t iops = std::lround(_write_requests.get_iops() + _read_requests.get_iops());
    logger.debug("collectd get iops {}", iops);
    return iops;
}

uint64_t volume_stream::get_read_iops(){
    uint64_t iops = std::lround(_read_requests.get_iops());
    logger.debug("collectd get read iops {}", iops);
    return iops;
}

uint64_t volume_stream::get_write_iops(){
    uint64_t iops = std::lround(_write_requests.get_iops());
    logger.debug("collectd get write iops {}", iops);
    return iops;
}

uint64_t volume_stream::get_bandwidth(){
    uint64_t bandwidth = std::lround(_write_requests.get_bandwidth() + _read_requests.get_bandwidth());
    logger.debug("collectd get bandwidth {}", bandwidth);
    return bandwidth;
}

uint64_t volume_stream::get_read_bandwidth(){
    uint64_t bandwidth = std::lround(_read_requests.get_bandwidth());
    logger.debug("collectd get read bandwidth {}", bandwidth);
    return bandwidth;
}

uint64_t volume_stream::get_write_bandwidth(){
    uint64_t bandwidth = std::lround(_write_requests.get_bandwidth());
    logger.debug("collectd get write bandwidth {}", bandwidth);
    return bandwidth;
}

uint64_t volume_stream::get_latency(){
    uint64_t latency = std::lround(std::max(_write_requests.get_latency(), _read_requests.get_latency()));
    logger.debug("collectd get latency {}", latency);
    return latency;
}

uint64_t volume_stream::get_read_latency(){
    uint64_t latency = std::lround(_read_requests.get_latency());
    logger.debug("collectd get read iops {}", latency);
    return latency;
}

uint64_t volume_stream::get_write_latency(){
    uint64_t latency = std::lround(_write_requests.get_latency());
    logger.debug("collectd get write iops {}", latency);
    return latency;
}

future<> volume_stream::init_metric(sstring volume_id, sstring cluster_uuid, sstring storage_pool_uuid, sstring container_uuid){
    sstring instance_id = "cluster_uuid="+cluster_uuid
                        + "&storage_pool_uuid="+storage_pool_uuid
                        + "&container_uuid="+container_uuid
                        + "&volume_uuid=" + _volume_id;
    _collectd_registrations = std::make_unique<scollectd::registrations>(scollectd::registrations({
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iops", "volume_iops")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_iops();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "bandwidth", "volume_bandwidth")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_bandwidth();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iolatency", "volume_iolatency")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_latency();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iops", "volume_iops_w")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_write_iops();})
        ), 
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "bandwidth", "volume_bandwidth_w")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_write_bandwidth();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iolatency", "volume_iolatency_w")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_write_latency();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iops", "volume_iops_r")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_read_iops();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "bandwidth", "volume_bandwidth_r")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_read_bandwidth();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "iolatency", "volume_iolatency_r")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, [this]{return get_read_latency();})
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "unavailable", "read")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, _stats.read_unavailables)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "unavailable", "write")
                , scollectd::make_typed(scollectd::data_type::ABSOLUTE, _stats.write_unavailables)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "hit_rate", "local_ssd_hit_rate")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _stats.local_ssd_hit_rate)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "hit_rate", "local_hdd_hit_rate")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _stats.local_hdd_hit_rate)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "hit_rate", "remote_ssd_hit_rate")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _stats.remote_ssd_hit_rate)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id(instance_id
                , scollectd::per_cpu_plugin_instance
                , "hit_rate", "remote_hdd_hit_rate")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _stats.remote_hdd_hit_rate)
        )
    }));
    return make_ready_future<>();
}
void volume_stream::print_performance_stats(){
    logger.error("periodic_print_performace_stats_in_s, iops(r+w):{}, max_latency(r/w_mean,ns):{}, bandwidth(r+w):{}, local_ssd_hit_rate(r):{}, local_hdd_hit_rate(r):{}, remote_ssd_hit_rate(r):{}, remote_hdd_hit_rate(r):{}"
        , get_iops()
        , get_latency()
        , get_bandwidth()
        , _stats.local_ssd_hit_rate
        , _stats.local_hdd_hit_rate
        , _stats.remote_ssd_hit_rate
        , _stats.remote_hdd_hit_rate);
}

future<> volume_stream::init(){
    logger.debug("[{}] start, volume_id:{}", __func__,  _volume_id);

    //tododl:yellow i think primary_journal init and context ensure need start by driver???
    //_primary_journal_id = hive_tools::generate_commitlog_id(_volume_id);
    return hive::get_local_journal_service().start_primary_journal(_volume_id)
    .then([this](){
        auto  primary_journal = hive::get_local_journal_service().get_primary_journal(_volume_id);
        lw_shared_ptr<volume_drainer> drainer = make_lw_shared<volume_drainer>(_volume_id
                                           , *_config
                                           , hive::get_local_stream_service().get_drain_manager()
                                           , *primary_journal
                                           , &(hive::get_local_stream_service().get_stats()));
        hive::get_local_stream_service().add_drainer(drainer);
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_volume_context(_volume_id);
    }).then([this](auto volume_context_entry){
        sstring cluster_uuid = volume_context_entry.get_cluster_uuid();
        sstring storage_pool_uuid = volume_context_entry.get_storage_pool_uuid();
        sstring container_uuid = volume_context_entry.get_container_uuid();
        return this->init_metric(_volume_id, cluster_uuid, storage_pool_uuid, container_uuid);
    }).then_wrapped([this](auto f){
        try{
            f.get();
            logger.debug("[init] done, volume_id:{}", _volume_id);
        }catch(...){
            std::ostringstream out;
            out << "[init] error, volume_id:" << _volume_id;
            out << ", exception:{}" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            throw std::runtime_error(error_info);
        }
    });
}

future<> volume_stream::rebuild(uint64_t vclock){
    logger.debug("[{}] start, volume_id:{}, vclock:{}", __func__,  _volume_id, vclock);

    //tododl:yellow i think primary_journal init and context ensure need start by driver???
    //_primary_journal_id = hive_tools::generate_commitlog_id(_volume_id);
    return hive::get_local_journal_service().rebuild_primary_journal(
            _volume_id, vclock).then([this](){
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_volume_context(_volume_id);
    }).then([this](auto volume_context_entry){
        sstring cluster_uuid = volume_context_entry.get_cluster_uuid();
        sstring storage_pool_uuid = volume_context_entry.get_storage_pool_uuid();
        sstring container_uuid = volume_context_entry.get_container_uuid();
        return this->init_metric(_volume_id, cluster_uuid, storage_pool_uuid, container_uuid);
    });
}

future<> volume_stream::stop(){
    logger.debug("[{}] start, volume_id:{}", __func__,  _volume_id);
    return hive::get_local_journal_service().stop_primary_journal(_volume_id);
}

void volume_stream::read_statistics(uint64_t read_length, uint64_t read_latency){
    auto& extent_store_proxy = hive::get_local_extent_store_proxy();
    auto extent_store_stats = extent_store_proxy.get_stats();
    _stats.local_ssd_hit_rate = extent_store_stats.read_requests ? 
        (double)extent_store_stats.local_ssd_hit_count/extent_store_stats.read_requests : 0.0;
    _stats.local_hdd_hit_rate = extent_store_stats.read_requests ? 
        (double)extent_store_stats.local_hdd_hit_count/extent_store_stats.read_requests : 0.0;
    _stats.remote_ssd_hit_rate = extent_store_stats.read_requests ? 
        (double)extent_store_stats.remote_ssd_hit_count/extent_store_stats.read_requests : 0.0;
    _stats.remote_hdd_hit_rate = extent_store_stats.read_requests ? 
        (double)extent_store_stats.remote_hdd_hit_count/extent_store_stats.read_requests : 0.0;
  
    _stats.read_requests++;
    _stats.read_bytes += read_length;
    _read_requests.add_request(read_length, read_latency);
}


void volume_stream::write_statistics(uint64_t write_length, uint64_t write_latency){
    _stats.write_requests++;
    _stats.write_bytes += write_length;
    _write_requests.add_request(write_length, write_latency);
}

//future<> volume_stream::rwrite_volume(hive_write_command write_cmd){
//    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
//    if(_config->debug_mode() && _config->write_debug_level() == 2){
//        logger.warn("[{}] do nothing, because the config write_debug_level = 2, write_cmd:{}"
//            , __func__, write_cmd);
//        return make_ready_future<>();
//    }
//
//    utils::latency_counter lc;
//    lc.start();
//    uint64_t latency;
//    auto length = write_cmd.length;
//    lw_shared_ptr<stream_writer> writer = make_lw_shared<stream_writer>(_volume_id, write_cmd);
//    return writer->execute().then_wrapped([this, lc, latency, length](auto f)mutable{
//        try {
//            f.get(); 
//            if(lc.is_start()){
//                latency = lc.stop().latency_in_nano();
//            }
//            this->write_statistics(length, latency);
//            return make_ready_future<>();
//        }catch(...){
//            if(lc.is_start()){
//                latency = lc.stop().latency_in_nano();
//            }
//            _stats.write_unavailables++;
//            _write_requests.add_request(length, latency);
//            std::ostringstream out;
//            out << "[rwrite_volume] error";
//            out << ", exception:" << std::current_exception();
//            auto error_info = out.str();
//            logger.error(error_info.c_str());
//            return make_exception_future<>(std::runtime_error(error_info));
//        }
//    });
//}
future<write_plan> volume_stream::get_write_plan(sstring volume_id, uint64_t offset, uint64_t length, lw_shared_ptr<bytes> buffer) {
    auto& volume_service = hive::get_local_volume_service();
    return volume_service.get_read_plan(volume_id, offset, length, buffer);
}

future<> volume_stream::rwrite_volume(hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    if(_config->debug_mode() && _config->write_debug_level() == 2){
        logger.warn("[{}] do nothing, because the config write_debug_level = 2, write_cmd:{}"
            , __func__, write_cmd);
        return make_ready_future<>();
    }

    utils::latency_counter lc;
    lc.start();

    uint64_t latency;
    auto volume_id = write_cmd.owner_id;
    auto offset = write_cmd.offset;
    auto length = write_cmd.length;
    lw_shared_ptr<bytes> buffer = make_lw_shared<bytes>(std::move(write_cmd.data)); 

    return get_write_plan(volume_id, offset, length, buffer).then([this, volume_id](auto plan){
        lw_shared_ptr<stream_writer> writer = make_lw_shared<stream_writer>(volume_id, plan);
        return writer->execute();
    }).then_wrapped([this, lc, latency, length](auto f){
        try{
            f.get(); 
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
            this->write_statistics(length, latency);
            return make_ready_future<>();
        }catch(...){
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
            _stats.write_unavailables++;
            _write_requests.add_request(length, latency);
            std::ostringstream out;
            out << "[rwrite_volume] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
        
    });
}

/////////////////////
//for read_volume
////////////////////
future<read_plan> volume_stream::get_read_plan(sstring volume_id, uint64_t offset, uint64_t length) {
    auto& volume_service = hive::get_local_volume_service();
    return volume_service.get_read_plan(volume_id, offset, length);
}

future<bytes> volume_stream::read_volume(hive_read_command read_cmd){
    logger.debug("[{}] start, read_cmd:{}", __func__, read_cmd);
    utils::latency_counter lc;
    lc.start();
    uint64_t latency;
    auto length = read_cmd.length;
    auto volume_id = read_cmd.owner_id;
    auto offset = read_cmd.offset;

    return get_read_plan(volume_id, offset, length).then([this, volume_id](auto plan){
        stream_reader reader(volume_id, plan);
        lw_shared_ptr<stream_reader> reader_ptr = make_lw_shared<stream_reader>(std::move(reader));
        return reader_ptr->execute();
    }).then_wrapped([this, lc, length, latency](auto fut) mutable{
        try{
            auto&& data = fut.get0();
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
            this->read_statistics(length, latency);
            return make_ready_future<bytes>(std::move(data));
        }catch (...) {
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
            _stats.read_unavailables++;
            _read_requests.add_request(length, latency);

            std::ostringstream out;
            out << "[read_volume] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str()); 
            throw std::runtime_error(error_info);
        }
    });
}


}//namespace hive



