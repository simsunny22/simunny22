#pragma once
#include "hive/journal/primary_journal.hh"
#include "hive/journal/volume_revision.hh"
#include "hive/commitlog/hive_commitlog.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/hive_result.hh"
#include "hive/hive_request.hh"
#include "hive/extent_datum.hh"
#include "hive/context/disk_context_entry.hh"
#include "hive/hive_plan.hh"
#include "hive/trail/access_trail.hh"
#include "hive/metadata_service/calculate/io_planner.hh"

#include "core/shared_ptr.hh"
#include "core/timer.hh"
#include "utils/histogram.hh"
#include <chrono>
#include <boost/circular_buffer.hpp>
#include "log.hh"

#define NANO_TO_SECOND 1000000000
#define TIMEOUT 2000000000
#define MAX_BANDWIDTH 1000000000
#define MAX_IOPS 100000

namespace hive{

struct stream_stats{
    int64_t write_requests = 0; 
    int64_t write_bytes = 0;
    int64_t read_requests = 0; 
    int64_t read_bytes = 0;
    double local_ssd_hit_rate = 0.0;
    double local_hdd_hit_rate = 0.0;
    double remote_ssd_hit_rate = 0.0;
    double remote_hdd_hit_rate = 0.0;
    int64_t write_unavailables = 0.0;
    int64_t read_unavailables = 0.0;
};

class requests_stats{
private:
    enum Option {bandwidth, iops, latency};

    boost::circular_buffer<std::tuple<std::chrono::system_clock::time_point, uint64_t, uint64_t>> requests;
    
    std::chrono::system_clock::time_point get_current_time(){
        return std::chrono::system_clock::now();
    }
    
    double get_parameter(Option op);

public:
    requests_stats(int64_t size):requests(size){}
    
    void add_request(uint64_t length, uint64_t latency){
        requests.push_back(std::make_tuple(get_current_time(), length, latency));
    }

    double get_iops(){
        return get_parameter(iops); 
    }

    double get_bandwidth(){
        return get_parameter(bandwidth); 
    }

    double get_latency(){
        return get_parameter(latency); 
    }
};

class volume_stream {
private:
    lw_shared_ptr<db::config> _config;
    sstring _volume_id;
    sstring _primary_journal_id;
    std::unique_ptr<scollectd::registrations> _collectd_registrations;
    
    stream_stats _stats;
    requests_stats _read_requests;
    requests_stats _write_requests;
    timer<lowres_clock> _timer;

private:
    void set_timer();
    void on_timer();

    //for read_volume
    future<read_plan>  get_read_plan(sstring volume_id, uint64_t offset, uint64_t length);
    future<write_plan> get_write_plan(sstring volume_id, uint64_t offset, uint64_t length, lw_shared_ptr<bytes> buffer);

public:
    volume_stream(const db::config& config, sstring volume_id, uint64_t requests_capacity = 256);
    volume_stream();
    ~volume_stream();

    future<> init();
    future<> rebuild(uint64_t vclock);
    future<> stop();
    future<> init_metric(sstring volume_id, sstring cluster_uuid, sstring storage_pool_uuid, sstring container_uuid);

    void print_performance_stats();

    bool cache_enabled(){
        return _config->hive_cache_enabled(); 
    }

    future<> rwrite_volume(hive_write_command write_cmd);
    future<bytes> read_volume(hive_read_command read_cmd);

    //for statistics
    void read_statistics(uint64_t read_length, uint64_t read_latency);
    void write_statistics(uint64_t read_length, uint64_t read_latency);
    uint64_t get_iops();
    uint64_t get_bandwidth();
    uint64_t get_latency();
    uint64_t get_write_iops();
    uint64_t get_write_bandwidth();
    uint64_t get_write_latency();
    uint64_t get_read_iops();
    uint64_t get_read_bandwidth();
    uint64_t get_read_latency();
}; //class volume_stream

}//namespace hive
