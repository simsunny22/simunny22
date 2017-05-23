#pragma once
#include "core/distributed.hh"
#include "utils/histogram.hh"

#include "hive/drain/drain_manager.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/drain/volume_drainer.hh"
#include "hive/volume_stream.hh"

namespace db{
    class config;
}

namespace hive{
struct hive_stats {
    int64_t pending_memtables_drains_count = 0;
    int64_t pending_memtables_drains_bytes = 0;
};

class stream_service: public seastar::async_sharded_service<stream_service> {
private:
    std::unique_ptr<db::config> _cfg;
    std::map<sstring, lw_shared_ptr<volume_stream> > _streams;
    std::map<sstring, lw_shared_ptr<volume_drainer> > _drainers;

    logalloc::region_group _dirty_memory_region_group;
    logalloc::region_group _streaming_dirty_memory_region_group;
    drain_manager _drain_manager;
    hive_stats _hive_stats;

public:
    stream_service();
    stream_service(const db::config& cfg);
    ~stream_service();

    void start_drain_manager();
    future<> stop_drain_manager();

    future<> stop();


    std::map<sstring, lw_shared_ptr<volume_stream> >& get_volume_streams(){
        return _streams;
    }

    const db::config& get_config() const {
        return *_cfg;
    }

    future<lw_shared_ptr<volume_stream>> find_or_create_stream(sstring volume_id);
    future<lw_shared_ptr<volume_stream>> find_stream(sstring volume_id);


    drain_manager& get_drain_manager(){
        return _drain_manager;
    }

    hive_stats& get_stats(){
        return _hive_stats;  
    }

    logalloc::region_group& get_dirty_memory_region_group(){
        return _dirty_memory_region_group;
    }

    logalloc::region_group& get_streaming_dirty_memory_region_group(){
        return _streaming_dirty_memory_region_group;
    }

    unsigned shard_of(const sstring extent_group_id);
    future<> start_volume_stream(sstring volume_id);
    future<> rebuild_volume_stream(sstring volume_id, uint64_t vclock);
    future<> stop_volume_stream(sstring volume_id);

    lw_shared_ptr<volume_drainer> find_drainer(sstring volume_id);
    void add_drainer(lw_shared_ptr<volume_drainer> drainer);
    future<> drain_task_group_done(drain_task_group task_group);
};

extern distributed<stream_service> _the_stream_service;

inline distributed<stream_service>& get_stream_service() {
    return _the_stream_service;
}

inline stream_service& get_local_stream_service() {
    return _the_stream_service.local();
}

inline shared_ptr<stream_service> get_local_shared_stream_service() {
    return _the_stream_service.local_shared();
}

}//namespace hive
