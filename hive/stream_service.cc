
#include "hive/stream_service.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "db/config.hh"

#include "log.hh"
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "dht/i_partitioner.hh"
#include "sstables/key.hh"


namespace hive{
static logging::logger logger("stream_service");

distributed<stream_service> _the_stream_service;
using namespace exceptions;

stream_service::~stream_service() {
}

stream_service::stream_service()
    : _cfg(std::make_unique<db::config>(db::config())){
    logger.info("constructor start");
    
    start_drain_manager();
}

stream_service::stream_service(const db::config& cfg)
    : _cfg(std::make_unique<db::config>(cfg)) {
    logger.info("constructor start");
    
    start_drain_manager();
}

void stream_service::start_drain_manager() {
    int drain_manager_task_num = _cfg->drain_manager_task_num();
    _drain_manager.start(drain_manager_task_num);
}

future<> stream_service::stop_drain_manager() {
    return _drain_manager.stop();
}

future<> stream_service::stop() {
    return make_ready_future<>();
}

unsigned stream_service::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

future<> stream_service::start_volume_stream(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}", __func__,  volume_id);
    return find_or_create_stream(volume_id).then([volume_id](auto stream){
        logger.debug("[start_volume_stream] done, volume_id:{}", volume_id);
        return make_ready_future<>();
    });
}


future<lw_shared_ptr<volume_stream>> stream_service::find_or_create_stream(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}", __func__, volume_id);
    std::map<sstring, lw_shared_ptr<volume_stream>>::iterator it = _streams.find(volume_id);
    if(it != _streams.end()){
        logger.debug("[{}] find success,volume_id:{}", __func__, volume_id);
        return make_ready_future<lw_shared_ptr<volume_stream>>(it->second);
    }else{
        logger.debug("[{}] find failed need create, volume_id:{}", __func__, volume_id);
        lw_shared_ptr<volume_stream>  stream_ptr = make_lw_shared<volume_stream>(std::ref(*_cfg), volume_id);
        return stream_ptr->init().then([this, stream_ptr, volume_id](){
            _streams.insert(std::pair<sstring, lw_shared_ptr<volume_stream>>(volume_id, stream_ptr));
            return make_ready_future<lw_shared_ptr<volume_stream>>(stream_ptr);
        });
    }
}

future<> stream_service::rebuild_volume_stream(sstring volume_id, uint64_t vclock){
    std::map<sstring, lw_shared_ptr<volume_stream>>::iterator it = _streams.find(volume_id);
    if(it == _streams.end()){
        lw_shared_ptr<volume_stream>  stream_ptr = make_lw_shared<volume_stream>(std::ref(*_cfg), volume_id);
        return stream_ptr->rebuild(vclock).then([this, stream_ptr, volume_id](){
            _streams.insert(std::pair<sstring, lw_shared_ptr<volume_stream>>(volume_id, stream_ptr));
            return make_ready_future<>();
        });       
    }else{
        sstring error_info = "volume stream has already exist" ;
        error_info += ", cpu_id:"    + engine().cpu_id();
        error_info += ", volume_id:" + volume_id;
        logger.error(error_info.c_str());
        return make_ready_future<>();
    }
}

future<> stream_service::stop_volume_stream(sstring volume_id){
    std::map<sstring, lw_shared_ptr<volume_stream>>::iterator it = _streams.find(volume_id);
    if(it != _streams.end()){
        auto stream_ptr = it->second;
        return stream_ptr->stop().then([this, it](){
            _streams.erase(it);
            return make_ready_future<>();
        });       
    }else{
        sstring error_info = "volume stream do not exist" ;
        error_info += ", cpu_id:"    + engine().cpu_id();
        error_info += ", volume_id:" + volume_id;
        logger.error(error_info.c_str());
        return make_ready_future<>();
    }
}

future<lw_shared_ptr<volume_stream>> stream_service::find_stream(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}, current _streams.size:{}", __func__, volume_id, _streams.size());
    std::map<sstring, lw_shared_ptr<volume_stream>>::iterator it = _streams.find(volume_id);
    if(it != _streams.end()){
        return make_ready_future<lw_shared_ptr<volume_stream>>(it->second);
    }else{
        throw std::runtime_error("can not find stream of volume_id:"+volume_id);
    }
}

//future<> stream_service::recover_commitlog(){
//    try{
//        auto streams = this->get_volume_streams();
//        return parallel_for_each(streams, [this] (auto& pair){
//            auto stream = pair.second;
//            return stream->recover_commitlog();
//        });
//    }catch(const std::exception& e){
//        logger.error(" (error  {})", e.what());
//        throw std::runtime_error("recover_commilog failed");
//    };
//}

//future<> stream_service::drain_streams(){
//    return parallel_for_each(_streams, [this] (auto& pair){
//        auto stream= pair.second;
//        return stream->get_primary_journal()->drain();
//    });
//
//}

future<> stream_service::drain_task_group_done(drain_task_group result){
    auto drainer_ptr = find_drainer(result.volume_id);
    return drainer_ptr->drain_task_group_done(result);
}


lw_shared_ptr<volume_drainer> stream_service::find_drainer(sstring volume_id){
    std::map<sstring, lw_shared_ptr<volume_drainer>>::iterator it = _drainers.find(volume_id);
    if(it != _drainers.end()){
        return it->second;
    }else{
        throw std::runtime_error("can not find drainer of volume_id:"+volume_id);
    }
   
}

void stream_service::add_drainer(lw_shared_ptr<volume_drainer> drainer){
    std::map<sstring, lw_shared_ptr<volume_drainer>>::iterator it = _drainers.find(drainer->get_volume_id());
    if(it != _drainers.end()){
        _drainers.erase(it);
        _drainers.insert(std::pair<sstring, lw_shared_ptr<volume_drainer>>(drainer->get_volume_id(), drainer));
    }else{
        _drainers.insert(std::pair<sstring, lw_shared_ptr<volume_drainer>>(drainer->get_volume_id(), drainer));
    }

}

}
