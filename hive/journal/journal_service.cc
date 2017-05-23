
#include "hive/journal/journal_service.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "db/config.hh"

#include "log.hh"
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "dht/i_partitioner.hh"
#include "sstables/key.hh"

#include "hive/stream_service.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"

namespace hive{
static logging::logger log_js("journal_service");
static size_t default_max_memtable_size = 4*1024*1024; //4M

distributed<journal_service> _the_journal_service;
using namespace exceptions;

static inline sstring get_local_ip() {
    return utils::fb_utilities::get_broadcast_address().to_sstring();
}

journal_service::~journal_service() {
}

journal_service::journal_service(const db::config& config, uint64_t total_segments_token)
    : _config(make_lw_shared<db::config>(config))
    //, _secondary_journal( make_lw_shared<secondary_journal>(std::ref(*_config)) )
    //, _secondary_journal( make_lw_shared<secondary_journal>(std::ref(*_config)) )
    ,_schema( hive_config::generate_schema() )
    ,_total_segments_token(total_segments_token)
{
    log_js.info("constructor start");
}

future<> journal_service::stop() {
    log_js.info("stop journal_service begin");
    //return _secondary_journal->stop().then([this]()mutable{
        log_js.info("stop secondary journal ok");
        std::vector<future<>> futures;
        for(auto& pair : _primary_journals) {
            auto f = pair.second->stop(); 
            futures.push_back(std::move(f));
        }

        return when_all(futures.begin(), futures.end()).then([](auto ret){
            try {
                for(auto& f : ret) {
                    f.get();
                }
            } catch (...) {
                throw std::runtime_error("stop primary journals failed"); 
            }
            log_js.info("stop primary journals ok");
            return make_ready_future<>();
        });
    //});
}

unsigned journal_service::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

//future<std::unique_ptr<hive::hive_commitlog>> journal_service::_make_primary_commitlog(sstring volume_id, sstring commitlog_id) {
//    return hive::hive_commitlog::create_commitlog(volume_id, commitlog_id, *_config
//            , _config->primary_commitlog_directory()).then([this, volume_id](auto commitlog) mutable {
//        auto commitlog_ptr = std::make_unique<hive::hive_commitlog>(std::move(commitlog));
//        return make_ready_future<std::unique_ptr<hive::hive_commitlog> >(std::move(commitlog_ptr));
//    });
//}


//future<lw_shared_ptr<journal_segment>> journal_service::_make_journal_segment(sstring volume_id){
//   return journal_segment::make_journal_segment(volume_id);
//}

journal_config journal_service::_build_journal_config() {
    journal_config cfg;
    cfg.datadir = "";
    cfg.enable_disk_reads = true;
    cfg.enable_disk_writes = !_config->enable_in_memory_data_store();
    cfg.enable_commitlog = true;
    cfg.enable_cache = _config->hive_cache_enabled();
    cfg.debug_mode = _config->debug_mode();
    cfg.write_debug_level = _config->write_debug_level();
    auto memtable_total_space = size_t(_config->per_memtable_max_size_in_mb()) << 20;
    if (!memtable_total_space) {
        memtable_total_space =  default_max_memtable_size; 
    }
    cfg.max_memtable_size = memtable_total_space;
    cfg.dirty_memory_region_group = &(hive::get_local_stream_service().get_dirty_memory_region_group());
    cfg.streaming_dirty_memory_region_group = &(hive::get_local_stream_service().get_streaming_dirty_memory_region_group());
    cfg.hive_stats = &(hive::get_local_stream_service().get_stats());
    cfg.pithos_server_url = _config->pithos_server_url();
    cfg.commit_to_pithos_for_drain_done = _config->commit_to_pithos_for_drain_done();
    cfg.disable_memtable = _config->disable_memtable();
    cfg.total_segments_token = _config->total_segments_token();
    cfg.volume_segments_token_ratio = _config->volume_segments_token_ratio(); 
    cfg.timing_drain_interval_in_seconds = _config->timing_drain_interval_in_seconds();

    return cfg;
}

future<> journal_service::_init_primary_journal(sstring volume_id) {
    log_js.debug("_init_primary_journal, start, volume_id:{}", volume_id);
    std::map<sstring, primary_journal_ptr>::iterator  itor = _primary_journals.find(volume_id);

    if(itor != _primary_journals.end()) {
        log_js.debug("_init_primary_journal, done, already exist volume_id:{}", volume_id);
        return make_ready_future<>(); 
    } else {
        journal_config cfg = this->_build_journal_config();
        auto total_segments_token = cfg.total_segments_token;
        auto volume_segments_token_ratio = cfg.volume_segments_token_ratio;

        //find 
        primary_journal_ptr primary_ptr = make_lw_shared<primary_journal>(
            volume_id
            , cfg
            , total_segments_token * volume_segments_token_ratio);

        _primary_journals.insert(std::make_pair(volume_id, primary_ptr));
        log_js.debug("_init_primary_journal, done, volume_id:{}", volume_id);
        return make_ready_future<>(); 
    }
}

future<> journal_service::_rebuild_primary_journal(sstring volume_id, uint64_t vclock) {
    log_js.debug("_rebuild_primary_journal, start, volume_id:{}", volume_id);
    return this->_rebuild_primary_journal_segments(volume_id, vclock).then([this
        , volume_id](auto segments)mutable{
        journal_config cfg = this->_build_journal_config();
        auto total_segments_token = cfg.total_segments_token;
        auto volume_segments_token_ratio = cfg.volume_segments_token_ratio;
        primary_journal_ptr primary_ptr = make_lw_shared<primary_journal>(
            volume_id
            , cfg
            , total_segments_token * volume_segments_token_ratio
            , std::move(segments));
        _primary_journals.insert(std::make_pair(volume_id, primary_ptr));
        auto it = _secondary_journals.find(volume_id);
        if(it != _secondary_journals.end()){
            _secondary_journals.erase(it);
        }
        return primary_ptr->init_for_rebuild();
    });
}

future<std::vector<lw_shared_ptr<journal_segment>>> 
journal_service::_rebuild_primary_journal_segments(sstring volume_id, uint64_t vclock){
    try{
        log_js.debug("journal_service::sync_secondary_segments .... sync secondary");
        auto secondary_journal_ptr = get_secondary_journal(volume_id);
        auto segments = secondary_journal_ptr->get_journal_segments(vclock);
        return make_ready_future<std::vector<lw_shared_ptr<journal_segment>>>(std::move(segments));
    }catch(...){
        log_js.warn("journal_service::sync_secondary_segments .... no secondary");
        std::vector<lw_shared_ptr<journal_segment>> segments;
        return make_ready_future<std::vector<lw_shared_ptr<journal_segment>>>(std::move(segments));
    }
}

primary_journal_ptr journal_service::get_primary_journal(sstring volume_id) {
    log_js.debug("get_primary_journal, start, volume_id:{}", volume_id);
    std::map<sstring, primary_journal_ptr>::iterator itor = _primary_journals.find(volume_id);
    if(itor != _primary_journals.end()) {
        return itor->second; 
    }else {
        throw std::runtime_error("can not found primary journal, volume_id:" + volume_id);  
    }
}

secondary_journal_ptr journal_service::get_or_create_secondary_journal(sstring volume_id){
    log_js.debug("create_secondary_journal, start, volume_id:{}", volume_id);
    std::map<sstring, secondary_journal_ptr>::iterator itor = _secondary_journals.find(volume_id);
    if(itor != _secondary_journals.end()) {
        return itor->second;
    }else{
        auto secondary_journal_ptr = make_lw_shared<secondary_journal>(volume_id, _config);
        _secondary_journals.insert(std::make_pair(volume_id, secondary_journal_ptr));
        return secondary_journal_ptr;
    }
}


secondary_journal_ptr journal_service::get_secondary_journal(sstring volume_id){
    log_js.debug("get_secondary_journal, start, volume_id:{}", volume_id);
    std::map<sstring, secondary_journal_ptr>::iterator itor = _secondary_journals.find(volume_id);
    if(itor != _secondary_journals.end()) {
        return itor->second; 
    }

    sstring error_info = "[get_secondray_journal] can not found secondary journal" ;
    error_info +=  ", cpu_id:"    + engine().cpu_id();
    error_info +=  ", volume_id:" + volume_id;
    log_js.error(error_info.c_str());
    throw std::runtime_error(error_info);  
}

future<> journal_service::start_primary_journal(sstring volume_id){
    log_js.debug("[{}] start, volume_id:{}", __func__, volume_id);
    std::map<sstring, primary_journal_ptr>::iterator itor = _primary_journals.find(volume_id);
    if(itor == _primary_journals.end()){
        return _init_primary_journal(volume_id);
    }
    sstring error_info = "start_primray_journar already has primary_journal";
    error_info += ", cpu_id"    + engine().cpu_id();
    error_info += ", volume_id" + volume_id;
    log_js.error(error_info.c_str());
    throw std::runtime_error(error_info);
    //return make_ready_future<>();
}

future<> journal_service::rebuild_primary_journal(sstring volume_id,  uint64_t vclock){
    log_js.debug("[{}] start, volume_id:{}", __func__, volume_id);
    std::map<sstring, primary_journal_ptr>::iterator itor1 = _primary_journals.find(volume_id);

    if(itor1 == _primary_journals.end()){
        return _rebuild_primary_journal(volume_id, vclock);
    }
    return itor1->second->init_for_rebuild();
}

future<> journal_service::stop_primary_journal(sstring volume_id) {
    log_js.debug("stop_primary_journal, start");
    auto it = _primary_journals.find(volume_id);

    if(it != _primary_journals.end()){
        auto primary_journal =  it->second;
        return primary_journal->stop_driver().then([this, it](){
           _primary_journals.erase(it);
           return make_ready_future<>(); 
        });
    }
    sstring error_info = "stop_primray_journar do not have primary_journal";
    error_info += ", cpu_id"    + engine().cpu_id();
    error_info += ", volume_id" + volume_id;
    log_js.error(error_info.c_str());
    throw std::runtime_error(error_info);
}


future<> journal_service::force_drain(std::vector<sstring> volume_ids) {
    log_js.debug("force_drain, start");
    std::vector<future<>> futures;
    for(auto volume_id : volume_ids) {
    log_js.debug("force_drain, volume_id:{}", volume_id);
        auto primary_journal =  get_primary_journal(volume_id);
        auto f = primary_journal->force_drain();
        futures.push_back(std::move(f));
    }
    
    return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> ret) {
        try {
           
            for(auto& f : ret){
               f.get();
            }
        }catch(...) {
            log_js.error("force_drain, error: {}",std::current_exception());
            throw std::runtime_error("force_drain failed");
        }
        return make_ready_future<>();
    });
}

future<> journal_service::force_drain_all() {
    log_js.debug("force_drain_all, start");
    std::vector<future<>> futures;
    for(auto& pair : _primary_journals) {
        auto primary_journal = pair.second;  
        auto f = primary_journal->force_drain();
        futures.push_back(std::move(f));
    }
    
    return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> ret) {
        try {
            for(auto& f : ret){
               f.get();
            }
        }catch(...) {
            log_js.error("force_drain_all, error: {}",std::current_exception());
            throw std::runtime_error("force_drain_all failed");
        }
        return make_ready_future<>();
    });
}

future<int> journal_service::check_force_drain(sstring volume_id){
    auto drainer_ptr = hive::get_local_stream_service().find_drainer(volume_id);
    int num = drainer_ptr->get_pending_drains();
    return make_ready_future<int>(num);
}


future<int> journal_service::check_force_drain_all(){
    int total_num = 0;
    for(auto& pair: _primary_journals){
        auto drainer_ptr = hive::get_local_stream_service().find_drainer(pair.first);
        int num = drainer_ptr->get_pending_drains();
        total_num = total_num + num;
    }   
    return make_ready_future<int>(total_num);
}

future<> journal_service::get_total_segments_token(){
    return _total_segments_token.wait(1).then([this](){
        //log_js.error("journal_service:get_segment_token current_num:{}", _total_segments_token.current());
        return make_ready_future<>();
    });
}

future<> journal_service::release_total_segments_token(uint64_t i){
    _total_segments_token.signal(i);
    return make_ready_future<>();
}


//for get_journal_data
future<en_data_location> journal_service::get_journal_data_location(sstring volume_id){
    auto& context_service = get_local_context_service();
    return context_service.get_or_pull_volume_context(volume_id).then([this, volume_id](auto volume_context){
        sstring local_ip = get_local_ip();
        auto& driver_node = volume_context.get_driver_node();
        if(driver_node.get_ip() == local_ip){
            return make_ready_future<en_data_location>(en_data_location::IN_PRIMARY_JOURNAL);
        }else {
            auto& replicate_nodes = volume_context.get_journal_nodes();
            for(auto& replicate_node : replicate_nodes){
                if(replicate_node.get_ip() == local_ip){
                    return make_ready_future<en_data_location>(en_data_location::IN_SECONDARY_JOURNAL);
                }
            }
        }
        auto error_info = sprint("can not found journal replicate data of volume(%s) in %s"
            , volume_id, local_ip);
        throw hive::invalid_request_exception(error_info);
    });
}

future<std::vector<volume_revision_view>>
journal_service::scan_journal_segment(sstring volume_id, sstring segment_id){
    auto primary_journal_ptr = get_primary_journal(volume_id);
    return primary_journal_ptr->get_volume_revision_view(segment_id);
}

future<std::map<sstring, std::vector<revision_data>>>
journal_service::get_journal_data_from_primary(
    sstring volume_id,
    sstring segment_id,
    std::map<sstring, std::vector<extent_group_revision>> revision_map){
    auto primary_journal_ptr = get_primary_journal(volume_id);
    return primary_journal_ptr->get_journal_data(segment_id, std::move(revision_map));
}

future<std::map<sstring, std::vector<revision_data>>>
journal_service::get_journal_data_from_second(
    sstring volume_id,
    sstring segment_id,
    std::map<sstring, std::vector<extent_group_revision>> revision_map){
    auto secondary_journal_ptr = get_secondary_journal(volume_id);
    return secondary_journal_ptr->get_journal_data(segment_id, std::move(revision_map));
}

future<std::map<sstring, std::vector<revision_data>>>
journal_service::get_journal_data(
    sstring volume_id,
    sstring segment_id,
    std::map<sstring, std::vector<extent_group_revision>> revision_map){
    return get_journal_data_location(volume_id).then([this, volume_id, segment_id,
            revision_map = std::move(revision_map)](auto location)mutable{
        if(en_data_location::IN_PRIMARY_JOURNAL == location) {
            return this->get_journal_data_from_primary(volume_id, segment_id, std::move(revision_map)); 
        }else if (en_data_location::IN_SECONDARY_JOURNAL == location){
            return this->get_journal_data_from_second(volume_id, segment_id, std::move(revision_map)); 
        }

        auto error_info = sprint("can not found journal replicate data of volume(%s) in %s"
            , volume_id, get_local_ip());
        throw hive::invalid_request_exception(error_info);
    });
}

} //namespace hive
