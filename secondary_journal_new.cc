#include "secondary_journal_new.hh"

#include "log.hh"
//#include "db/config.hh"
//#include "core/seastar.hh"
//
//#include "hive/hive_config.hh"
//#include "hive/hive_tools.hh"
//#include "hive/journal/journal_service.hh"

namespace hive {

static logging::logger logger("secondary_journal_new");


secondary_journal_new::secondary_journal_new(lw_shared_ptr<db::config> config)
    : _config(config)
    , _create_semaphore(1)
{}

secondary_journal_new::~secondary_journal_new() {
}


//future<> secondary_journal_new::stop() {
//    logger.debug("stop begin");
//    std::vector<future<>> futures;
//    for(auto& itor: _commitlogs) {
//        auto f = itor.second->shutdown();
//        futures.push_back(std::move(f));
//    }
//   
//    return when_all(futures.begin(), futures.end()).discard_result();
//}

//future<lw_shared_ptr<segmnet_commitlog>> 
//secondary_journal_new::get_or_create_commitlog(sstring volume_id, sstring commitlog_id) {
//    auto itor = _commitlogs.find(commitlog_id);
//    if(itor != _commitlogs.end()){
//        return make_ready_future<lw_shared_ptr<segment_commitlog>>(itor->second);
//    }
//   
//    return _create_semaphore.wait().then([this, commitlog_id](){
//        auto itor = _commitlogs.find(commitlog_id);
//        if(itor != _commitlogs.end()){
//            return make_ready_future<lw_shared_ptr<segment_commitlog>>(itor->second);
//        }
//        return create_commitlog(volume_id, commitlog_id);
//    }).finally([this](){
//        _create_semaphore.signal(); 
//    }); 
//}

future<> secondary_journal_new::create_segment(
        sstring volume_id, 
        sstring commitlog_id,
        uint64_t max_size) {
    auto path = _config->secondary_commitlog_directory();
    logger.debug("secondary_journal_new::create_segment  volume_id:{}, commitlog_id:{}, commitlog_path:{}"
        ,volume_id, commitlog_id, path);
    
    return journal_segment::make_secondary_segment(volume_id, commitlog_id
             , path, max_size).then([this, commitlog_id](auto segment_ptr){
         _segments.insert(std::make_pair(commitlog_id, segment_ptr));
         return make_ready_future<>();
    });
}


future<> secondary_journal_new::sync_segment(sstring commitlog_id, sstring extent_group_id, sstring extent_id
        , uint64_t offset, uint64_t size, uint64_t vclock, bytes data){
    //logger.error("wztest secondary_journal_new::sync_commitlog commitlog_id:{}, extent_group_id:{} , extent_id:{}, offset:{}, size:{}"
    //    , commitlog_id, extent_group_id, extent_id, offset, size);
    auto it = _segments.find(commitlog_id);
    if(it != _segments.end()){
        auto segment_ptr = it->second;
        return segment_ptr->sync_secondary_segment(commitlog_id, extent_group_id, extent_id
            , offset, size, vclock, std::move(data));
        
    }
    std::ostringstream out;
    out << "[sync_commitlog error, commitlog_id:" << commitlog_id;
    out << ", exception:" << "no commitlog file";
    auto error_info = out.str();
    logger.error(error_info.c_str());
    return make_exception_future<>(std::runtime_error(error_info));
}

future<> secondary_journal_new::discard_segment(sstring commitlog_id){
    auto it = _segments.find(commitlog_id);
    if(it != _segments.end()){
        std::vector<future<>> futs;
        auto fut = it->second->discard_secondary_commitlog();
        futs.push_back(std::move(fut));

        for(auto begin = _segments.begin(); begin != it; begin ++){
            auto fut = begin->second->discard_secondary_commitlog();
            futs.push_back(std::move(fut));
        }

        return when_all(futs.begin(), futs.end()).then([this, commitlog_id](auto futs){
            try{
                for(auto& fut: futs){
                    fut.get();
                }
                auto it = _segments.find(commitlog_id);
                _segments.erase(_segments.begin(), ++it);
                return make_ready_future<>();
            }catch(...){
                std::ostringstream out;
                out << "[discard_commitlog error, commitlog_id:" << commitlog_id;
                out << ", exception:" <<  std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());
                return make_exception_future<>(std::runtime_error(error_info));
            }
        });
        #if 0
        auto segment_ptr = it->second;
        return segment_ptr->discard_secondary_commitlog().then([this, it = std::move(it)](){
            _segments.erase(it);
            return make_ready_future<>();
        });
        #endif
    }
    std::ostringstream out;
    out << "[discard_commitlog error, commitlog_id:" << commitlog_id;
    out << ", exception:" << "no commitlog file";
    auto error_info = out.str();
    logger.error(error_info.c_str());
    return make_exception_future<>(std::runtime_error(error_info));
}

std::vector<lw_shared_ptr<journal_segment>> secondary_journal_new::get_journal_segments(uint64_t vclock){
    std::vector<lw_shared_ptr<journal_segment>> segments;
    for(auto& it: _segments){
        it.second->rollback_for_rebuild(vclock);
        segments.push_back(std::move(it.second));
    }
    _segments.erase(_segments.begin(), _segments.end());
    return std::move(segments);
    
}

} //namespace hive
