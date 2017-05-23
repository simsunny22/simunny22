#include "secondary_journal.hh"

#include "log.hh"

namespace hive {

static logging::logger logger("secondary_journal");


secondary_journal::secondary_journal(sstring volume_id, lw_shared_ptr<db::config> config)
    : _volume_id(volume_id)
    , _config(config)
    , _create_semaphore(1)
{}

secondary_journal::~secondary_journal() {
}


future<> secondary_journal::create_segment(
        sstring volume_id, 
        sstring segment_id,
        uint64_t max_size) {
    auto path = _config->secondary_commitlog_directory();
    logger.debug("secondary_journal::create_segment  volume_id:{}, segment_id:{}, commitlog_path:{}"
        ,volume_id, segment_id, path);
    
    return journal_segment::make_secondary_segment(volume_id, segment_id
             , path, max_size).then([this, segment_id](auto segment_ptr){
         _segments.insert(std::make_pair(segment_id, segment_ptr));
         return make_ready_future<>();
    });
}


future<> secondary_journal::sync_segment(
        sstring segment_id,
        uint64_t offset_in_segment,
        uint64_t offset_in_volume, 
        sstring extent_id,
        uint64_t offset_in_extent, 
        uint64_t data_length, 
        uint64_t serialize_length, 
        uint64_t vclock, 
        bytes data){
    //logger.error("wztest secondary_journal::sync_commitlog segment_id:{}, extent_group_id:{} , extent_id:{}, offset:{}, size:{}"
    //    , segment_id, extent_group_id, extent_id, offset, size);
    auto it = _segments.find(segment_id);
    if(it != _segments.end()){
        auto segment_ptr = it->second;
        return segment_ptr->sync_secondary_segment(
            segment_id, 
            offset_in_segment,
            offset_in_volume,
            extent_id, 
            offset_in_extent, 
            data_length,
	    serialize_length,
	    vclock, 
            std::move(data));
        
    }
    std::ostringstream out;
    out << "[sync_commitlog error, segment_id:" << segment_id;
    out << ", exception:" << "no commitlog file";
    auto error_info = out.str();
    logger.error(error_info.c_str());
    return make_exception_future<>(std::runtime_error(error_info));
}

future<> secondary_journal::discard_segment(sstring segment_id){
    auto it = _segments.find(segment_id);
    if(it != _segments.end()){
        std::vector<future<>> futs;
        auto fut = it->second->discard_secondary_segment();
        futs.push_back(std::move(fut));

        for(auto begin = _segments.begin(); begin != it; begin ++){
            auto fut = begin->second->discard_secondary_segment();
            futs.push_back(std::move(fut));
        }

        return when_all(futs.begin(), futs.end()).then([this, segment_id](auto futs){
            try{
                for(auto& fut: futs){
                    fut.get();
                }
                auto it = _segments.find(segment_id);
                _segments.erase(_segments.begin(), ++it);
                return make_ready_future<>();
            }catch(...){
                std::ostringstream out;
                out << "[discard_commitlog error, segment_id:" << segment_id;
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
    out << "[discard_commitlog error, segment_id:" << segment_id;
    out << ", exception:" << "no commitlog file";
    auto error_info = out.str();
    logger.error(error_info.c_str());
    return make_exception_future<>(std::runtime_error(error_info));
}

std::vector<lw_shared_ptr<journal_segment>> secondary_journal::get_journal_segments(uint64_t vclock){
    std::vector<lw_shared_ptr<journal_segment>> segments;
    for(auto& it: _segments){
        it.second->rollback_for_rebuild(vclock);
        segments.push_back(std::move(it.second));
    }
    _segments.erase(_segments.begin(), _segments.end());
    return std::move(segments);
    
}

lw_shared_ptr<journal_segment> secondary_journal::get_segment(sstring segment_id) {
    auto itor = _segments.find(segment_id);
    if(itor != _segments.end()){
        return itor->second; 
    }
   
    auto error_info = sprint("not found segment(%s) in secondary_journal", segment_id);
    throw std::runtime_error(error_info);
}

future<std::map<sstring, std::vector<revision_data>>>
secondary_journal::get_journal_data(
    sstring segment_id, 
    std::map<sstring, std::vector<extent_group_revision>> revision_map){
    auto segment_ptr = get_segment(segment_id);
    return segment_ptr->get_revision_data(std::move(revision_map));
}



} //namespace hive
