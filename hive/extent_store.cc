#include "dht/i_partitioner.hh"
#include "log.hh"
#include "hive/extent_store.hh"
#include "hive/context/context_service.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "core/do_with.hh"
#include <core/fstream.hh>
#include <core/align.hh>
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "core/sleep.hh"
#include "utils/logalloc.hh"
#include "bytes_ostream.hh"
#include "hive/file_store.hh"
#include "hive/hive_result.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "bytes.hh"
#include "types.hh"

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include <map>
//#include "hive/log/log_service.hh"
//#include "hive/log/access_map_log.hh"

using namespace std::chrono_literals;
using namespace query;


namespace hive {
static logging::logger logger("extent_store");

distributed<extent_store> _the_extent_store;

extent_store::extent_store(){
   logger.info( "constructor start" );

   //auto periodic = hive_config::wash_monad_queue_periodic;
   //_timer.set_callback(std::bind(&extent_store::on_timer, this));
   //_timer.arm(lowres_clock::now()+std::chrono::seconds(1)
   //         , std::experimental::optional<lowres_clock::duration> {std::chrono::seconds(periodic)}
   //         );
}

extent_store::~extent_store()
{
}

future<> extent_store::stop() {
    return make_ready_future<>();
}

unsigned extent_store::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

#if 0
//tododl:should abandon
future<lw_shared_ptr<hive_result>> 
extent_store::read_extent_group(lw_shared_ptr<hive_read_command> read_cmd){
    logger.debug("[{}] start, read_cmd:{}", __func__, read_cmd); 

    auto extent_group_id = read_cmd->extent_group_id;
    return with_monad(extent_group_id, [this, read_cmd, extent_group_id](){
        logger.debug("[read_extent_group] in monad, read_cmd:{}", read_cmd);
        //tododl:yellow need use other method replace this [0]
        sstring disk_id = hive_tools::split_to_vector(read_cmd->disk_ids, ":")[0]; 
    
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_disk_context(disk_id).then([this, read_cmd, disk_id](auto disk_entry)mutable{
            sstring file_path = hive_tools::build_extent_group_file_path(
                disk_entry.get_mount_path(),
                read_cmd->extent_group_id);
    
            auto offset = read_cmd->extent_offset_in_group + read_cmd->data_offset_in_extent;
            auto length = read_cmd->length;
            
            return hive::read_file_ex(file_path, offset, length).then([disk_id](bytes&& data){
                //tododl:yellow why need data copy here???
                bytes_ostream result_buf;
                bytes_view data_view(data.begin(), data.size());
                result_buf.write(data_view);
                lw_shared_ptr<hive_result> ret = make_lw_shared<hive_result>(std::move(result_buf), std::move(disk_id));
                return make_ready_future<lw_shared_ptr<hive_result>>(std::move(ret));
            });
        }).finally([this, disk_id, read_cmd]{
        });
    });
}
#endif

future<bytes> 
extent_store::read_extent_group(
    sstring extent_group_id
    , uint64_t offset
    , uint64_t length
    , sstring disk_mount_path){
    logger.debug("[{}] start, extent_group_id:{}, offset:{}, length:{}, disk_mount_path:{}"
        , __func__, extent_group_id, offset, length, disk_mount_path);

    return with_monad(extent_group_id, [this, extent_group_id, offset, length, disk_mount_path](){
        auto file_path = hive_tools::build_extent_group_file_path(disk_mount_path, extent_group_id);
        return hive::read_file_ex(file_path, offset, length);
    });
}

//tododl:yellow should abandon
future<> extent_store::create_extent_group(sstring extent_group_id, sstring disk_ids){
    logger.debug("[{}] start, extent_group_id:{}, disk_ids:{}", __func__, extent_group_id, disk_ids);
    return with_monad(extent_group_id, [this, extent_group_id, disk_ids]{
        //FIXME tododl:yellow need use other method replace this [0]
        sstring disk_id = hive_tools::split_to_vector(disk_ids, ":")[0];
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_disk_context(disk_id).then([extent_group_id](auto disk_entry){
            sstring file_path = hive_tools::build_extent_group_file_path(
                disk_entry.get_mount_path(),
                extent_group_id);
            logger.debug("[create_extent_group] will create, path:{}", file_path);
            return hive::create_file(file_path, hive_config::extent_group_size);
        });
    });
}

future<> extent_store::create_extent_group_ex(sstring extent_group_id, sstring path){
    logger.debug( "[{}] start, extent_group_id:{}, path:{}", __func__, extent_group_id, path);
    return with_monad(extent_group_id, [this, extent_group_id, path](){
        auto file_path = hive_tools::build_extent_group_file_path(path, extent_group_id);
        return hive::create_file(file_path, hive_config::extent_group_size);
    });
}

future<> extent_store::create_extent_group_with_data(
    sstring extent_group_id
    , sstring disk_mount_path
    , bytes data){
    logger.debug( "[{}] start, extent_group_id:{}, disk_mount_path:{}, data.size:{}"
        , __func__, extent_group_id, disk_mount_path, data.size());

    return with_monad(extent_group_id, [extent_group_id, disk_mount_path, data=std::move(data)]()mutable{
        auto file_path = hive_tools::build_extent_group_file_path(disk_mount_path, extent_group_id);
        return hive::create_file(file_path, hive_config::extent_group_size).then(
                [file_path, data=std::move(data)]()mutable{
            auto offset = 0;
            auto length = hive_config::extent_group_size;
            assert(length == data.size());
            return do_with(std::move(data), [file_path, offset, length](auto& data){
                return hive::write_file(file_path, offset, length, data);  
            });
        });
    });
}

future<> extent_store::delete_extent_group(sstring extent_group_id, sstring path){
    logger.debug( "[{}] start, extent_group_id:{}, path:{}", __func__, extent_group_id, path);
    auto file_path = hive_tools::build_extent_group_file_path(path, extent_group_id);
    return hive::delete_file(file_path);
}

future<> extent_store::write_revisions(sstring file_path, std::vector<extent_revision>& revisions) {
    return seastar::async([this, file_path, &revisions]() mutable {
        for(auto& revision : revisions) {
            auto length = revision.length; 
            auto offset = revision.extent_offset_in_group + revision.data_offset_in_extent;
            hive::write_file(file_path, offset, length, revision.data).get();
        }
    });
}

future<> extent_store::write_revisions(sstring file_path, std::vector<revision_data> revision_datas) {
    return seastar::async([this, file_path, revision_datas=std::move(revision_datas)]() mutable {
        hive::write_file_batch(file_path, std::move(revision_datas)).get();
        //for(auto& revision_data : revision_datas) {
        //    auto length = revision_data.length; 
        //    auto offset = revision_data.offset_in_extent_group;
        //    hive::write_file(file_path, offset, length, revision_data.data).get();
        //}
    });
}

//tododl:should abandon
//future<> extent_store::write_revisions(sstring path, extent_revision_set& revision_set, sstring target_disk_id) {
//    return seastar::async([this, path, &revision_set, target_disk_id]() mutable {
//        for(auto& revision : revision_set.get_revisions()) {
//            auto length = revision.length; 
//            auto offset = revision.extent_offset_in_group + revision.data_offset_in_extent;
//            hive::write_file(path, offset, length, revision.data).get();
//        }
//    });
//}

//tododl:yellow should abandon
//future<> extent_store::rwrite_extent_group(extent_revision_set revision_set, sstring target_disk_id) {
//    logger.debug( "[{}] start, target_disk_id:{}", __func__, target_disk_id);
//    sstring extent_group_id = revision_set.extent_group_id;
//    return with_monad(extent_group_id, [this, revision_set=std::move(revision_set), target_disk_id]()mutable{
//        auto& context_service = hive::get_local_context_service();
//        return context_service.get_or_pull_disk_context(target_disk_id).then(
//                [this, revision_set=std::move(revision_set), target_disk_id](auto disk_entry)mutable{
//            sstring extent_group_id = revision_set.extent_group_id; 
//            sstring file_path = hive_tools::build_extent_group_file_path(
//                disk_entry.get_mount_path(),
//                extent_group_id);
//            return this->write_revisions(file_path, std::move(revision_set), target_disk_id);
//        });
//    });
//}

future<> extent_store::rwrite_extent_group(
    sstring extent_group_id
    , sstring disk_mount_path
    , std::vector<revision_data> revision_datas) {
    return with_monad(extent_group_id, [this, extent_group_id, disk_mount_path
            , revision_datas]()mutable{
        auto file_path = hive_tools::build_extent_group_file_path(disk_mount_path, extent_group_id);
        return write_revisions(file_path, std::move(revision_datas));
    });
}

future<> extent_store::rwrite_extent_group(
    sstring extent_group_id
    , sstring disk_mount_path
    , std::vector<extent_revision>& revisions) {
    return with_monad(extent_group_id, [this, extent_group_id, disk_mount_path
            , &revisions]()mutable{
        auto file_path = hive_tools::build_extent_group_file_path(disk_mount_path, extent_group_id);
        return write_revisions(file_path, revisions);
    });
}
future<> extent_store::write_extent_group_exactly(sstring extent_group_id
                                                , size_t offset
                                                , size_t length
                                                , bytes data 
                                                , sstring target_disk_id){
    logger.debug("{} start, extent_group_id:{}, offset:{}, length:{}, data.size:{}, target_disk_id:{}"
        , __func__, extent_group_id, offset, length, data.size(), target_disk_id);

    return with_monad(extent_group_id, [this, extent_group_id, offset, length
            , data=std::move(data), target_disk_id]()mutable{
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_disk_context(target_disk_id).then(
                [this, extent_group_id, offset, length, data=std::move(data)](auto disk_entry)mutable{
            
            sstring file_path = hive_tools::build_extent_group_file_path(
                disk_entry.get_mount_path(),
                extent_group_id);

            return do_with(std::move(data), [file_path, offset, length](auto& data){
                return hive::write_file(file_path, offset, length, data);
            });
        });
    });
}

future<> extent_store::touch_extent_group_file(sstring extent_group_id
                                             , sstring target_disk_id
                                             , size_t truncate_size){
    logger.debug("[{}] start, extent_group_id:{}, target_disk_id:{}", __func__, extent_group_id, target_disk_id);
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(target_disk_id).then(
            [this, extent_group_id, truncate_size](auto disk_entry)mutable{

        sstring file_path = hive_tools::build_extent_group_file_path(
            disk_entry.get_mount_path(),
            extent_group_id);
        logger.debug("[touch_extent_group_file] touch start file_path:{}", file_path);
        return hive::create_file(file_path, truncate_size);
    });
}

//bool extent_store::check_context(sstring extent_group_id, sstring target_disk_id){
//    auto& context_service = hive::get_local_context_service();
//    auto context = context_service.get_or_pull_extent_group_context(extent_group_id);
//    if(!context.second){
//        return false;
//    }
//   
//    auto context_entry = context.first;
//    if(!context_entry.is_credible()){
//        auto context_tmp = context_service.pull_extent_group_context(extent_group_id);
//        if(!context_tmp.second){
//            return false; 
//        }
//        context_entry = context_tmp.first;
//    }
//  
//    if(!context_entry.check_disk_id_exist(target_disk_id)){
//        return false; 
//    }
//
//    return true;
//}


//for monad 
//void extent_store::on_timer(){
//    wash_monad_queue();
//}
//
//void extent_store::wash_monad_queue(){
//    std::map<sstring, hive_shared_mutex>::iterator itor;
//    for(itor = _monad_queue.begin(); itor != _monad_queue.end(); itor++){
//        if(itor->second.is_idle()){
//            _monad_queue.erase(itor); 
//        }
//    }
//}

hive_shared_mutex& extent_store::get_or_create_mutex(sstring extent_group_id){
    std::map<sstring, hive_shared_mutex>::iterator itor = _monad_queue.find(extent_group_id);
    if(itor != _monad_queue.end()){
        return itor->second; 
    }else{
        auto ret = _monad_queue.insert(std::make_pair(extent_group_id, hive_shared_mutex()));
        if(true == ret.second){
            //get_or_create_mutex_timestamp(extent_group_id);
            return ret.first->second; 
        }else{
            throw std::runtime_error("error, insert map error[extent_store::get_or_create_mutex]"); 
        }
    }
}

//int64_t extent_store::get_or_create_mutex_timestamp(sstring extent_group_id){
//   std::map<sstring, int64_t>::iterator itor = _monad_timestamp.find(extent_group_id);
//   if(itor != _monad_timestamp.end()){
//       //logger.debug("wztest extent-store get_mutex_timestamp time:{}", itor->second);       
//       return itor->second;
//   }else{
//       auto now_time =  hive_tools::get_current_time();
//       auto ret = _monad_timestamp.insert(std::make_pair(extent_group_id, now_time));
//       //logger.debug("wztest extent-store create_mutex_timestamp time:{}",now_time);       
//       if(true == ret.second){
//           return ret.first->second;
//       }else{
//           throw std::runtime_error("error, insert map error[extent_store::get_or_create_mutex_timestamp]"); 
//       }
//   }
//}
//
//void extent_store::update_mutex_timestamp_for_test(sstring extent_group_id){
//    auto last_time = get_or_create_mutex_timestamp(extent_group_id);
//    auto now_time = hive_tools::get_current_time();
//    //logger.debug("wztest extent-store update_mutex_timestamp last_time:{}, now_time:{}",last_time, now_time);       
//    assert(last_time <= now_time);
//    _monad_timestamp.erase(extent_group_id);
//    auto ret = _monad_timestamp.insert(std::make_pair(extent_group_id, now_time));
//    if(true == ret.second){
//    }else{
//        throw std::runtime_error("error, insert map error[extent_store::update_mutex_timestamp]"); 
//    }
//}

future<sstring> extent_store::get_extent_group_md5(sstring extent_group_id, uint64_t offset
        , uint64_t length, sstring disk_id) {
    assert(offset >= 0 && offset+length <= hive_config::extent_group_size);
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then(
            [this, extent_group_id, offset, length](auto disk_entry)mutable{
        
        sstring file_path = hive_tools::build_extent_group_file_path(
            disk_entry.get_mount_path(),
            extent_group_id);
        return hive::read_file_ex(file_path, offset, length).then([this](bytes&& data){
            sstring md5 = hive_tools::calculate_md5(data); 
            return make_ready_future<sstring>(std::move(md5));
        });
    });
}


} //namespace hive
