#include "hive/stream_reader.hh"
#include "hive/stream_service.hh"
#include "hive/volume_service.hh"
#include "hive/extent_datum.hh"
#include "hive/extent_store.hh"
#include "hive/hive_service.hh"
#include "hive/hive_config.hh"
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
static logging::logger logger("stream_reader");
static uint64_t CACHE_SLAB_SIZE = 64*1024;

static inline bool is_me(gms::inet_address from){
    return from == utils::fb_utilities::get_broadcast_address();
}
static inline void throw_exception(sstring msg){
    std::ostringstream out;
    out << msg;
    auto error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);
}

static std::string make_cache_key(sstring extent_id, uint64_t offset) {
    std::string off_str = boost::lexical_cast<std::string>(offset);
    std::string key_str = std::string(extent_id.begin(), extent_id.size()) + ":" + off_str;
    return key_str;
}


stream_reader::stream_reader(sstring volume_id, read_plan plan)
    :_volume_id(volume_id)
    ,_read_plan(plan)
{
    _cache_enabled = hive::get_local_hive_service().get_hive_config()->hive_cache_enabled();
}

stream_reader::~stream_reader(){
}

/////////////////////
//for read_volume
////////////////////
future<std::vector<hive_read_subcommand>>
stream_reader::build_read_subcommand(){
    std::vector<hive_read_subcommand> sub_cmds;
    for(auto action : _read_plan.read_actions) {
      hive_read_subcommand read_cmd(
          _volume_id,
          action.extent_group_id,
          action.extent_id,
          action.extent_offset_in_group,
          action.data_offset_in_extent,
          action.length,
          action.disk_ids,
          action.md5,
          "");

      sub_cmds.push_back(std::move(read_cmd));
    }


    return make_ready_future<std::vector<hive_read_subcommand>>(std::move(sub_cmds));
}

future<std::vector<volume_revision>> 
stream_reader::load_journal_data(hive_read_subcommand read_subcmd) {
   logger.debug("[{}] start read_subcmd:{}", __func__, read_subcmd);
   auto extent_id = read_subcmd.extent_id;
   auto shard = hive::get_local_journal_service().shard_of(extent_id);

   return hive::get_journal_service().invoke_on(shard, [read_subcmd]
           (auto& journal_service)mutable{
       auto volume_id = read_subcmd.owner_id;
       auto primary_journal = journal_service.get_primary_journal(volume_id); 
       return primary_journal->read_journal(read_subcmd).then([](auto&& revisions){
           return make_ready_future<std::vector<volume_revision>>(std::move(revisions)); 
       });
   });
}

future<> stream_reader::write_trail_log(
    sstring volume_id
    , sstring extent_group_id
    , sstring disk_ids
    , uint64_t length 
    , sstring options
    , access_trail_type trail_type){

    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_volume_context(volume_id).then(
            [this, volume_id, extent_group_id, disk_ids, length, options, trail_type](auto&& volume_context)mutable{
        sstring node_id = volume_context.get_driver_node().get_id();
        if(options.empty()){
            options = volume_context.get_container_name();
        }   
        auto& trail_service = hive::get_local_trail_service();
        auto trail_params = make_shared<access_trail_params>(disk_ids, extent_group_id, node_id, length, options, trail_type);
        auto access_trail = trail_service.get_trail(vega_trail_type::ACCESS_TRAIL);
        return access_trail->trace(trail_params);
    }); 
}


future<bytes> 
stream_reader::load_extent_store_data(hive_read_subcommand read_subcmd){
    if(read_subcmd.extent_group_id == "undefined") {
        bytes empty_buffer(bytes::initialized_later(), read_subcmd.length); 
	      std::memset(empty_buffer.begin(), 0, read_subcmd.length);
        return make_ready_future<bytes>(std::move(empty_buffer));
    }

    auto write_log_func = [this, read_subcmd](){
        //with async, not need wait
        write_trail_log(read_subcmd.owner_id
                      , read_subcmd.extent_group_id
                      , read_subcmd.disk_ids
                      , read_subcmd.length
                      , read_subcmd.options
                      , access_trail_type::READ); 
    };

    auto& extent_store_proxy = hive::get_local_extent_store_proxy();
    return extent_store_proxy.read_extent_group(read_subcmd).then([write_log_func, read_subcmd](auto&& result){
        if(result->data.length() != read_subcmd.length){
            std::ostringstream out;
            out << "[load_extent_store_data] error";
            out << "need length:" << read_subcmd.length << ", but readed data length:" << result->data.length();
            out << ", read_subcmd:" << read_subcmd;
            auto error_info = out.str();
            logger.error(error_info.c_str());
            throw std::runtime_error(error_info);
        }
        write_log_func();
        return make_ready_future<bytes>(std::move(result->data));
    });    
}

void stream_reader::replay_revisions_on_datum(std::vector<volume_revision>& revisions
                                             , bytes& data 
                                             , uint64_t datum_data_offset_in_extent ) {

    auto datum_length = data.length(); 
    auto datum_start  = datum_data_offset_in_extent;
    auto datum_end = datum_start + datum_length;
    auto datum_buf = data.begin();
    logger.debug("[{}] datum_start:{} datum_length:{} datum_end:{}", __func__, datum_start, datum_length, datum_end);
    for(auto& revision : revisions) {
        auto revision_start = revision.offset_in_extent; 
        auto revision_end   = revision_start + revision.length;

        logger.debug("[{}] revision.vclock:{}, revision_start:{} revision_end:{}", __func__, revision.vclock, revision_start, revision_end);
        if(revision_end <= datum_start || revision_start >= datum_end) {
            continue; 
        }

        auto overlap_start = datum_start > revision_start ? datum_start : revision_start;
        auto overlap_end   = datum_end > revision_end ? revision_end : datum_end;
        auto length = overlap_end - overlap_start;
        auto dest_start = 0;
        auto src_start  = 0;

        if(revision_start <= datum_start) {
            dest_start = 0;
            src_start  = datum_start - revision_start;
        }else{
            dest_start = revision_start - datum_start; 
            src_start  = 0;
        }
        auto revision_buf = revision.data.begin();
        std::memcpy(datum_buf+dest_start, revision_buf+src_start, length); 
    }
}



future<bytes> stream_reader::execute_subcommads(
    std::vector<hive_read_subcommand> read_subcmds){
   
    std::vector<future<std::tuple<uint64_t, bytes>>> futures;
    uint64_t order_id = 0;
    for(auto read_subcmd : read_subcmds){
        auto fut = do_read_volume(order_id++, read_subcmd); 
        futures.push_back(std::move(fut));
    }

    return when_all(futures.begin(), futures.end()).then([](auto futs){
        std::vector<std::tuple<uint64_t, bytes>> datas;
        uint64_t total_length = 0;
        for(auto& fut : futs) {
            auto&& data = fut.get0();  
            bytes& content = std::get<1>(data);
            total_length += content.length();
            datas.push_back(std::move(data));
        }

        //splice data in order
        std::sort(datas.begin(), datas.end(),[&](auto& x, auto& y){
            auto x_order_id = std::get<0>(x);
            auto y_order_id = std::get<0>(y);
            return x_order_id < y_order_id;
        });

        bytes buf(bytes::initialized_later(), total_length);
	      std::memset(buf.begin(), 0, total_length);
        uint64_t offset = 0;
        for(auto& data:datas) {
            bytes& content = std::get<1>(data);
            std::memcpy(buf.begin()+offset, content.begin(), content.length());
            offset += content.length();
        }

        assert(offset == total_length);
        return make_ready_future<bytes>(std::move(buf));
    });
}

future<std::tuple<uint64_t, bytes>> 
stream_reader::do_read_volume(uint64_t order_id, hive_read_subcommand read_subcmd){
    logger.debug("[{}] start, read_subcmd:{}", __func__, read_subcmd);  
//// chenjl to do cache for data > 64k
    if(_cache_enabled && read_subcmd.length <= CACHE_SLAB_SIZE){
        return load_cache_data(read_subcmd).then([this, order_id](auto data){
            std::tuple<uint64_t, bytes> data_tuple= std::make_tuple(order_id, std::move(data));
            return std::move(data_tuple); 
        });
    }else{
        return load_data(read_subcmd).then([this, order_id](auto&& data){
            std::tuple<uint64_t, bytes> data_tuple = std::make_tuple(order_id, std::move(data));
            return std::move(data_tuple); 
        });
    }
}

future<bytes> 
stream_reader::load_data(hive_read_subcommand read_subcmd){
    return seastar::async([this, read_subcmd]()mutable{
         //1. load journal data 
        auto&& volume_revisions = this->load_journal_data(read_subcmd).get0();
        
        //2. load extent_group data 
        bytes data = this->load_extent_store_data(read_subcmd).get0();
   
        //3. sort journal revisions by vclock
        std::sort(volume_revisions.begin(), volume_revisions.end(),
            [&](volume_revision& x, volume_revision& y){return x.vclock<y.vclock;});
   
        //4. replay journal data on extent store data 
        auto data_offset_in_extent = read_subcmd.data_offset_in_extent;
        replay_revisions_on_datum(volume_revisions, data, data_offset_in_extent);
        return std::move(data);
    });
}

static std::vector<cache_revision> split_cmd(hive_read_subcommand cmd){
    uint64_t begin_offset= cmd.data_offset_in_extent;
    uint64_t end_offset= cmd.data_offset_in_extent + cmd.length;
    uint64_t down_offset = align_down(begin_offset, CACHE_SLAB_SIZE);
    uint64_t up_offset = align_up(end_offset, CACHE_SLAB_SIZE);
    int count = (up_offset - down_offset) / CACHE_SLAB_SIZE;
    std::vector<cache_revision> revisons;
    revisons.reserve(count);
    uint64_t tmp_begin = down_offset;
    uint64_t tmp_end = down_offset + CACHE_SLAB_SIZE;
    while(tmp_begin < end_offset){
        sstring key = make_cache_key(cmd.extent_id, tmp_begin);
        uint64_t offset_in_cache_datum = tmp_begin < begin_offset?(begin_offset - tmp_begin):0;
        uint64_t offset_in_extent = tmp_begin < begin_offset ? begin_offset:tmp_begin;
        uint64_t length = (tmp_end < end_offset?tmp_end:end_offset) - offset_in_extent;

        cache_revision revision;
        revision.key = key;
        revision.offset_in_cache_datum = offset_in_cache_datum;
        revision.offset_in_extent = offset_in_extent;
        revision.length = length;
        revisons.push_back(std::move(revision));

        tmp_begin += CACHE_SLAB_SIZE;
        tmp_end += CACHE_SLAB_SIZE;
    }
    return std::move(revisons);
}

static bytes merge_cache_data(std::vector<cache_revision> datums, int64_t total_length){
    bytes buf(bytes::initialized_later(), total_length);
    std::memset(buf.begin(), 0, total_length);
    uint64_t offset = 0;
    for(auto data:datums) {
        std::memcpy(buf.begin()+offset, data.content->begin(), data.content->length());
        offset += data.content->length();
    }
    return std::move(buf);
}

//public functions
future<bytes> stream_reader::load_cache_data(hive_read_subcommand cmd){
    return seastar::async([this, cmd]()mutable{
        uint64_t total_length = cmd.length;
        std::vector<cache_revision> revisions = split_cmd(cmd);
        for(auto& revision:revisions){
            cmd.data_offset_in_extent = revision.offset_in_extent;
            cmd.length = revision.length;

            sstring key = revision.key;
            auto src_data_ptr = hive::get_local_stream_cache().get(key).get0();
            datum_origin src_origin = src_data_ptr->origin;
            switch(src_origin){
                case datum_origin::INIT:{
                    throw_exception("get from cache return datum origin:INIT, error key:" + key); 
                    break;
                }
                case datum_origin::POPULATE:{
                    auto&& target_data = this->load_data(cmd).get0();
                    //set sequence number to src_data's sequence num to resolve after write confilict 
                    lw_shared_ptr<datum> target_data_ptr = make_lw_shared<datum>();
                    target_data_ptr->seq = src_data_ptr->seq;
                    target_data_ptr->origin = datum_origin::EXTENT_STORE;
                    target_data_ptr->data = make_lw_shared<bytes>(std::move(target_data));
                    hive::get_local_stream_cache().set(key, target_data_ptr).get0();
                    revision.content =  target_data_ptr->data;
                    break;
                }
                case datum_origin::CACHE:{
                    bytes ret = this->load_data(cmd).get0();
                    revision.content = make_lw_shared<bytes>(std::move(ret));
                    break;
                }
                case datum_origin::EXTENT_STORE:{
                    revision.content =  src_data_ptr->data;
                    break;
                }
                    
            }
        }
        return merge_cache_data(std::move(revisions), total_length);
    });
}

//
//public funtions
//
future<bytes> stream_reader::execute(){
    logger.debug("[{}] {} start, read_plan:{}", __func__, _volume_id, _read_plan);
    utils::latency_counter lc;
    lc.start();
    uint64_t latency;
    return make_ready_future<>().then([](){
    });
    return seastar::async([this]()mutable{
        auto&& sub_cmds = build_read_subcommand().get0();
        auto&& data = execute_subcommads(std::move(sub_cmds)).get0();
        return std::move(data);
    }).then_wrapped([this, lc, latency](auto fut) mutable{
        try{
            auto&& data = fut.get0();
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
            return make_ready_future<bytes>(std::move(data));
        }catch (...) {
            if(lc.is_start()){
                latency = lc.stop().latency_in_nano();
            }
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



