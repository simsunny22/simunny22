#include "rwrite_volume_handler_test_need_body.hh"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/lexical_cast.hpp>

#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

#include "hive/trail/trail_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/hive_service.hh"

namespace hive{

static sstring got_random2(int range){
    return to_sstring(rand() % range + 1);
}

static sstring get_extent_group_id(sstring volume_id, int64_t extent_group_num){
    uint64_t r_extent_group_num = rand() % extent_group_num + 1;
    uint64_t dir_num = r_extent_group_num % 10;
    sstring sub_dir_name = sprint("%s%s", dir_num, dir_num); 
    return volume_id + "_extent_group_" + to_sstring(r_extent_group_num) + "_" + sub_dir_name ;
}



static logging::logger logger("rwrite_volume_handler_test_need_body");

static sstring get_header_value(sstring header_key, header_map& headers, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = default_value; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

void rwrite_volume_handler_test_need_body::check_write_command(hive_write_command& write_cmd){
    if(  write_cmd.length != write_cmd.data.size() 
      || write_cmd.data_offset_in_extent + write_cmd.length > hive_config::extent_size)
    {
        std::ostringstream out;
        out << "[check_write_command] error params invalid, write_cmd:" << write_cmd;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

bool rwrite_volume_handler_test_need_body::check_is_access_trail_test(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring is_access_trail_test = get_header_value("x-log-test", new_headers, "false");
    if("true" == is_access_trail_test){
        return true;
    }
    return false;
}

hive_access_trail_command rwrite_volume_handler_test_need_body::build_access_trail_command(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring trail_disk_id = get_header_value("x-trail-disk-id", new_headers,  "1"); 
    sstring trail_node_id = get_header_value("x-trail-node-id", new_headers, "1"); 
    hive_access_trail_command cmd(trail_disk_id, trail_node_id);
    return std::move(cmd);
}

hive_write_command rwrite_volume_handler_test_need_body::build_write_command(request& req){
    header_map& headers = req._headers;
    sstring volume_num_str = get_header_value("volume_num", headers, "10");
    int64_t volume_num = hive_tools::str_to_int64(volume_num_str); 
    sstring extent_group_num_str = get_header_value("extent_group_num", headers, "10");
    int64_t extent_group_num = hive_tools::str_to_int64(extent_group_num_str); 
    sstring disk_ids = get_header_value("disk_id", headers, "1");

    sstring volume_id = "volume_" + got_random2(volume_num);
    sstring extent_group_id = get_extent_group_id(volume_id, extent_group_num);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent = (rand()%2000)*512;
    int64_t length = req.get_content_length();
    temporary_buffer<char> req_body = req.move_body();
    assert(req_body.size() == (size_t)req.get_content_length());
    bytes data = bytes(reinterpret_cast<const signed char *>(req_body.get()), req.get_content_length());

    hive_write_command write_cmd(
        volume_id
        , extent_group_id
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , std::move(data)
        , "default options" 
    );    
    
    check_write_command(write_cmd);
    return std::move(write_cmd);
}

future<std::unique_ptr<reply> > rwrite_volume_handler_test_need_body::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    int64_t start_timestamp = hive_tools::get_current_time();
    srand((unsigned)start_timestamp); // set seeds

    //bool pseudo_write_enable = hive::get_local_hive_service().get_hive_config()->pseudo_write_enable();
    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);

    if(check_is_access_trail_test(*req)){
        //for test access trail
        auto access_trail_cmd =  build_access_trail_command(*req);
        return this->pseudo_rwrite_by_volume_stream(std::move(rep), std::move(write_cmd), std::move(access_trail_cmd));
    }

    // 1. bind volume_driver
    sstring volume_id = write_cmd.owner_id;
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id]
            (auto& shard_volume_service){
        return shard_volume_service.bind_volume_driver(volume_id);
    }).then([this, volume_id, rep=std::move(rep), write_cmd=std::move(write_cmd)]
            (auto driver_ex)mutable{ 
        if(driver_ex.need_redirect){
            // 1.1 redirect write
            logger.debug("[handle] redirect to driver node, volume_id:{}, redirect_ip:{}"
                , volume_id, driver_ex.redirect_ip);
            return this->redirect_rwrite(std::move(rep), driver_ex.redirect_ip);
        }else{
            // 1.2 write locally 
            write_cmd.set_vclock(driver_ex.vclock);
            return this->rwrite_by_volume_stream(std::move(rep), std::move(write_cmd)); 
        }
    });
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test_need_body::pseudo_rwrite_by_volume_stream(std::unique_ptr<reply> rep 
        , hive_write_command write_cmd
        , hive_access_trail_command access_trail_cmd){
    sstring  extent_group_id = write_cmd.extent_group_id;
    uint64_t length          = write_cmd.length;
    uint64_t vclock          = write_cmd.vclock; 
    sstring  options         = write_cmd.options;

    sstring  pseudo_disk_id  = access_trail_cmd.disk_id;
    sstring  pseudo_node_id  = access_trail_cmd.node_id;
    this->trace_access_trail(pseudo_disk_id, extent_group_id, pseudo_node_id, length, options);

    sstring response  = this->build_return_json(vclock);
    rep->set_status(reply::status_type::ok, response);
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test_need_body::rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                             , hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    uint64_t start_timestamp = hive_tools::get_current_time();
    sstring volume_id        = write_cmd.owner_id; 
    sstring extent_group_id  = write_cmd.extent_group_id; 
    sstring disk_ids         = write_cmd.disk_ids; 
    uint64_t length          = write_cmd.length; 
    uint64_t vclock          = write_cmd.vclock; 
    sstring  options         = "default option";

    auto shard = hive::get_local_token_service().shard_of(extent_group_id);
    auto throttle_flag = make_lw_shared<bool>(false);
    
    return hive::get_token_service().invoke_on(shard, [volume_id, length, throttle_flag]
            (auto& token_service){
        // 1. get token
        auto disable_memtable = get_local_hive_service().get_hive_config()->disable_memtable();
        if(disable_memtable){
            return make_ready_future<>();
        }
        return token_service.throttle_memory(volume_id, length).then([throttle_flag](auto flag){
            *throttle_flag = flag;
        }); 
    }).then([this, write_cmd=std::move(write_cmd)]()mutable{
        // 2. do write
        auto shard = hive::get_local_stream_service().shard_of(write_cmd.extent_group_id);
        return hive::get_stream_service().invoke_on(shard, [this, write_cmd=std::move(write_cmd)]
                (auto& stream_service)mutable{
            auto volume_id = write_cmd.owner_id;
            return stream_service.find_or_create_stream(volume_id).then(
                    [this, write_cmd=std::move(write_cmd)](auto stream) mutable{
                return stream->rwrite_volume(std::move(write_cmd));
            });
        }); 
    }).then([this, volume_id, disk_ids, extent_group_id, length, options](){
        //4. do write log
        auto& context_service = hive::get_local_context_service();
        return context_service.get_or_pull_volume_context(volume_id).then([
                this, disk_ids, extent_group_id, length, options](auto volume_context){
            sstring node_id =  volume_context.get_driver_node().get_id();
            this->trace_access_trail(disk_ids, extent_group_id, node_id, length, options);
        });
    }).then_wrapped([this, volume_id, extent_group_id, rep=std::move(rep), vclock, start_timestamp, length, throttle_flag]
            (auto f) mutable{ 
        try{
            f.get();
            sstring response  = this->build_return_json(vclock);
            rep->set_status(reply::status_type::ok, response);
            rep->done();
            logger.debug("[rwrite_by_volume_stream] done, rwrite latency:{} usec, volume_id:{}, extent_group_id:{}"
                , hive_tools::get_current_time()-start_timestamp, volume_id, extent_group_id); 
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             if(*throttle_flag == true){
                 auto shard = hive::get_local_token_service().shard_of(extent_group_id);
                 hive::get_token_service().invoke_on(shard, [volume_id, length](auto& token_service){
                     token_service.unthrottle_memory(volume_id, length);
                 });
             }
             std::ostringstream out;
             out << "[rwrite_by_volume_stream] error";
             out << ", volume_id:" << volume_id << ", extent_group_id:" << extent_group_id;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
    });
}

future<> 
rwrite_volume_handler_test_need_body::trace_access_trail(sstring disk_ids, sstring extent_group_id, sstring node_id, uint64_t size, sstring options){
    auto& trail_service = hive::get_local_trail_service();
    auto trail_params = make_shared<access_trail_params>(disk_ids, extent_group_id, node_id, size, options, access_trail_type::WRITE);
    auto access_trail = trail_service.get_trail(vega_trail_type::ACCESS_TRAIL);
    access_trail->trace(trail_params);
    return make_ready_future<>();
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test_need_body::redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:red this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/rwrite_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

sstring rwrite_volume_handler_test_need_body::build_return_json(int64_t vclock){
    sstring str_vclock = to_sstring(vclock);
    sstring build_str = "{\"vclock\":" + str_vclock + "}";   
    return build_str;
}

}//namespace hive


