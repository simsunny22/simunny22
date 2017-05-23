#include "rwrite_volume_handler_test.hh"
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

#include "hive/hive_service.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"
#include "hive/stream/stream_plan.hh"
#include "hive/stream/stream_state.hh"
//#include "gms/inet_address.hh"

namespace hive{

static logging::logger logger("rwirte_volume_handler_test");

extern hive::hive_status hive_service_status;
const int LEN = 512*1024;
sstring got_random(int range){
    return to_sstring(rand() % range + 1);
}

char* make_random_body(int length){
    char range[62]; char c; int i =0; 
    for(c = 'a'; c<='z'; c++ ){
        range[i] = c;
        i++;
    }   

    for(c = 'A'; c <= 'Z'; c++){
        range[i] = c;
        i++;
    }   

    for(c = '0'; c<='9'; c++){
        range[i]= c;
        i++;
    }   

    char* body = new char[length - 1]; 
    for(int i =0; i < length; i ++){
        int num = rand() % 62; 
        body[i] = range[num];
    }   
    body[length] = '\0';
    return body;
}

bytes global_test_body = bytes(reinterpret_cast<const signed char *>(make_random_body(LEN)), LEN);


static sstring get_header_value(sstring header_key, header_map& headers, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = default_value; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

void rwrite_volume_handler_test::check_write_command(hive_write_command& write_cmd){
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

hive_write_command rwrite_volume_handler_test::build_write_command(request& req){
    header_map& headers = req._headers;
    sstring volume_num_str = get_header_value("volume_num", headers, "1");
    int64_t volume_num = hive_tools::str_to_int64(volume_num_str); 
    sstring extent_group_num_str = get_header_value("extent_group_num", headers, "10");
    int64_t extent_group_num = hive_tools::str_to_int64(extent_group_num_str); 
    sstring disk_ids = get_header_value("disk_id", headers, "1");

    sstring volume_id = "volume_" + got_random(volume_num);
    sstring extent_group_id = volume_id + "_extent_group_" + got_random(extent_group_num);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group  = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent   = 0; //tododl:yellow why only 0??
    int64_t length   = LEN; 

    hive_write_command write_cmd(
        volume_id
        , extent_group_id
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , global_test_body 
        , "default options"
    );    
    
    check_write_command(write_cmd);

    return std::move(write_cmd);
}

future<std::unique_ptr<reply> > rwrite_volume_handler_test::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    int64_t start_timestamp = hive_tools::get_current_time();
    srand((unsigned)start_timestamp); // set seeds

    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);

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
rwrite_volume_handler_test::rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                             , hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    uint64_t start_timestamp = hive_tools::get_current_time();
    sstring volume_id        = write_cmd.owner_id; 
    sstring extent_group_id  = write_cmd.extent_group_id; 
    sstring disk_ids         = write_cmd.disk_ids; 
    uint64_t length          = write_cmd.length; 
    uint64_t vclock          = write_cmd.vclock; 

    auto shard = hive::get_local_token_service().shard_of(extent_group_id);
    auto throttle_flag = make_lw_shared<bool>(false);
    return hive::get_token_service().invoke_on(shard, [volume_id, length, throttle_flag]
            (auto& token_service){
        // 1. get token
        return token_service.throttle_memory(volume_id, length).then([throttle_flag](auto flag){
            *throttle_flag = flag;
        }); 
    }).then([write_cmd=std::move(write_cmd)]()mutable{
        // 3. do write
        auto shard = hive::get_local_stream_service().shard_of(write_cmd.extent_group_id);
        return hive::get_stream_service().invoke_on(shard, [write_cmd=std::move(write_cmd)]
                (auto& stream_service)mutable{
            auto volume_id = write_cmd.owner_id;
            return stream_service.find_or_create_stream(volume_id).then(
                    [write_cmd=std::move(write_cmd)](auto stream) mutable{
                return stream->rwrite_volume(std::move(write_cmd));
            });
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

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test::redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:red this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/rwrite_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

sstring rwrite_volume_handler_test::build_return_json(int64_t vclock){
    sstring str_vclock = to_sstring(vclock);
    sstring build_str = "{\"vclock\":" + str_vclock + "}";   
    //std::string err;
    //auto json_tmp = hive::Json::parse(build_str, err);
    //auto json_str = json_tmp.dump();
    //return json_str;
    return build_str;
}

}//namespace hive

