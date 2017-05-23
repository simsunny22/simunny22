#include "read_volume_handler_test.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/trail/trail_service.hh"


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

namespace hive{

static logging::logger logger("read_volume_handler_test");

sstring read_volume_handler_test::get_header_value(header_map& headers, sstring header_key, sstring default_value){
    hive::header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return header_value;    
        }
    }
 
    if(!default_value.empty()){
        return default_value; 
    }

    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    auto error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);
}

hive_read_command read_volume_handler_test::build_read_cmd(request& req){
    auto config = hive::get_local_hive_service().get_hive_config();
    int volume_count = config->ctx_volume_count(); 
    sstring volume_id = "volume_" + to_sstring(hive_tools::random(volume_count));
    int extent_group_count = config->ctx_extent_group_count_per_volume();
    uint64_t volume_size = extent_group_count * 4*1024*1024; 
    uint64_t offset = (hive_tools::random(volume_size-4096)/512)*512; 


    uint64_t length  = 4*1024;
 
    hive_read_command read_cmd(
        volume_id
        , offset
        , length
    );

    check_read_cmd(read_cmd);
    return std::move(read_cmd);
}

void read_volume_handler_test::check_read_cmd(hive_read_command& read_cmd){
    if(  read_cmd.owner_id.empty() 
      || read_cmd.offset < 0 
      || read_cmd.length < 0)
    {
        std::ostringstream out;
        out << "[check_read_cmd] error, params invalid, read_cmd:" << read_cmd;
        sstring error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info); 
    }
}

future<std::unique_ptr<reply>> 
read_volume_handler_test::redirect_read(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:red this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/read_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>>
read_volume_handler_test::read_by_volume_stream(
    std::unique_ptr<reply> rep
    , hive_read_command read_cmd){

    logger.debug("[{}] start, read_cmd:{}", __func__, read_cmd);
    auto volume_id = read_cmd.owner_id;
    auto offset = read_cmd.offset;
    auto length = read_cmd.length;

    auto& stream_service = hive::get_local_stream_service();
    return stream_service.find_stream(volume_id).then(
            [this, read_cmd=std::move(read_cmd)](auto stream)mutable{
        return stream->read_volume(std::move(read_cmd)); 
    }).then_wrapped([this, volume_id, offset, length, rep=std::move(rep)](auto f) mutable{ 
        try{
            //TODO:yellow need some method to avoid data copy
            bytes data = f.get0();
            rep->_content.append(reinterpret_cast<const char*>(data.c_str()), data.size());
            rep->set_status(reply::status_type::ok);
            rep->done();
            logger.debug("[read_by_volume_stream] done, volume_id:{}, offset:{}, length:{}"
                , volume_id, offset, length);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             //tododl:yellow we should return accurate error code
             std::ostringstream out;
             out << "[read_by_volume_stream] error";
             out << ", volume_id:" << volume_id;
             out << ", offset:" << offset;
             out << ", length:" << length;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
    });
}

future<std::unique_ptr<reply> > read_volume_handler_test::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep){

    auto read_cmd = build_read_cmd(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, read_cmd);
   
    // 1. bind volume driver
    sstring volume_id = read_cmd.owner_id; 
    logger.debug("[{}] bind volume driver start, volume_id:{}", __func__, volume_id);
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){
        return shard_volume_service.bind_volume_driver(volume_id);
    }).then([this, volume_id,  read_cmd=std::move(read_cmd)
            , rep=std::move(rep)](auto driver_info) mutable{
        if(driver_info.need_redirect){
            // 1.1 redirect read
            return this->redirect_read(std::move(rep), driver_info.redirect_ip);            
        }else{
            // 1.2 do read
            return this->read_by_volume_stream(std::move(rep), std::move(read_cmd)); 
        }
    });
}

}//namespace hive
