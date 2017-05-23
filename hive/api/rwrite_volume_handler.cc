#include "rwrite_volume_handler.hh"

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
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

#include "hive/trail/trail_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/hive_service.hh"
#include "hive/exceptions/exceptions.hh"

namespace hive{

extern hive::hive_status hive_service_status;

static logging::logger logger("rwrite_volume_handler");

sstring rwrite_volume_handler::get_header_value(header_map& headers, sstring header_key, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
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

hive_write_command rwrite_volume_handler::build_write_command(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring owner_id = get_header_value(new_headers, "x-id");

    sstring offset_t = get_header_value(new_headers, "x-offset");
    uint64_t offset = hive_tools::str_to_uint64(offset_t); 

    sstring length_t = get_header_value(new_headers, "x-length");
    uint64_t length = hive_tools::str_to_uint64(length_t); 


    temporary_buffer<char> req_body = req.move_body();
    assert(req_body.size() == (size_t)req.get_content_length());
    bytes data = bytes(reinterpret_cast<const signed char *>(req_body.get()), req.get_content_length());
    
    hive_write_command write_cmd(
        owner_id
        , offset 
        , length 
        , std::move(data)
    );    
    
    return std::move(write_cmd);
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler::redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:yellow this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/rwrite_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler::rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                             , hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    auto volume_id = write_cmd.owner_id;
    auto offset = write_cmd.offset;
    auto length = write_cmd.length;

    auto& stream_service = hive::get_local_stream_service();
    return stream_service.find_stream(volume_id).then(
        [this, write_cmd=std::move(write_cmd)](auto stream)mutable{
        logger.debug("[rwrite_volume] start, write_cmd:{}", write_cmd);
        return stream->rwrite_volume(std::move(write_cmd)); 
    }).then_wrapped([this, volume_id, offset, length, rep=std::move(rep)](auto f) mutable{ 
        try{
            f.get();
            sstring response  = this->build_return_json();
            rep->set_status(reply::status_type::ok, response);
            rep->done("json");
            logger.debug("[rwrite_by_volume_stream] done, volume_id:{}, offset:{}, length:{}"
                , volume_id, offset, length);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             //tododl:yellow we should return accurate error code
             std::ostringstream out;
             out << "[rwrite_by_volume_stream] error";
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

sstring rwrite_volume_handler::build_return_json(){
    hive::Json json_obj = hive::Json::object {
        {"success", true}
    };
    auto str = json_obj.dump();
    return str;
}

future<std::unique_ptr<reply> > rwrite_volume_handler::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep) {

    if(NORMAL != hive_service_status ){
        rep->set_status(reply::status_type::service_unavailable);
        rep->done();
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }

    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);

    // 1. bind volume_driver
    sstring volume_id = write_cmd.owner_id;
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){
        return shard_volume_service.bind_volume_driver(volume_id);
    }).then([this, rep=std::move(rep), write_cmd=std::move(write_cmd)]
            (auto driver_ex)mutable{ 
        if(driver_ex.need_redirect){
            // 1.1 redirect write
            logger.debug("[handle] redirect to driver node, write_cmd:{}, redirect_ip:{}"
                , write_cmd, driver_ex.redirect_ip);
            return this->redirect_rwrite(std::move(rep), driver_ex.redirect_ip);
        }else{
            // 1.2 write locally 
            // TODO:write_cmd.set_vclock(driver_ex.vclock);
            return this->rwrite_by_volume_stream(std::move(rep), std::move(write_cmd)); 
        }
    });
}//handle


}//namespace hive
