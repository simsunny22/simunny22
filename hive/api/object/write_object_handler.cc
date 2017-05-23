#include "write_object_handler.hh"

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
#include "hive/object/object_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive{

static logging::logger logger("write_object_handler");
sstring write_object_handler::get_header_value(header_map& headers, sstring header_key, sstring default_value){
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

hive_write_command write_object_handler::build_write_command(request& req){
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
   
    check_write_command(write_cmd);
    return std::move(write_cmd);
}

void write_object_handler::check_write_command(hive_write_command& write_cmd){
    if(write_cmd.length != write_cmd.data.size()) {
        std::ostringstream out;
        out << "[check_write_command] error params invalid, write_cmd:" << write_cmd;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

sstring write_object_handler::build_return_json(){
    hive::Json json_obj = hive::Json::object {
        {"success", true}
    };
    auto str = json_obj.dump();
    return str;
}

future<std::unique_ptr<reply> > write_object_handler::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep) {

    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
     
    // 1. bind object_driver
    auto object_id = write_cmd.owner_id;
    auto offset = write_cmd.offset;
    auto length = write_cmd.length;
    auto shard = hive::get_local_object_service().shard_of(object_id);
    return hive::get_object_service().invoke_on(shard, [this, write_cmd=std::move(write_cmd)]
            (auto& shard_object_service){
        return shard_object_service.bind_object_driver().then([&shard_object_service](auto driver_ptr){
            return shard_object_service.bind_object_stream();
        }).then([this, write_cmd=std::move(write_cmd)](auto object_stream_ptr){
            return object_stream_ptr->write_object(std::move(write_cmd));
        });
    }).then_wrapped([this, object_id, offset, length, rep=std::move(rep)](auto f) mutable{
        try{
            auto size = f.get0();
            rep->set_status(reply::status_type::ok, to_sstring(size));
            rep->done();
            logger.debug("[handle] done object_id:{}, offset:{}, length:{}", object_id, offset, length); 
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             std::ostringstream out;
             out << "[handle] error";
             out << ", object_id:" << object_id << ", offset:" << offset << ", length:" << length;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }

    });
}//handle

}//namespace hive
