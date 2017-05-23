#include "read_object_handler.hh"

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

#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/object/object_service.hh"
#include "hive/hive_tools.hh"


namespace hive{
static logging::logger logger("read_object_handler");

sstring read_object_handler::get_header_value(header_map& headers, sstring header_key, sstring default_value){
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

hive_read_command read_object_handler::build_read_command(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring owner_id = get_header_value(new_headers, "x-id");

    sstring offset_t = get_header_value(new_headers, "x-offset");
    uint64_t offset = hive_tools::str_to_uint64(offset_t); 

    sstring length_t = get_header_value(new_headers, "x-length");
    uint64_t length = hive_tools::str_to_uint64(length_t); 

    hive_read_command read_cmd(
        owner_id
        , offset
        , length
    );
    return std::move(read_cmd);
}

future<std::unique_ptr<reply> > read_object_handler::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep){
    auto read_cmd = build_read_command(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, read_cmd);

    auto object_id = read_cmd.owner_id; 
    auto offset = read_cmd.offset;
    auto length = read_cmd.length;
    auto shard = hive::get_local_object_service().shard_of(object_id);
    return hive::get_object_service().invoke_on(shard, [this, read_cmd] (auto& shard_object_service){
        return shard_object_service.bind_object_driver().then([&shard_object_service](auto driver_ptr){
            return shard_object_service.bind_object_stream();
        }).then([this, read_cmd](auto object_stream_ptr){
            return object_stream_ptr->read_object(read_cmd);
        }); 
    }).then_wrapped([this, object_id, offset, length, rep=std::move(rep)](auto f) mutable {
        try{
            bytes data = f.get0();
            rep->_content.append(reinterpret_cast<const char*>(data.c_str()), data.size());
            rep->set_status(reply::status_type::ok);
            rep->done();
            rep->done();
            logger.debug("[handle] done, object_id:{}, offset:{}, length:{}", object_id, offset, length); 
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        } catch(...) {
             std::ostringstream out;
             out << "[handle] error";
             out << ", object_id:" << object_id << ", offset:" << offset << ", length:" << length;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
    });
}

}//namespace hive
