#include "inject_volume_context_handler.hh"

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
#include "hive/hive_tools.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/volume_stream.hh"
#include "hive/journal/journal_service.hh"


namespace hive{

logging::logger log_inject("inject_volume_context_handler");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

future<std::unique_ptr<reply> > inject_volume_context_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
log_inject.debug("inject_volume_context_handler, start");
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring context_ttl_t = get_header_value("x-context-ttl", new_headers);
    int32_t context_ttl   = hive_tools::str_to_int32(context_ttl_t); 
    sstring volume_context_json(req->_body.get(), req->get_content_length()); 

log_inject.debug("inject_volume_context_handler, ttl:{},  body:{}", context_ttl, volume_context_json);

    return seastar::async([json = std::move(volume_context_json), rep=std::move(rep), context_ttl]()mutable {
        auto& context_service = hive::get_local_context_service();
        context_service.save_volume_context(json).get();
        rep->_content = "inject volume context success";
        rep->done("success");
        return std::unique_ptr<reply>(std::move(rep));
    });
}

}//namespace hive
