#include "get_context_handler.hh"

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

logging::logger log_get_context_handler("get_context_handler");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

future<std::unique_ptr<reply> > get_context_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    log_get_context_handler.debug("get_context_handler, start");
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring context_key = get_header_value("x-key", new_headers);
    log_get_context_handler.debug("get_context_handler, context_key:{}", context_key);

    return seastar::async([rep=std::move(rep), context_key]()mutable {
        auto& context_service = hive::get_local_context_service();
        sstring context_value = context_service.get_context_value(context_key);
        sstring response = "{\"context_value\":" + context_value + "}";   
        log_get_context_handler.debug("get_context_handler, response:{}", response);
        rep->_content = response;
        rep->done("json");
        return std::unique_ptr<reply>(std::move(rep));
    });
}


}//namespace hive
