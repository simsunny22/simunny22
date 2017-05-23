#include "remove_context_handler.hh"

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
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/volume_stream.hh"
#include "hive/journal/journal_service.hh"
#include "hive/context/context_service.hh"


namespace hive{

static logging::logger logger("remove_context_handler");
//static sstring get_header_value(sstring header_key, header_map& headers){
//    header_map::const_iterator itor = headers.find(header_key);
//    sstring header_value = ""; 
//    if( itor != headers.end() ){
//        header_value = itor->second;   
//    }
//    return header_value;
//}

future<std::unique_ptr<reply> > remove_context_handler::handle(const sstring& path
                                                                    , std::unique_ptr<request> req
                                                                    , std::unique_ptr<reply> rep){
    logger.debug("[{}] start", __func__);

    return seastar::async([rep=std::move(rep)]()mutable {
        auto& context_service = hive::get_local_context_service();
        context_service.remove_all_on_every_shard().get();
        rep->_content = "success";
        rep->done("text");
        return std::unique_ptr<reply>(std::move(rep));
    });
}

}//namespace hive
