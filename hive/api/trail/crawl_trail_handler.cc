#include "hive/api/log/crawl_extent_group_log_handler.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/volume_service.hh"
#include "hive/hive_tools.hh"

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

static logging::logger logger("crawl_extent_group_log_handler");

//static sstring get_header_value(sstring header_key, header_map& headers){
//    header_map::const_iterator itor = headers.find(header_key);
//    
//    sstring header_value = ""; 
//    if( itor != headers.end() ){
//        header_value = itor->second;   
//    }
//    
//    return header_value;
//}


future<std::unique_ptr<reply> > 
crawl_extent_group_log_handler::handle(const sstring& path
                                     , std::unique_ptr<request> req
                                     , std::unique_ptr<reply> rep) {
    logger.debug("[{}] start", __func__);


    rep->set_status(reply::status_type::ok, "not implement");
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

}//namespace hive
