#include "hive/api/log/read_log_handler.hh"
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

static logging::logger logger("read_log_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return header_value;    
        }
    }
   
    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    auto error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);
}

future<std::unique_ptr<reply> > 
read_log_handler::handle(const sstring& path
                       , std::unique_ptr<request> req
                       , std::unique_ptr<reply> rep) {
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring log_name = get_header_value("x-log-name", new_headers);
    sstring pk = get_header_value("x-pk", new_headers);
    sstring ck = get_header_value("x-ck", new_headers);

    logger.debug("[{}] start, log_name:{}, Pk:{}, Ck:{}", __func__, log_name, pk, ck);


    rep->set_status(reply::status_type::ok, "not implement");
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

}//namespace hive
