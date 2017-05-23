#include "hive/api/log/create_log_table_handler.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/volume_service.hh"
#include "hive/hive_tools.hh"
#include "hive/log/log_service.hh"
#include "hive/log/abstract_log.hh"
#include "hive/log/access_map_log.hh"


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

static logging::logger logger("create_log_table_handler");

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
create_log_table_handler::handle(const sstring& path
                       , std::unique_ptr<request> req
                       , std::unique_ptr<reply> rep) {
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring table_name = get_header_value("x-table-name", new_headers);

    logger.debug("[{}] start, table_name:{}", __func__, table_name);

    auto log_type = log_service::get_log_type(table_name);
    
    auto& log_service = hive::get_local_log_service();
    auto log_entry = log_service.get_log(log_type);
    return log_entry->create_log_table().then([rep=std::move(rep)]()mutable{
        rep->set_status(reply::status_type::ok, "success");
        rep->done();
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}

}//namespace hive
