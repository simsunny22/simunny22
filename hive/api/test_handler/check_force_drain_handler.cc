#include "check_force_drain_handler.hh"

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

#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "hive/journal/journal_service.hh"




namespace hive{

logging::logger check_force_drain_logger("check_force_drain_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    sstring header_value = "";
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    return header_value;
}


//params data
lw_shared_ptr<request_params> check_force_drain_handler::parse_write_params(request& req){
    header_map& headers = req._headers;
    sstring volume_id = get_header_value("X-Volume-Id", headers);

    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("volume_id", volume_id));
    return std::move(params);
}

//swrite data
future<std::unique_ptr<reply>> check_force_drain_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    lw_shared_ptr<request_params> params =  parse_write_params(*req);
    sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);
    
    
    return hive::get_journal_service().map_reduce0(
        [volume_id](auto& journal_service) { 
            if(volume_id.empty())
              return journal_service.check_force_drain_all();
            return journal_service.check_force_drain(volume_id);
        },
        0,
        std::plus<int>()
    ).then([volume_id, rep=std::move(rep)](int num)mutable{
        sstring content = "volume:"  +volume_id + " has " + to_sstring(num) + " memtable not drain \n";
        rep->_content = content;
        rep->done("done");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });

}

//future<std::unique_ptr<reply>> check_force_drain_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
//    lw_shared_ptr<request_params> params =  parse_write_params(*req);
//    sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);
//    
//    return hive::get_journal_service().map_reduce0(
//        [volume_id](auto& journal_service) { 
//            return journal_service.check_force_drain(volume_id);
//        },
//        0,
//        std::plus<int>()
//    ).then([volume_id, rep=std::move(rep)](int num)mutable{
//        sstring content = "volume:"  +volume_id + " has " + to_sstring(num) + " memtable not drain \n";
//        rep->_content = content;
//        rep->done("done");
//        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
//    });
//  
//}


}//namespace hive
