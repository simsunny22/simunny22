#include "hive/api/trail/write_access_trail_handler.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/volume_service.hh"
#include "hive/hive_tools.hh"
#include "hive/trail/trail_service.hh"
#include "hive/trail/abstract_trail.hh"
#include "hive/trail/access_trail.hh"
#include "hive/system_service/system_service.hh"


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

// ===================================================
// static 
// ===================================================
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



// ===================================================
// public 
// ===================================================
future<std::unique_ptr<reply> > 
write_access_trail_handler::handle(const sstring& path
                       , std::unique_ptr<request> req
                       , std::unique_ptr<reply> rep) {
    logger.debug("[{}] start", __func__);

    trail_data data;
    make_trail_data(req, data);

    auto& trail_service = hive::get_local_trail_service();
    return trail_service.trace(data).then([rep = std::move(rep)]()mutable{
        rep->done();
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
    
}

// ===================================================
// private 
// ===================================================
trail_data& write_access_trail_handler::make_trail_data(std::unique_ptr<request>& req, trail_data& data){
    srand((unsigned)time(0));
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    data["keyspace"] = trail_service::keyspace_name;
    data["table_name"] = trail_service::get_trail_name(vega_trail_type::ACCESS_TRAIL);

    //disk id
    sstring m_disk_id = get_header_value("x-max-disk-id", new_headers);
    sstring r_disk_id = to_sstring(rand() % atoi(m_disk_id.c_str()) + 1); 

    //node 
    sstring m_node_id = get_header_value("x-max-node-id", new_headers);
    sstring r_node_id = to_sstring(rand() % atoi(m_node_id.c_str()) + 1); 

    //timestamp
    auto& system_service = hive::get_local_system_service();
    auto timestamp = system_service.get_system_time_ptr()->get_10_second_time_slot();

    //access_id
    data["access_id"] = r_disk_id +"#" + r_node_id + "#" + timestamp;   


    //extent group id
    sstring m_volume_id = get_header_value("x-max-volume-id", new_headers);
    sstring r_volume_id = to_sstring(rand() % atoi(m_volume_id.c_str()) + 1); 
    sstring m_extent_group_id = get_header_value("x-max-extent-group-id", new_headers);
    sstring r_extent_group_id = to_sstring(rand() % atoi(m_extent_group_id.c_str()) + 1);
    sstring extent_group_id = "volume_" + r_volume_id + "_extent_group_" + r_extent_group_id;
    data["extent_group_id"] = extent_group_id;

    //io pattern
		data["random_read"]    = "0";
		data["random_write"]   = "0";
		data["sequence_read"]  = "0";
		data["sequence_write"] = "0";
    switch (rand() % 4){
    case 0:
        data["random_read"] = "1";
        break;
    case 1:
        data["random_write"] = "1";
        break;
    case 2:
        data["sequence_read"] = "1";
        break;
    case 3:
        data["sequence_write"] = "1";
        break;
    }

    //tier
    switch (rand() % 2){
    case 0:
        data["tier"] = "ssd";
        break;
    case 1:
        data["tier"] = "hhd";
        break;
    }

    //option
    //sstring  s_length = get_header_value("x-length", new_headers);
    //uint64_t i_lenght = atoi(s_lenght.c_str()) 


    temporary_buffer<char> req_body = req->move_body();
    if (!req_body.empty()){
        data["options"] = sstring(req_body.get());
    }

    return data;
}



}//namespace hive
