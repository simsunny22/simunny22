#include "message_service_test_handler.hh"
#include "hive/messaging_service.hh"
#include "message/messaging_service.hh"
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
#include "utils/latency.hh"

#include "hive/http/json11.hh"
#include "hive/hive_tools.hh"
#include "hive/stream_service.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/test_message.hh"
#include "gms/inet_address.hh"

//using clock_type = std::chrono::steady_clock; 
namespace hive{

#if 0
const int LEN = 1024*1024;

static uint32_t got_random(int range){
    return rand() % range + 1;
}


static char* make_random_body(int length){
    char range[62]; char c; int i =0; 
    for(c = 'a'; c<='z'; c++ ){
        range[i] = c;
        i++;
    }   

    for(c = 'A'; c <= 'Z'; c++){
        range[i] = c;
        i++;
    }   

    for(c = '0'; c<='9'; c++){
        range[i]= c;
        i++;
    }   

    char* body = new char[length - 1]; 
    for(int i =0; i < length; i ++){
        int num = rand() % 62; 
        body[i] = range[num];
    }   
    body[length] = '\0';
    return body;
}

static bytes global_test_body = bytes(reinterpret_cast<const signed char *>(make_random_body(LEN)), LEN);

static logging::logger logger("message_service_test_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}
#endif

future<std::unique_ptr<reply> > message_service_test_handler::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {

throw std::runtime_error("abandoned");

#if 0
    logger.debug("message_service_test_handle start! ");
    int64_t start_timestamp = hive_tools::get_current_time();
    srand((unsigned)start_timestamp); // set seeds

    uint32_t shard = got_random(smp::count); 
    test_message message(global_test_body);

    header_map& headers = req->_headers;
    sstring to_ip = get_header_value("X-To-Address", headers);
    if(to_ip.size() == 0){
        to_ip = "10.100.123.149";
    }
    gms::inet_address from("10.100.123.117");
    gms::inet_address to(to_ip);

    logger.debug("message_service_test_handle 1! ");

    migrate_params_entry migrate_params("default_driver_node_ip"
                                      , "default_container_name"
                                      , "default_intent_id"
                                      , "volume_1"
                                      , "volume_1_extent_group_1"
                                      , "1"
                                      , "2"
                                      , 0
                                      , 0);
    auto timeout = clock_type::now() + std::chrono::milliseconds(get_local_stream_service().get_config().write_request_timeout_in_ms());
    // return hive::get_local_messaging_service().send_test_message(hive::messaging_service::msg_addr{to, shard}, timeout, std::move(message))
    //.then_wrapped([this, rep = std::move(rep)](auto f) mutable {
     return hive::get_local_messaging_service().migrate_extent_group(hive::messaging_service::msg_addr{to, shard}, timeout
             , migrate_params 
             , from, "1", engine().cpu_id(), 123).then_wrapped([this, rep = std::move(rep)](auto f) mutable {
     //return net::get_local_messaging_service().migrate_extent_group_done(net::messaging_service::msg_addr{to, shard}, 1,  "1" 
     //               , 123)
     //               .then_wrapped([this, rep = std::move(rep)](auto f) mutable {
        try{
             logger.debug("message_service_test_handle 2! ");
             f.get();
             sstring response = "{\"ok\":\"success\"}";   
             rep->_content = response;
             rep->done("success");
             
             logger.debug("message_service_test_handle done! ");
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             logger.error("message_service_test_handle error! ");
             throw std::runtime_error("message_service_test_handle failed!");
        }
     });
    //});
#endif

}

}//namespace hive


