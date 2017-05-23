#include "write_object_handler_test.hh"
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

#include "hive/hive_service.hh"
#include "hive/object/object_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"
#include "hive/stream/stream_plan.hh"
#include "hive/stream/stream_state.hh"
//#include "gms/inet_address.hh"

namespace hive{

static logging::logger logger("rwirte_object_handler_test");

extern hive::hive_status hive_service_status;
extern sstring got_random(int range);
extern char* make_random_body(int length);
const int LEN = 512*1024;
bytes write_object_test_body = bytes(reinterpret_cast<const signed char *>(make_random_body(LEN)), LEN);

static sstring get_header_value(sstring header_key, header_map& headers, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = default_value; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

void write_object_handler_test::check_write_command(hive_write_command& write_cmd){
    if(  write_cmd.length != write_cmd.data.size() 
      || write_cmd.data_offset_in_extent + write_cmd.length > hive_config::extent_size)
    {
        std::ostringstream out;
        out << "[check_write_command] error params invalid, write_cmd:" << write_cmd;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

hive_write_command write_object_handler_test::build_write_command(request& req){
    header_map& headers = req._headers;
    sstring object_num_str = get_header_value("object_num", headers, "1");
    int64_t object_num = hive_tools::str_to_int64(object_num_str); 
    sstring extent_group_num_str = get_header_value("extent_group_num", headers, "10");
    int64_t extent_group_num = hive_tools::str_to_int64(extent_group_num_str); 
    sstring disk_ids = get_header_value("disk_id", headers, "1");

    sstring object_id = "volume_" + got_random(object_num);
    sstring extent_group_id = object_id + "_extent_group_" + got_random(extent_group_num);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group  = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent   = 0; //tododl:yellow why only 0??
    int64_t length   = LEN; 

    hive_write_command write_cmd(
        object_id
        , extent_group_id
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , write_object_test_body 
        , "default options"
    );    
    
    check_write_command(write_cmd);

    return std::move(write_cmd);
}

future<std::unique_ptr<reply>> write_object_handler_test::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);

    // 1. bind object_driver
    sstring object_id = write_cmd.owner_id;
    sstring disk_ids = write_cmd.disk_ids;
    sstring extent_group_id = write_cmd.extent_group_id;
    auto shard = hive::get_local_object_service().shard_of(object_id);
    return hive::get_object_service().invoke_on(shard, [disk_ids](auto& shard_object_service){
        return shard_object_service.bind_object_driver(disk_ids
            ).then([&shard_object_service](){
                return shard_object_service.bind_object_stream();
        });
    }).then([this, write_cmd=std::move(write_cmd)](auto object_stream_ptr){
        return object_stream_ptr->write_object(std::move(write_cmd));
    }).then_wrapped([object_id, extent_group_id, rep= std::move(rep)](auto f) mutable{
        try{
            f.get();
            //sstring reposne = build_return_json(); 
            sstring response  = "write_object_success";
            rep->set_status(reply::status_type::ok, response);
            rep->done();
            logger.debug("[write_by_object_stream] done object_id:{}, extent_group_id:{}"
                ,object_id, extent_group_id); 
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             std::ostringstream out;
             out << "[write_by_object_stream] error";
             out << ", object_id:" << object_id << ", extent_group_id:" << extent_group_id;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }

    });
}//handle

sstring write_object_handler_test::build_return_json(int64_t vclock){
    sstring str_vclock = to_sstring(vclock);
    sstring build_str = "{\"vclock\":" + str_vclock + "}";   
    //std::string err;
    //auto json_tmp = hive::Json::parse(build_str, err);
    //auto json_str = json_tmp.dump();
    //return json_str;
    return build_str;
}

}//namespace hive


