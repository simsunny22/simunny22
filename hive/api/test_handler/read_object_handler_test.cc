#include "read_object_handler_test.hh"

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
#include "hive/object/object_service.hh"
#include "hive/hive_tools.hh"


namespace hive{

static logging::logger logger("read_object_handler_test");
const int LEN = 4*1024;
extern sstring got_random(int range);

void read_object_handler_test::check_read_cmd(lw_shared_ptr<hive_read_command> read_cmd){
    if(  read_cmd->owner_id.empty() 
      || read_cmd->extent_group_id.empty() 
      || read_cmd->extent_id.empty()
      || read_cmd->extent_offset_in_group < 0
      || read_cmd->data_offset_in_extent < 0 
      || read_cmd->length < 0
      || read_cmd->data_offset_in_extent + read_cmd->length > hive_config::extent_size)
    {
        std::ostringstream out;
        out << "[check_read_cmd] error, params invalid, read_cmd:" << *read_cmd;
        sstring error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info); 
    }
}

lw_shared_ptr<hive_read_command> 
read_object_handler_test::build_read_cmd(request& req){
    sstring object_id = "volume_1";
    sstring extent_group_id = object_id + "_extent_group_" + got_random(50);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent = (rand()%2000)*512;
    int64_t length   = LEN; 
    sstring disk_ids = "1";
    sstring md5 = "";
    
    auto read_cmd = make_lw_shared<hive_read_command>(
          object_id
        , extent_group_id
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , md5
        , "default options"
    );
    check_read_cmd(read_cmd);
    return std::move(read_cmd);
}

future<std::unique_ptr<reply> > read_object_handler_test::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    auto read_cmd = build_read_cmd(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, *read_cmd);

    // 1. bind object driver
    sstring object_id = read_cmd->owner_id; 
    sstring disk_ids = read_cmd->disk_ids; 
    logger.debug("[{}] bind object driver start, object_id:{}", __func__, object_id);
    auto shard = hive::get_local_object_service().shard_of(object_id);
    return hive::get_object_service().invoke_on(shard, [disk_ids] (object_service& object_service){
        return object_service.bind_object_driver(disk_ids).then([&object_service](){
            return object_service.bind_object_stream();
        });
    }).then([this, read_cmd=std::move(read_cmd)](auto stream){
        return stream->read_object(std::move(read_cmd)); 
    }).then_wrapped([this, object_id, rep=std::move(rep)](auto f) mutable {
        try{
            auto result = f.get0();
            //sstring response  = "{\"message\":\"write object success\"}";
            auto datum = extent_datum::from_raw_result(*result);
            rep->set_status(reply::status_type::ok, sstring(datum.data.begin(), datum.data.end()));
            rep->done();
            logger.debug("[rwrite_by_object_stream] done, object_id:{}", object_id); 
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        } catch(...) {
             std::ostringstream out;
             out << "[rwrite_by_object_stream] error";
             out << ", object_id:" << object_id ;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
    });
}
}//namespace hive
