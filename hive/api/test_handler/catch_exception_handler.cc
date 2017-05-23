#include "catch_exception_handler.hh"

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
#include "hive/hive_tools.hh"
#include "hive/exceptions/exceptions.hh"


namespace hive{

static logging::logger logger("read_volume_handler_test");
const int LEN = 4*1024;
extern sstring got_random(int range);

lw_shared_ptr<hive_read_command> 
catch_exception_handler::build_read_cmd(request& req){
    sstring volume_id = "volume_1";
    sstring extent_group_id = volume_id + "_extent_group_" + got_random(50);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent = (rand()%2000)*512;
    int64_t length   = LEN; 
    sstring disk_ids = "2";
    sstring md5 = "";
    
    auto read_cmd = make_lw_shared<hive_read_command>(
          volume_id
        , extent_group_id
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , md5
        , "default options"
    );
    return std::move(read_cmd);
}

future<std::unique_ptr<reply> > catch_exception_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    int64_t start_timestamp = hive_tools::get_current_time();
    auto read_cmd = build_read_cmd(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, *read_cmd);

    auto volume_id = read_cmd->owner_id;
    //bind volume driver
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (volume_service& volume_service){
        return volume_service.bind_volume_driver(volume_id);
    }).then([](auto driver_info){
        //to check token 
        return make_ready_future<>();
    }).then([this, volume_id, start_timestamp, read_cmd=std::move(read_cmd), rep=std::move(rep)]() mutable{
        lw_shared_ptr<volume_reader> reader = make_lw_shared<volume_reader>(std::move(read_cmd), std::move(rep));
        return reader->_is->consume(*reader).then([this, reader, volume_id, start_timestamp]() mutable{
            logger.debug("[handle] done, volume_id:{}, read latency:{}"
                , volume_id, hive_tools::get_current_time()-start_timestamp);
            return make_ready_future<std::unique_ptr<reply>>(std::move(reader->_rep));
        });
    });
}

}//namespace hive
