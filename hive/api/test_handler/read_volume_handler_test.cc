#include "read_volume_handler_test.hh"

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


namespace hive{

static logging::logger logger("read_volume_handler_test");
const int LEN = 4*1024;
extern sstring got_random(int range);

static sstring get_extent_group_id(sstring volume_id, int64_t extent_group_num){
    uint64_t r_extent_group_num = rand() % extent_group_num + 1;
    uint64_t dir_num = r_extent_group_num % 10;
    sstring sub_dir_name = sprint("%s%s", dir_num, dir_num); 
    return volume_id + "_extent_group_" + to_sstring(r_extent_group_num) + "_" + sub_dir_name ;
}

static sstring get_header_value(sstring header_key, header_map& headers, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = default_value; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

lw_shared_ptr<hive_read_command> 
read_volume_handler_test::build_read_cmd(request& req){
    header_map& headers = req._headers;
    sstring volume_num_str = get_header_value("volume_num", headers, "50");
    int64_t volume_num = hive_tools::str_to_int64(volume_num_str); 
    sstring extent_group_num_str = get_header_value("extent_group_num", headers, "50");
    int64_t extent_group_num = hive_tools::str_to_int64(extent_group_num_str); 
    sstring disk_ids = get_header_value("disk_id", headers, "1");
    
    
    sstring volume_id = "volume_" + got_random(volume_num);
    sstring extent_group_id = get_extent_group_id(volume_id, extent_group_num);

    int extent_id_ = rand() % 4 + 1;
    sstring extent_id = "extent_" + extent_group_id + "_" + to_sstring(extent_id_);

    int64_t extent_offset_in_group = (extent_id_ - 1) * 1024 * 1024; 
    int64_t data_offset_in_extent = (rand()%2000)*512;
    int64_t length   = LEN; 
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

future<std::unique_ptr<reply> > read_volume_handler_test::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    int64_t start_timestamp = hive_tools::get_current_time();
    auto read_cmd = build_read_cmd(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, *read_cmd);

    auto volume_id = read_cmd->owner_id;
    //bind volume driver
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (volume_service& volume_service){
        return volume_service.bind_volume_driver(volume_id);
    }).then([](auto driver_ex){
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
