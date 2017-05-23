#include "write_extent_group_handler.hh"

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


namespace hive{

logging::logger log_write_extent_group_handler("wirte_extent_group_handler");
static int64_t s_vclock = 0;

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}


//params data
lw_shared_ptr<request_params> write_extent_group_handler::parse_write_params(request& req){
    header_map& headers = req._headers;

    sstring volume_id = get_header_value("X-Volume-Id", headers);
    sstring extent_id = get_header_value("X-Extent-Id", headers);
    sstring extent_group_id = get_header_value("X-Extent-Group-Id", headers);
    sstring disk_ids = get_header_value("X-Disk-Ids", headers);
    sstring offset_g_t = get_header_value("X-Offset-In-Group", headers);
    int64_t offset_g   = hive_toos::str_to_int64(offset_g_t); 
    sstring offset_e_t = get_header_value("X-Offset_In-Extent", headers);
    int64_t offset_e   = hive_toos::str_to_int64(offset_e_t);
    sstring length_t = get_header_value("X-Length", headers);
    int64_t length   = hive_toos::str_to_int64(length_t); 
    int64_t vclock = s_vclock++; 
    bytes body = bytes(reinterpret_cast<const signed char *>(req._body.get()), req.get_content_length());

    //assert((size_t)length == body.size());
    //assert(offset_e+length<=hive_config::extent_size);

    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("volume_id", volume_id));
    params->insert(std::make_pair("extent_group_id", extent_group_id));
    params->insert(std::make_pair("extent_id", extent_id));
    params->insert(std::make_pair("vclock", vclock));
    params->insert(std::make_pair("extent_offset_in_group", offset_g));
    params->insert(std::make_pair("data_offset_in_extent", offset_e));
    params->insert(std::make_pair("length", length));
    params->insert(std::make_pair("disk_ids", disk_ids));
    params->insert(std::make_pair<sstring, data_value>("body", data_value(body)));

    return std::move(params);
}

//swrite data
future<std::unique_ptr<reply>> write_extent_group_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
#if 0

    std::cout << "===========write_extent_group_handler ...start... by wz===============" << std::endl;
    lw_shared_ptr<request_params> params = parse_write_params(*req);
    sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);

    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, params = std::move(params), rep = std::move(rep)] (volume_service& volume_service){
    //return hive::get_volume_service().invoke_on(shard, [params = std::move(params), rep = std::move(rep)](volume_service&　volume_service){
        //bind volume driver
        //sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);
        return volume_service.bind_volume_driver(volume_id);
    }).then([params = std::move(params), rep = std::move(rep)](auto volume_driver_ptr){
        //to check token
        return make_ready_future<>();
    }).then([params = std::move(params), rep = std::move(rep)](){
        //write to stream
        sstring extent_group_id = value_cast<sstring>(params->find("extent_group_id")->second);

        auto shard = hive::get_local_stream_service().shard_of(extent_group_id);
        return hive::get_stream_service().invoke_on(shard, [params = std::move(params)](stream_service& stream_service){
            sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);
            sstring extent_id              = value_cast<sstring>(params->find("extent_id")->second);
            sstring extent_group_id        = value_cast<sstring>(params->find("extent_group_id")->second);
            sstring disk_ids               = value_cast<sstring>(params->find("disk_ids")->second);
            int64_t extent_offset_in_group = value_cast<int64_t>(params->find("extent_offset_in_group")->second);
            bytes data                     = value_cast<bytes>(params->find("body")->second);
            sstring md5 = hive_tools::calculate_md5(data);

            extent_datum datum(extent_id, extent_group_id, extent_offset_in_group, disk_ids, md5, std::move(data));
            std::vector<extent_datum> datums;
            datums.push_back(std::move(datum));
            return stream_service.find_or_create_stream(volume_id).then([volume_id, datums = std::move(datums)](auto stream) mutable{
                return stream->write_extent_group(std::move(datums));
            });
        });
    }).then_wrapped([volume_id,rep = std::move(rep)](auto f) mutable{
        try{
            f.get();
            rep->_content = "success";
            rep->done("success");
            log_write_extent_group_handler.debug("[{}]write_extent_group_handler handle done!", volume_id);
            std::cout << "===========write_extent_group_handler successssssssssssssssssssss...end... by wz===============" << std::endl;
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
            std::cout << "===========write_extent_group_handler failllllllllllllllllllllllll..end... by wz===============" << std::endl;
            log_write_extent_group_handler.error("[{}]write_extent_group_handler handle failed!", volume_id);
            throw std::runtime_error("write_extent_group_handler handle failed!");
        }
    });
#endif

}

}//namespace hive
