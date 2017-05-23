#include "read_extent_group_handler.hh"

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


namespace hive{

logging::logger log_read_extent_group_handler("read_extent_group_handler");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

lw_shared_ptr<request_params> read_extent_group_handler::parse_read_params(request& req){
    header_map& headers = req._headers; 
    
    sstring volume_id              = get_header_value("X-Volume-Id", headers);
    sstring extent_group_id        = get_header_value("X-Extent-Group-Id", headers);
    sstring extent_id              = get_header_value("X-Extent-Id", headers);
    sstring offset_g_t             = get_header_value("X-Offset-In-Group", headers);
    int64_t extent_offset_in_group = hive_tools::str_to_int64(offset_g_t); 
    sstring offset_e_t             = get_header_value("X-Offset-In-Extent", headers);
    int64_t offset_in_extent       = hive_tools::str_to_int64(offset_e_t); 
    sstring length_t               = get_header_value("X-Length", headers);
    int64_t length                 = hive_tools::str_to_int64(length_t); 
    sstring disk_ids               = get_header_value("X-Disk-Ids", headers);
    sstring datum_md5              = get_header_value("X-Datum-md5", headers);
    
    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("volume_id", volume_id));
    params->insert(std::make_pair("extent_group_id", extent_group_id));
    params->insert(std::make_pair("extent_id", extent_id));
    params->insert(std::make_pair("extent_offset_in_group", extent_offset_in_group));
    params->insert(std::make_pair("data_offset_in_extent", offset_in_extent));
    params->insert(std::make_pair("length", length));
    params->insert(std::make_pair("disk_ids", disk_ids));
    params->insert(std::make_pair("datum_md5", datum_md5));

    return std::move(params);
}

//read_extent_group_handler
future<std::unique_ptr<reply>> read_extent_group_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    lw_shared_ptr<request_params> params = parse_read_params(*req);
    sstring volume_id              = value_cast<sstring>(params->find("volume_id")->second);
     
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, params = std::move(params), rep = std::move(rep)] 
            (volume_service& volume_service){
        return volume_service.bind_volume_driver(volume_id);
    }).then([volume_id, params = std::move(params), rep = std::move(rep)](auto volume_driver_ptr){
    //to check token 
        return make_ready_future<>();
    }).then([volume_id, params = std::move(params), rep = std::move(rep)](){
    //write to stream
        sstring extent_group_id = value_cast<sstring>(params->find("extent_group_id")->second);
        auto shard = hive::get_local_stream_service().shard_of(extent_group_id);
        return hive::get_stream_service().invoke_on(shard, [volume_id, params = std::move(params)] (stream_service& stream_service){
            sstring extent_group_id        = value_cast<sstring>(params->find("extent_group_id")->second);
            sstring extent_id              = value_cast<sstring>(params->find("extent_id")->second);
            int64_t extent_offset_in_group = value_cast<int64_t>(params->find("extent_offset_in_group")->second);
            int64_t data_offset_in_extent       = value_cast<int64_t>(params->find("data_offset_in_extent")->second);
            int64_t length                 = value_cast<int64_t>(params->find("length")->second);
            sstring disk_ids               = value_cast<sstring>(params->find("disk_ids")->second);
            sstring datum_md5              = value_cast<sstring>(params->find("datum_md5")->second);
            auto cmd = ::make_lw_shared<hive_read_command>(
                  volume_id 
                , extent_group_id 
                , extent_id
                , extent_offset_in_group
                , data_offset_in_extent
                , length
                , disk_ids
                , datum_md5
                , "default options");
            return stream_service.find_or_create_stream(volume_id).then([cmd = std::move(cmd)](auto stream) mutable{
                return stream->read_extent_group(std::move(cmd));
            });
       });
    }).then_wrapped([volume_id, rep = std::move(rep)](auto f) mutable{
       try{
            extent_datum datum = f.get0();
            temporary_buffer<char> buf(datum.data.size());
            auto data_begin = datum.data.begin();
            auto data_size  = datum.data.size();
            std::copy_n(data_begin, data_size, buf.get_write());

            rep->_content.append(buf.get(), buf.size());
            log_read_extent_group_handler.debug("[{}]read_extent_group_handler handle done!", volume_id);
            std::cout << "read_extent_group_handle........111111111111111111111 true" << std::endl;
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
       }catch(...){
            log_read_extent_group_handler.error("[{}]read_extent_group_handler handle failed!", volume_id);
            std::cout << "read_extent_group_handle........22222222222222222222  false" << std::endl;
            throw std::runtime_error("read_extent_group_handler handle failed!");
       }
    });
}

}//namespace hive
