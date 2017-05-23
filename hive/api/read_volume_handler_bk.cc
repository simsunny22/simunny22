#include "read_volume_handler.hh"

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
#include "hive/hive_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/trail/trail_service.hh"


namespace hive{

static logging::logger logger("read_volume_handler");

sstring read_volume_handler::get_header_value(header_map& headers, sstring header_key, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return header_value;    
        }
    }
 
    if(!default_value.empty()){
        return default_value; 
    }

    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    auto error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);
}

bool read_volume_handler::check_is_access_trail_test(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring is_access_trail_test = get_header_value(new_headers, "x-log-test", "false"); 
    if("true" == is_access_trail_test){
        return true;
    }
    return false;
}

hive_access_trail_command read_volume_handler::build_access_trail_command(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring trail_disk_id = get_header_value(new_headers, "x-trail-disk-id", "1");
    sstring trail_node_id = get_header_value(new_headers, "x-trail-node-id", "1");
    hive_access_trail_command cmd(trail_disk_id, trail_node_id);
    return std::move(cmd);
}

lw_shared_ptr<hive_read_command> read_volume_handler::build_read_cmd(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    
    sstring volume_id       = get_header_value(new_headers, "x-volume-id");
    sstring extent_group_id = get_header_value(new_headers, "x-extent-group-id");
    sstring extent_id       = get_header_value(new_headers, "x-extent-id");

    sstring offset_g_t = get_header_value(new_headers, "x-offset-in-group"); //tododl:yellow extent_offset_in_group
    uint64_t extent_offset_in_group = atoi(offset_g_t.c_str()); 
  
    sstring offset_e_t = get_header_value(new_headers, "x-offset-in-extent");  //tododl:yellow data_offset_in_extent
    uint64_t data_offset_in_extent = atoi(offset_e_t.c_str()); 

    sstring length_t = get_header_value(new_headers, "x-length");
    uint64_t length   = atoi(length_t.c_str()); 
 
    sstring disk_ids = get_header_value(new_headers, "x-disk-ids");
    //sstring datum_md5 = get_header_value(new_headers, "x-datum-md5");
    sstring datum_md5 = ""; //tododl:red fix from guru
    sstring options = get_header_value(new_headers, "x-options", "empty"); //tododl:red fix from guru

    auto read_cmd = make_lw_shared<hive_read_command>(
          volume_id
        , extent_group_id 
        , extent_id
        , extent_offset_in_group
        , data_offset_in_extent
        , length
        , disk_ids
        , datum_md5
        , options
    );

    check_read_cmd(read_cmd);
    return std::move(read_cmd);
}

void read_volume_handler::check_read_cmd(lw_shared_ptr<hive_read_command> read_cmd){
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
// ============================================================================
// public
// ============================================================================
future<std::unique_ptr<reply> > read_volume_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    int64_t start_timestamp = hive_tools::get_current_time();
    //bool pseudo_read_enable = get_local_hive_service().get_hive_config()->pseudo_read_enable();
    auto read_cmd = build_read_cmd(*req);
    logger.debug("[{}] start, read_cmd:{}", __func__, *read_cmd);
    
    if(check_is_access_trail_test(*req)){
        //for test access trail
        auto access_trail_cmd =  build_access_trail_command(*req);
        return this->pseudo_read(std::move(rep), std::move(read_cmd), std::move(access_trail_cmd));
    }

    // 1. bind volume driver
    sstring volume_id = read_cmd->owner_id; 
    logger.debug("[{}] bind volume driver start, volume_id:{}", __func__, volume_id);
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (volume_service& volume_service){
        return volume_service.bind_volume_driver(volume_id);
    }).then([this, volume_id,  start_timestamp, read_cmd=std::move(read_cmd)
            , rep=std::move(rep)](auto driver_ex) mutable{
        if(driver_ex.need_redirect){
            // 1.1 redirect read
            return this->redirect_read(std::move(rep), driver_ex.redirect_ip);            
        }else{
            // 1.2 do read
            auto reader = make_lw_shared<volume_reader>(std::move(read_cmd), std::move(rep));
            logger.debug("[handle] volume_reader start, volume_id:{}", volume_id);
            return reader->_is->consume(*reader).then( [reader, volume_id, start_timestamp, this]() mutable{
                logger.debug("[handle] volume_reader done, volume_id:{}, read latency:{} usec"
                    , volume_id, hive_tools::get_current_time()-start_timestamp);
                return  make_ready_future<std::unique_ptr<reply>>(std::move(reader->_rep));
            });
        }
    });
}
// ============================================================================
// private
// ============================================================================
future<std::unique_ptr<reply>> 
read_volume_handler::redirect_read(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:red this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/read_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> 
read_volume_handler::pseudo_read(std::unique_ptr<reply> rep
        , lw_shared_ptr<hive_read_command> read_cmd
        , hive_access_trail_command access_trail_cmd){
    sstring extent_group_id  = read_cmd->extent_group_id;
    uint64_t length          = read_cmd->length;
    sstring  options         = read_cmd->options;
    sstring  pseudo_disk_id  = access_trail_cmd.disk_id;
    sstring  pseudo_node_id  = access_trail_cmd.node_id;
    trace_access_trail(pseudo_disk_id, extent_group_id, pseudo_node_id, length, options);
    rep->set_status(reply::status_type::ok);
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}


future<>
read_volume_handler::trace_access_trail(sstring disk_ids, sstring extent_group_id, sstring node_id, uint64_t size, sstring options){
    auto& trail_service = hive::get_local_trail_service();
    auto trail_params = make_shared<access_trail_params>(disk_ids, extent_group_id, node_id, size, options, access_trail_type::READ);
    auto trail_ptr = trail_service.get_trail(vega_trail_type::ACCESS_TRAIL);
    trail_ptr->trace(trail_params);
    return make_ready_future<>();
}

}//namespace hive
