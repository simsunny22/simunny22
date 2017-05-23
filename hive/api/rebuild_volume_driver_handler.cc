#include "rebuild_volume_driver_handler.hh"
#include "hive/volume_service.hh"



namespace hive{


static logging::logger logger("rebuild_volume_driver_handler");

sstring rebuild_volume_driver_handler::get_header_value(header_map& headers, sstring header_key, sstring default_value){
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


// =============================================================================
// public
// =============================================================================
future<std::unique_ptr<reply> > rebuild_volume_driver_handler::handle(
        const sstring& path
        , std::unique_ptr<request> req
        , std::unique_ptr<reply> rep){

    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring volume_ids = get_header_value(new_headers, "x-volume-ids");
   
    auto volume_id_set = hive_tools::split_to_set(volume_ids, ":");

    std::vector<future<>> futs;
    for(auto& volume_id: volume_id_set){
        auto fut = rebuild_volume_driver(volume_id);
        futs.push_back(std::move(fut));
    }


    return when_all(futs.begin(), futs.end()).then([rep = std::move(rep)](auto futs) mutable{
        try{
            for(auto& fut: futs){
                fut.get();
            }
            sstring response = "{\"success\":\"true\"}";
            rep->_content = response;
            rep->done();
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             std::ostringstream out;
             out << "[rebuild_volume_driver_handler] error";
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);           
        }
    });

    #if 0
    auto shard = get_local_volume_service().shard_of(volume_id);
    return get_volume_service().invoke_on(shard, [volume_id](auto& volume_service){
        return volume_service.rebuild_volume_driver(volume_id);
    }).then_wrapped([volume_id, rep = std::move(rep)](auto f)mutable{
        try{
            f.get();
            rep->done();
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             std::ostringstream out;
             out << "[rebuild_volume_driver_handler] error";
             out << ", volume_id:" << volume_id ;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
        
    });
    #endif

}//handle


future<> rebuild_volume_driver_handler::rebuild_volume_driver(sstring volume_id){
    auto shard = get_local_volume_service().shard_of(volume_id);
    return get_volume_service().invoke_on(shard, [volume_id](auto& volume_service){
        return volume_service.rebuild_volume_driver(volume_id);
    });
}


}//namespace hive
