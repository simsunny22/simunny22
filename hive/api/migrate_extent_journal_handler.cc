#include "migrate_extent_journal_handler.hh"

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
#include "hive/http/seawreck.hh"
#include "hive/http/json11.hh"
#include "hive/migration_manager.hh"
#include "hive/context/context_service.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/stream_file_reader.hh"
#include "hive/migrate_forward_proxy.hh"
#include "hive/file_store.hh"
#include "hive/hive_service.hh"


namespace hive{

static logging::logger logger("migrate_extent_journal_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

//static bool is_me(gms::inet_address from) {
//    return from == utils::fb_utilities::get_broadcast_address();
//}

static migrate_scene get_scene(uint64_t scene_type){
    switch(scene_type) {
        case 11:
            return migrate_scene::PRIMARY_TO_PRIMARY;
        case 12:
            return migrate_scene::PRIMARY_TO_SECONDARY;
        case 22: 
            return migrate_scene::SECONDARY_TO_SECONDARY;
        case 21:
            return migrate_scene::SECONDARY_TO_PRIMARY;
        default:
            return migrate_scene::UNKNOWN_SCENE;
    }
}

static migrate_params_entry parse_migrate_params(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring volume_id   = get_header_value("x-volume-id", new_headers);
    sstring src_node_ip = get_header_value("x-src-node-ip", new_headers);
    sstring dst_node_ip = get_header_value("x-dst-node-ip", new_headers);
    sstring scene_type_t  = get_header_value("x-scene-type", new_headers);
    uint64_t scene_type = hive_tools::str_to_uint64(scene_type_t);
    migrate_scene scene = get_scene(scene_type);

    migrate_params_entry migrate_params(scene
                                      , volume_id
                                      , src_node_ip 
                                      , dst_node_ip);

    return std::move(migrate_params);
}

future<std::unique_ptr<reply>> migrate_extent_journal_handler::handle(const sstring& path
        , std::unique_ptr<request> req, std::unique_ptr<reply> rep) {

    auto migrate_params = parse_migrate_params(*req);
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);

    auto shard_id = hive::get_local_migrate_forward_proxy().shard_of(migrate_params.volume_id);
    return hive::get_migrate_forward_proxy().invoke_on(shard_id, [migrate_params, rep = std::move(rep)]
            (auto& shard_proxy) mutable {
        return shard_proxy.migrate_forward(migrate_params).then_wrapped(
                [rep=std::move(rep), migrate_params](auto f)mutable{
            try{
                f.get();
                rep->set_status(reply::status_type::ok, "done");
                rep->done("json");
                logger.debug("migrate_extent_group_handler done, migrate_params:{}", migrate_params);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }catch(...){
                logger.error("migrate_extent_group_handler failed, migrate_params:{}, exception:{}"
                    , migrate_params, std::current_exception());
                rep->set_status(reply::status_type::internal_server_error);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }
        });
    });
}

future<> migrate_extent_journal_handler::test(){
    sstring src_file_path = "/opt/vega/data/hive/disk1/src_file";
    sstring dst_file_path = "/opt/vega/data/hive/disk1/dst_file";
    logger.debug("[{}] start, src_file:{}, dst_file:{}", __func__, src_file_path, dst_file_path); 

    return open_file_dma(src_file_path, open_flags::ro).then([this, dst_file_path](file f) mutable {
        file_input_stream_options stream_options;
        stream_options.buffer_size = hive::get_local_hive_service().get_hive_config()->stream_chunk_size_in_kb()*1024;
        stream_file_reader file_reader(std::move(f), 0, 33554432, stream_options);

        uint64_t offset = 0;
        return do_with(std::move(file_reader), std::move(offset), [this, dst_file_path](auto& file_reader, auto& offset){
            return repeat([this, &offset, &file_reader, dst_file_path](){
                return file_reader().then([this, &offset, dst_file_path](auto data){
                    if(!data.empty()){
                        uint64_t length = data.size();
                        bytes content(length, 0);
                        std::memcpy(content.begin(), data.begin(), length);

                        return do_with(std::move(content), [dst_file_path, &offset, length](auto& content){
                            return hive::write_file(dst_file_path, offset, length, content).then(
                                    [&offset, length](){
                                logger.debug("[test] offset:{}, length:{}", offset, length);
                                offset += length;
                                return stop_iteration::no;
                            });
                        });
                    }else{
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                });
            });
        });
    });

}


}//namespace hive
