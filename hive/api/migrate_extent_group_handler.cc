#include "migrate_extent_group_handler.hh"

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
#include "hive/migrate_forward_proxy.hh"

namespace hive{

static logging::logger logger("migrate_extent_group_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

migrate_params_entry parse_migrate_params(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring driver_node_ip  = get_header_value("x-driver-node-ip", new_headers);
    sstring container_name  = get_header_value("x-container-name", new_headers);
    sstring intent_id       = get_header_value("x-intent-id", new_headers);

    sstring volume_id       = get_header_value("x-volume-id", new_headers);
    sstring extent_group_id = get_header_value("x-extent-group-id", new_headers);
    sstring src_disk_id     = get_header_value("x-src-disk-id", new_headers);
    sstring dst_disk_id     = get_header_value("x-dst-disk-id", new_headers);
    if(  driver_node_ip.empty() 
      || container_name.empty() 
      || intent_id.empty()
      || volume_id.empty() 
      || extent_group_id.empty()
      || src_disk_id.empty()
      || dst_disk_id.empty())
    {
        std::ostringstream out;
        out << "[migrate_extent_group_handler] params invalid for migrate";
        out << ", driver_node_ip:" << driver_node_ip;
        out << ", container_name:" << container_name;
        out << ", intent_id:" << intent_id;
        out << ", volume_id:" << volume_id;
        out << ", extent_group_id:" << extent_group_id;
        out << ", src_disk_id:" << src_disk_id;
        out << ", dst_disk_id:" << dst_disk_id;
        sstring error_info = out.str();
        throw std::invalid_argument(error_info); 
    }

    sstring offset = get_header_value("x-offset", new_headers);
    uint64_t offset_n = offset=="" ? 0 : atol(offset.c_str());
    sstring length = get_header_value("x-length", new_headers);
    uint64_t length_n = length=="" ? 4*1024*1024 : atol(length.c_str());

    migrate_params_entry migrate_params(driver_node_ip 
                                      , container_name
                                      , intent_id 
                                      , volume_id
                                      , extent_group_id
                                      , src_disk_id
                                      , dst_disk_id
                                      , offset_n
                                      , length_n);


    return std::move(migrate_params);
}

sstring build_return_content(migrate_result_entry migrate_result){
    hive::Json commit_json = hive::Json::object {
        {"volume_id", migrate_result.volume_id.c_str()},
        {"extent_group_id", migrate_result.extent_group_id.c_str()},
        {"src_disk_id", migrate_result.src_disk_id.c_str()},
        {"dst_disk_id", migrate_result.dst_disk_id.c_str()},
        {"copy_file_success", migrate_result.copy_file_success ? "true" : "false"},
        {"delete_src_file_success", migrate_result.delete_src_file_success ? "true" : "false"},
        {"other_message", migrate_result.message},
    };
    sstring content = commit_json.dump();
    return content;
}
#if 0
//for api directly sent to driver node
future<std::unique_ptr<reply>> migrate_extent_group_handler::handle(const sstring& path
        , std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    auto migrate_params = parse_migrate_params(*req);
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);

    auto shard_id = hive::get_local_migration_manager().shard_of(migrate_params.extent_group_id);
    return hive::get_migration_manager().invoke_on(shard_id, [migrate_params, rep = std::move(rep)]
            (auto& shard_migration_manager) mutable {
        return shard_migration_manager.migrate_extent_group(migrate_params).then_wrapped(
                [rep=std::move(rep), migrate_params](auto f)mutable{
            try{
                migrate_result_entry migrate_result = f.get0();
                auto return_content = build_return_content(migrate_result);
                rep->set_status(reply::status_type::ok, return_content);
                rep->done("json");
                logger.debug("migrate_extent_group_handler done, extent_group_id:{}", migrate_params.extent_group_id);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }catch(...){
                logger.error("migrate_extent_group_handler failed, extent_group_id:{}, exception:{}"
                    , migrate_params.extent_group_id, std::current_exception());
                rep->set_status(reply::status_type::internal_server_error);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }
        });
    });
}
#endif

//for api directly sent to any node, not necessarily the driver node
future<std::unique_ptr<reply>> migrate_extent_group_handler::handle(const sstring& path
        , std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    auto migrate_params = parse_migrate_params(*req);
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);

    auto& local_proxy = hive::get_local_migrate_forward_proxy();
    return local_proxy.migrate_forward(migrate_params).then_wrapped(
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
}

}//namespace hive
