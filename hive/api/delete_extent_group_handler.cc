#include "delete_extent_group_handler.hh"

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
#include "hive/hive_tools.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/hive_service.hh"
#include "hive/context/context_service.hh"
#include "hive/http/json11.hh"
#include "hive/http/seawreck.hh"
#include "hive/store/extent_store_proxy.hh"

namespace hive{

static logging::logger logger("delete_extent_group");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return header_value;    
        }
    }
   
    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    sstring error_info = out.str();
    logger.error(error_info.c_str());
    throw std::invalid_argument(error_info);
}

static future<> commit_to_metadata(
        sstring extent_group_id
        , sstring disk_id
        , sstring container_name){

    auto metadata_url = get_local_hive_service().get_hive_config()->pithos_server_url();
    sstring metadata_uri = metadata_url + "/v1/actions/commit_gc";
    hive::Json commit_json = hive::Json::object {
        {"extent_group_id", extent_group_id.c_str()},
        {"disk_id",         disk_id.c_str()},
        {"container_name",  container_name.c_str()},
    };
    auto commit_body = commit_json.dump();
    logger.debug("[{}] start, uri:{}, body:{}", __func__, metadata_uri, commit_body);
    hive::HttpClient client;
    return do_with(std::move(client), [metadata_uri, commit_body](auto& client){
        return client.post(metadata_uri, commit_body).then_wrapped([metadata_uri, commit_body](auto f){
            try {
                auto response = f.get0();
                if(!response.is_success()){
                    auto error_info = sprint("http response error code:", response.status()); 
                    throw std::runtime_error(error_info);
                }
                return make_ready_future<>();
            } catch (...) {
                std::ostringstream out;
                out << "[commit_to_metadata] failed";
                out << ", uri:" << metadata_uri;
                out << ", commit_body:" << commit_body;
                auto error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);
            }
        });
    });
}

static future<> delete_with_monad(
        sstring extent_group_id
        , sstring disk_id
        , sstring container_name
        , sstring file_path){
    auto& extent_store = hive::get_local_extent_store();
    return extent_store.with_monad(extent_group_id, 
            [extent_group_id, disk_id, container_name, file_path, &extent_store](){
        //1. delete extent_group 
        return extent_store.delete_extent_group(extent_group_id, file_path).then(
                [extent_group_id, disk_id, container_name](){
            //2. commit metadata
            return commit_to_metadata(extent_group_id, disk_id, container_name); 
        });
    });
}

static sstring build_success_return(sstring extent_group_id, sstring disk_ids, sstring container_name){
    hive::Json commit_json = hive::Json::object {
        {"extent_group_id", extent_group_id.c_str()},
        {"disk_id",         disk_ids.c_str()},
        {"container_name",  container_name.c_str()},
    };
    auto content = commit_json.dump();
    return content;
}

future<std::unique_ptr<reply> > delete_extent_group_handler::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring extent_group_id = get_header_value("x-extent-group-id", new_headers);
    sstring disk_id         = get_header_value("x-disk-id", new_headers);
    sstring container_name  = get_header_value("x-container-name", new_headers);

    logger.debug("{} start, extent_group_id:{}, disk_id:{}, container_name:{}"
        , __func__, extent_group_id , disk_id, container_name);

    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then([extent_group_id](auto context){
        //0. check disk_ip is me
        auto disk_ip = context.get_ip();
        if(!hive_tools::is_me(disk_ip)){
            throw std::runtime_error("bad request"); 
        }
        auto mount_path = context.get_mount_path();
        auto file_path = hive_tools::build_extent_group_file_path(mount_path, extent_group_id);
        return make_ready_future<sstring>(file_path);
    }).then([extent_group_id, disk_id, container_name](auto file_path){
        auto& extent_store = hive::get_local_extent_store();      
        auto shard_id = extent_store.shard_of(extent_group_id);
        return smp::submit_to(shard_id, [extent_group_id, disk_id, container_name, file_path](){
            //1. delete extent_group context    
            auto& context_service = hive::get_local_context_service();
            return context_service.remove_on_every_shard(extent_group_id).then(
                    [extent_group_id, disk_id, container_name, file_path](){
                //2. delete with_monad 
                return delete_with_monad(extent_group_id, disk_id, container_name, file_path); 
            });
        });
    }).then_wrapped([rep=std::move(rep), extent_group_id, disk_id, container_name](auto f)mutable{
        try{
            f.get(); 
            auto return_content = build_success_return(extent_group_id, disk_id, container_name);
            rep->set_status(reply::status_type::ok, return_content);
            rep->done("json");
            logger.debug("[delete_extent_group_handler] done, extent_group_id:{}", extent_group_id);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
            logger.error("[delete_extent_group_handler] failed, extent_group_id:{}, exception:{}"
                , extent_group_id, std::current_exception());
            //tododl:yellow need return detail error code
            rep->set_status(reply::status_type::internal_server_error);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }
    });
}

}//namespace hive
