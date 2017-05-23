#include "create_extent_group_handler.hh"

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
#include "hive/hive_tools.hh"
#include "hive/context/context_service.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/http/json11.hh"


namespace hive{

static logging::logger logger("create_extent_group");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return itor->second;   
        }
    }
    
    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    sstring error_info = out.str();
    logger.error(error_info.c_str());
    throw std::invalid_argument(error_info);
}

static future<std::unordered_set<gms::inet_address>> get_targets(sstring disk_ids_str){
    auto disk_ids = hive_tools::split_to_set(disk_ids_str, ":");

    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_ids).then([](auto disk_contexts){
        std::unordered_set<gms::inet_address> targets;
        for(auto& context : disk_contexts) {
            sstring ip = context.get_ip(); 
            targets.insert(gms::inet_address(ip));
        }
        return make_ready_future<std::unordered_set<gms::inet_address>>(std::move(targets));
    });
}

static sstring build_success_content(){
    hive::Json commit_json = hive::Json::object {
        {"success", true},
    };
    auto content = commit_json.dump();
    return content;
}

static sstring build_error_content(std::unordered_set<gms::inet_address>& targets
                                 , std::unordered_set<gms::inet_address>& success){

    sstring error_info = sprint("targets:{}, success:{}"
        , hive_tools::format(targets), hive_tools::format(success));

    hive::Json commit_json = hive::Json::object {
        {"success", false},
        {"error_info", error_info.c_str()},
    };
    auto content = commit_json.dump();
    return content;
}

static bool is_all_success(std::unordered_set<gms::inet_address>& targets
                         , std::unordered_set<gms::inet_address>& success){

    for(auto& addr : targets){
        auto itor = success.find(addr); 
        if(itor == success.end()){
            return false; 
        }
    }
    return true;
}

future<std::unique_ptr<reply> > create_extent_group_handler::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring extent_group_id = get_header_value("x-extent-group-id", new_headers);
    sstring disk_ids_str    = get_header_value("x-disk-ids", new_headers);
    logger.debug("{} start, extent_group_id:{}, disk_ids_str{}", __func__, extent_group_id , disk_ids_str);

    return get_targets(disk_ids_str).then([extent_group_id, disk_ids_str, rep=std::move(rep)]
            (auto targets)mutable{
        auto disk_ids = hive_tools::split_to_vector(disk_ids_str, ":");
        smd_create_extent_group smd_create(extent_group_id, disk_ids);
        
        auto& proxy = hive::get_local_extent_store_proxy();
        return proxy.create_extent_group(std::move(smd_create), std::move(targets)).then(
                [rep=std::move(rep), targets](auto success)mutable{

            if(is_all_success(targets, success)){
                auto response = build_success_content();
                rep->set_status(reply::status_type::ok, response);
            }else {
                auto error = build_error_content(targets, success);
                rep->set_status(reply::status_type::internal_server_error, error);
            }
            rep->done("json");
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        });
    });
}

}//namespace hive
