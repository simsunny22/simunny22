#include "replicate_extent_group_handler.hh"

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

namespace hive{

static logging::logger logger("replicate_extent_group_handler");


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

future<std::unique_ptr<reply>> replicate_extent_group_handler::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep) {
    
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring extent_group_id = get_header_value("x-extent-group-id", new_headers);
    sstring src_disk_id = get_header_value("x-src-disk-id", new_headers);
    sstring dst_disk_id = get_header_value("x-dst-disk-id", new_headers);
    sstring container_name  = get_header_value("x-container-name", new_headers);
    sstring repair_disk_id = get_header_value("x-repair-disk-id", new_headers);
 
    std::map<sstring, sstring> replicate_params;
    replicate_params["extent_group_id"] = extent_group_id;
    replicate_params["src_disk_id"] = src_disk_id;
    replicate_params["dst_disk_id"] = dst_disk_id;
    replicate_params["container_name"] = container_name;
    replicate_params["repair_disk_id"] = repair_disk_id;

    logger.debug("[{}] start, replicate_params:{}", __func__, hive_tools::format(replicate_params));

    auto shard_id = hive::get_local_migration_manager().shard_of(extent_group_id);
    return hive::get_migration_manager().invoke_on(shard_id, [replicate_params, rep = std::move(rep)]
            (auto& shard_manager) mutable {
        return shard_manager.replicate_extent_group(replicate_params).then_wrapped(
                [rep=std::move(rep), replicate_params](auto f)mutable{
            try{
                f.get();
                rep->set_status(reply::status_type::ok, "done");
                rep->done("json");
                logger.debug("replicate_extent_group_handler done, replicate_params:{}",hive_tools::format(replicate_params));
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }catch(...){
                logger.error("replicate_extent_group_handler failed, replicate_params:{}, exception:{}"
                    , hive_tools::format(replicate_params), std::current_exception());
                rep->set_status(reply::status_type::internal_server_error);
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            }
        });
    });
}

}//namespace hive
