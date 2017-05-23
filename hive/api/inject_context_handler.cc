#include "inject_context_handler.hh"

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

#include "hive/context/inject_context_helper.hh"
#include "hive/hive_tools.hh"


namespace hive{

static logging::logger logger("inject_context_handler");
static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

future<std::unique_ptr<reply> > inject_context_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    logger.debug("{} start", __func__);
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring context_ttl_t = get_header_value("x-context-ttl", new_headers);
    sstring disks = get_header_value("x-disks", new_headers);
    sstring volume_count_ = get_header_value("x-volume-count", new_headers);
    int32_t volume_count = hive_tools::str_to_int32(volume_count_); 
    sstring driver_node = get_header_value("x-driver-node", new_headers);
    sstring replica_nodes = get_header_value("x-replica-nodes", new_headers);
    sstring extent_group_count_ = get_header_value("x-extent-group-count", new_headers);
    int32_t extent_group_count = hive_tools::str_to_int32(extent_group_count_); 
    sstring target_disk_ids = get_header_value("x-target-disk-ids", new_headers);
    int32_t context_ttl   = hive_tools::str_to_int32(context_ttl_t); 
    logger.debug("{}, ttl:{}", __func__, context_ttl);
    
    return seastar::async([rep=std::move(rep), context_ttl, disks, volume_count, driver_node, replica_nodes, extent_group_count, target_disk_ids]()mutable {
        inject_context_helper context_helper;
        context_helper.inject_context(disks, volume_count, driver_node, replica_nodes, extent_group_count, target_disk_ids).get();
        rep->_content = "inject context success";
        rep->done("success");
        return std::unique_ptr<reply>(std::move(rep));
    });
}

}//namespace hive
