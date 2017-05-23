#include "get_extent_group_md5_handler.hh"

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
#include "hive/extent_store.hh"


namespace hive{

static logging::logger logger("get_extent_group_md5_handler");
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

static sstring get_header_value(sstring header_key, sstring default_value, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return itor->second;   
        }
    }
    return default_value;
}
future<std::unique_ptr<reply> > get_extent_group_md5_handler::handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    logger.debug("[{}] start", __func__);
    header_map& headers = req->_headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring extent_group_id = get_header_value("x-extent-group-id", new_headers);
    sstring offset_tmp = get_header_value("x-offset", "0", new_headers);
    uint64_t offset = hive_tools::str_to_uint64(offset_tmp);
    sstring length_tmp = get_header_value("x-length", "4194304", new_headers);
    uint64_t length = hive_tools::str_to_uint64(length_tmp);
    sstring disk_id = get_header_value("x-disk-id", new_headers);

    logger.debug("[{}] extent_group_id:{} disk_id:{}", extent_group_id, disk_id);

    //tododl: this for test so no use the proxy for simple
    auto& local_extent_store = hive::get_local_extent_store();
    return local_extent_store.get_extent_group_md5(extent_group_id, offset, length, disk_id).then_wrapped(
            [extent_group_id, disk_id, rep=std::move(rep)](auto f)mutable{
        try {
            sstring md5 = f.get0(); 
            sstring response = "{\"md5\":\"" + md5 + "\"}";   
            rep->_content = response;
            rep->done("json");
            return std::unique_ptr<reply>(std::move(rep));
        }catch (...) {
            std::ostringstream out; 
            out << "[handle] error, extent_group_id:" << extent_group_id;
            out << ", disk_id:" << disk_id;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            throw std::runtime_error(error_info);
        }
    });

}


}//namespace hive
