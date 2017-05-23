#include "replay_primary_journal_handler.hh"

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
#include "checked-file-impl.hh"
//#include "hive/http/json11.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_service.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "hive/stream_service.hh"

namespace hive{
static logging::logger logger("replay_primary_journal_handler");

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

lw_shared_ptr<request_params> replay_primary_journal_handler::parse_write_params(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);

    sstring volume_id = get_header_value("x-volume-id", new_headers);
    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("volume_id", volume_id));
    return std::move(params);
}

future<std::unique_ptr<reply> > replay_primary_journal_handler::handle(const sstring& path
                                                                     , std::unique_ptr<request> req
                                                                     , std::unique_ptr<reply> rep) {
    lw_shared_ptr<request_params> params = parse_write_params(*req);
    sstring volume_id = value_cast<sstring>(params->find("volume_id")->second);
    logger.debug("-->replay_primary_journal_handler handle start, volume_id:{}", volume_id);
    return replay_commitlog(volume_id).then([rep=std::move(rep)]() mutable {
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}

//tododl:test function for print
void print_commitlog_file(std::vector<sstring> commitlog_files){
    logger.debug("dltest===>>> print_commitlog_file, start");
    for(auto& file : commitlog_files){
        logger.debug("dltest===>>> print_commitlog_file, file:{}", file);
    }
}

future<> replay_primary_journal_handler::replay_commitlog(sstring volume_id) {
    logger.debug("-->replay_commitlog start");
    auto& hive_service = hive::get_hive_service();
    return hive_service.invoke_on(0, [volume_id](auto& shard_hive_service){
        return shard_hive_service.replay_commitlog(volume_id);
    });
}

}//namespace hive
