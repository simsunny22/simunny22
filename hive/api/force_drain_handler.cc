#include "force_drain_handler.hh"

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
#include "hive/stream_service.hh"
#include "hive/volume_stream.hh"
#include "hive/journal/journal_service.hh"


namespace hive{

static logging::logger logger("force_drain_handler");

static sstring get_header_value(sstring header_key, header_map& headers, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = default_value; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

lw_shared_ptr<request_params> force_drain_handler::parse_params(request& req){
    header_map& headers = req._headers;
    header_map new_headers = hive_tools::lower_key(headers);
    sstring volume_id = get_header_value("x-volume-id", new_headers, "all");
    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("volume_id", volume_id));
    return std::move(params);
}


future<std::unique_ptr<reply> > force_drain_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    logger.debug("[{}], start", __func__);
    std::vector<sstring> volume_ids = {};
    auto content_length = req->get_content_length();
    if (content_length > 0) {
        sstring force_drain_json_str(req->_body.get(), content_length);
     
        std::string err = "";
        auto force_drain_json = hive::Json::parse(force_drain_json_str, err);
        if (!err.empty()){
            rep->_content = "force drain error {json error}";
            rep->done("error");
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            
        }
        for(auto& volume_id : force_drain_json["volume_ids"].array_items()) {
            volume_ids.push_back(volume_id.string_value());
        }
    }

    return hive::get_journal_service().invoke_on_all([volume_ids] (auto& journal_service) {
        if (volume_ids.size() == 0) {
            logger.debug("[{}], start force_drain_all", "force_drain_handler");
            return journal_service.force_drain_all();
        }

        logger.debug("[{}], start force_drain, volume_ids:{}", "force_drain_handler", volume_ids);
        return journal_service.force_drain(volume_ids);
    }).then_wrapped([rep=std::move(rep)](auto f) mutable {
        try {
            f.get();
            rep->_content = "force drain success! ";
            rep->done("ok");
            logger.debug("[{}], force drain success", "force_drain_handler");
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        } catch(...) {
            logger.error("[{}], force drain error:{}", "force_drain_handler", std::current_exception());
        }
        rep->_content = "force drain error ";
        rep->done("error");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}

}//namespace hive
