#include "hive/context/context_service.hh"
#include "seastar/core/distributed.hh"
#include "seastar/core/print.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/sleep.hh"

namespace hive {

distributed<context_service> _the_context_service;

static logging::logger logger("context_service");

context_service::context_service(const db::config& config)
    : _config(std::make_unique<db::config>(config)) {
}

context_service::~context_service() {

}              

future<> context_service::start() {
    return make_ready_future<>();
}

future<> context_service::stop() { 
    return make_ready_future<>(); 
}

future<> context_service::set(context_type type, sstring key, std::shared_ptr<context_entry> value) {
    auto itor = _context_map.find(key);
    if(itor != _context_map.end()) {
       _context_map.erase(itor);
    }
    auto ret = _context_map.insert(std::make_pair<sstring, std::shared_ptr<context_entry>>(std::move(key), std::move(value)));
    if(false == ret.second){
        sstring error_info = "insert error, context_key:" + key;
        logger.error(error_info.c_str()); 
        throw std::runtime_error(error_info);
    }else{
        if(context_type::DISK_CONTEXT == type){
            _disk_ids.insert(key); 
        }else if(context_type::VOLUME_CONTEXT == type){
            _volume_ids.insert(key);  
        }
        return make_ready_future<>();
    }
}

future<> context_service::set_all_shard(context_type type, sstring key, std::shared_ptr<context_entry> value) {
    return _the_context_service.invoke_on_all([type, key, value](auto& shard_context_service) {
        return shard_context_service.set(type, key, value);            
    });
}   

future<> context_service::remove(sstring context_key){
    auto itor = _context_map.find(context_key);
    if(itor != _context_map.end()) {
       _context_map.erase(itor);
    }
    return make_ready_future<>();
}

future<> context_service::remove_on_every_shard(sstring context_key){
    return _the_context_service.invoke_on_all([context_key](auto& shard_context_service){
        return shard_context_service.remove(context_key); 
    });
}

future<> context_service::remove_all() {
    _context_map.clear(); 
	  return make_ready_future<>();
}

future<> context_service::remove_all_on_every_shard() {
    return _the_context_service.invoke_on_all([](auto& shard_context_service){
        return shard_context_service.remove_all(); 
    });
}

sstring context_service::get_context_value(sstring key) {
    auto itor = _context_map.find(key);
	  if(itor == _context_map.end()) {
        sstring value = "none";
	      return value;
	  }
	  return itor->second->get_entry_string();
}
// ==================================================================
// for volume context
// ==================================================================
future<volume_context_entry> context_service::get_or_pull_volume_context(sstring volume_id) {
    logger.debug("[{}] start, volume_id:{}", __func__, volume_id); 
    auto itor = _context_map.find(volume_id);
	  if(itor != _context_map.end()) {
        auto context_ptr = std::dynamic_pointer_cast<volume_context_entry>(itor->second);
        volume_context_entry context_entry = *context_ptr;
        logger.debug("[{}] done, volume_id:{}", __func__, volume_id); 
        return make_ready_future<volume_context_entry>(std::move(context_entry));
    }else{
        logger.debug("[{}] need pull from metadata, volume_id:{}", __func__, volume_id); 
        std::set<sstring> volume_ids;
        volume_ids.insert(volume_id);
        return pull_volume_context(volume_ids).then([this, volume_id](auto volume_context_json){
            return this->save_volume_context(volume_context_json);
        }).then([this, volume_id](){
            auto volume_context = this->get_volume_context(volume_id);
            return make_ready_future<volume_context_entry>(volume_context);
        });
    }
}

volume_context_entry context_service::get_volume_context(sstring volume_id) {
    auto itor = _context_map.find(volume_id);
	  if(itor != _context_map.end()) {
        auto volume_context = std::dynamic_pointer_cast<volume_context_entry>(itor->second);
        return *volume_context;
    }else{
        throw std::runtime_error(sprint("error, can not find volume context, volume_id:%s", volume_id));
    }
}

future<sstring> context_service::pull_volume_context(std::set<sstring> volume_ids) {
    logger.debug("[{}] start volume_ids.size()={}", __func__, volume_ids.size());
    if(0 == volume_ids.size()) {
        logger.warn("[{}] do nothing, because volume_ids is empty");
        return make_ready_future<sstring>("[]");
    }
        
    sstring server_url = _config->pithos_server_url();
    sstring uri = server_url + "/v1/actions/volume/context";
    sstring json_body = build_request_body_for_volume(volume_ids);
    return do_pull_context(uri, json_body);
}

future<> context_service::save_volume_context(sstring volume_contexts_json) {
    logger.debug("[{}] start, volume_context_json:{}", __func__, volume_contexts_json);
    std::vector<future<>> futures;
    std::string err;
    auto contexts = hive::Json::parse(volume_contexts_json, err);
    for(auto& volume_context : contexts["volume_contexts"].array_items()) {
        sstring volume_id = volume_context["volume_uuid"].string_value();
        auto context_ptr = parse_volume_context_json(volume_context);
         
        auto& local_context_service = hive::get_local_context_service();
        auto volume_fut = local_context_service.set_all_shard(context_type::VOLUME_CONTEXT, volume_id, context_ptr);
        futures.push_back(std::move(volume_fut));
    }

    return when_all(futures.begin(), futures.end()).discard_result();
}

sstring context_service::build_request_body_for_volume(std::set<sstring>& volume_ids) {
    sstring build_str = "{\"volume_ids\":[";
    int count = 0;
    for(auto volume_id : volume_ids) {
        if( 0 == count) {
            build_str += "\"" + volume_id + "\"";
        } else {
            build_str += ",\"" + volume_id + "\"";
        }

        ++count;
    }

    build_str += "]}";
    std::string err;
    auto post_json = hive::Json::parse(build_str, err);
    auto json_body = post_json.dump();
    return json_body;
}

std::shared_ptr<context_entry> context_service::parse_volume_context_json(hive::Json volume_context) {
    //volume_id  
    sstring volume_uuid = volume_context["volume_uuid"].string_value();

    //volume_driver_node
    auto driver_node_context = volume_context["volume_driver_node"].object_items(); 
    sstring driver_node_id = driver_node_context["node_id"].string_value();
    sstring driver_node_ip = driver_node_context["node_ip"].string_value();
    node_context_entry driver_node(driver_node_id, driver_node_ip);

    //journal_nodes
    std::vector<node_context_entry> journal_nodes;
    for(auto& journal_node : volume_context["node_map"].array_items()) {
        sstring journal_node_id = journal_node["node_id"].string_value();
        sstring journal_node_ip = journal_node["node_ip"].string_value();
        node_context_entry journal_node_entry(journal_node_id, journal_node_ip);
        journal_nodes.push_back(journal_node_entry);
    }

    uint64_t vclock           = volume_context["vclock"].uint64_value();
    sstring cluster_uuid      = volume_context["cluster_uuid"].string_value();
    sstring storage_pool_uuid = volume_context["storage_pool_uuid"].string_value();
    sstring container_name    = volume_context["container_name"].string_value();
    sstring container_uuid    = volume_context["container_uuid"].string_value();
    sstring last_extent_group_id = volume_context["last_extent_group_id"].string_value();

    auto context_ptr = std::make_shared<volume_context_entry>(volume_uuid, driver_node, journal_nodes, vclock
       , cluster_uuid, storage_pool_uuid, container_name, container_uuid, last_extent_group_id);
    return context_ptr;

}
// =========================================================================
// for disk context
// =========================================================================
future<disk_context_entry> context_service::get_or_pull_disk_context(sstring disk_id) {
    logger.debug("[{}] start disk_id:{}", __func__, disk_id);
    auto itor = _context_map.find(disk_id);
	  if(itor != _context_map.end()) {
        logger.debug("[{}] done disk_id:{}", __func__, disk_id);
        auto disk_context = std::dynamic_pointer_cast<disk_context_entry>(itor->second);
        return make_ready_future<disk_context_entry>(*disk_context);
    }else{
        logger.debug("[{}] need pull from metadata, disk_id:{}", __func__, disk_id);
        sstring server_url = _config->pithos_server_url();
        std::set<sstring> disk_ids;
        disk_ids.insert(disk_id);
        return pull_disk_context(server_url, disk_ids).then([this](auto disk_context_json){
            return this->save_disk_context(disk_context_json);
        }).then([this, disk_id](){
            auto disk_context = this->get_disk_context(disk_id); 
            logger.debug("[get_or_pull_disk_context] done disk_id:{}", disk_id);
            return make_ready_future<disk_context_entry>(disk_context);
        });
    }
}

future<std::vector<disk_context_entry>> 
context_service::get_or_pull_disk_context(std::set<sstring> disk_ids) {
    logger.debug("[{}] start disk_ids.size():{}", __func__, disk_ids.size());
    
    std::vector<disk_context_entry> contexts = get_disk_context(disk_ids);
    if(contexts.size() == disk_ids.size()){
        return make_ready_future<std::vector<disk_context_entry>>(std::move(contexts));
    }

    // contexts not enough need pull from guru
    sstring server_url = _config->pithos_server_url();
    return pull_disk_context(server_url, disk_ids).then([this](auto disk_context_json){
        return this->save_disk_context(disk_context_json);
    }).then([this, disk_ids](){
        std::vector<disk_context_entry> contexts = this->get_disk_context(disk_ids); 
        if(contexts.size() == disk_ids.size() ) {
            logger.debug("[get_or_pull_disk_context] done disk_ids.size():{}", disk_ids.size());
            return make_ready_future<std::vector<disk_context_entry>>(std::move(contexts));
        }else{
            std::ostringstream out;
            out << "[get_or_pull_disk_context] failed, target disk_ids:{";
            for(auto disk_id : disk_ids){
                out << disk_id << ",";   
            }
            out << "}, get success disk_ids:{";
            for(auto context : contexts){
                out << context.get_id() << ",";
            }
            out << "}";
            auto error_info = out.str();
            logger.error(error_info.c_str());
            throw std::runtime_error(error_info);
        }
    });
}

disk_context_entry context_service::get_disk_context(sstring disk_id) {
    logger.debug("[{}] start disk_id:{}", __func__, disk_id);
    auto itor = _context_map.find(disk_id);
	  if(itor != _context_map.end()) {
        auto disk_context = std::dynamic_pointer_cast<disk_context_entry>(itor->second);
        return *disk_context;
	  }
	
    std::ostringstream out;
    out << "error, get_disk_context failed, not found in contexts, disk_id:" << disk_id;
    sstring error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);  
}

std::vector<disk_context_entry> context_service::get_disk_context(std::set<sstring> disk_ids) {
    logger.debug("[{}] start disk_ids.size():{}", __func__, disk_ids.size());
    if(disk_ids.size() <= 0){
        auto error_info = sprint("[{}] disk_ids.size() == {}", __func__, disk_ids.size());
        logger.error(error_info.c_str()); 
        throw std::runtime_error(error_info);
    }
   
    std::vector<disk_context_entry> contexts;
    for(auto disk_id : disk_ids){
        auto itor = _context_map.find(disk_id);
	      if(itor != _context_map.end()) {
            auto disk_context = std::dynamic_pointer_cast<disk_context_entry>(itor->second);
            contexts.push_back(*disk_context);
        }
    }

    return std::move(contexts);
	
}
future<sstring> context_service::pull_disk_context(sstring server_url, std::set<sstring> disk_ids) {
    logger.debug("[{}] start, server_url:{}", __func__, server_url);
    if(0 == disk_ids.size()) {
        logger.warn("[pull_disk_context] do nothing, because disk_ids is empty");
        return make_ready_future<sstring>("[]");
    }else{
        sstring json_body = build_json_body_for_disk(disk_ids);
        sstring uri = server_url + "/v1/actions/disks";
        return do_pull_context(uri, json_body);
    }
}

future<> context_service::save_disk_context(sstring disk_contexts_json) {
    logger.debug("[{}] start, disk_context:{}", __func__, disk_contexts_json);
    std::vector<future<>> futures;
    std::string err;
    auto contexts = hive::Json::parse(disk_contexts_json, err);
    for(auto& context : contexts["disk_entries"].array_items()) {
        sstring disk_id = context["id"].string_value();
        auto context_ptr = parse_disk_context_json(context);
        
        context_service& local_context_service = hive::get_local_context_service();
        auto f = local_context_service.set_all_shard(context_type::DISK_CONTEXT, disk_id, context_ptr);
        futures.push_back(std::move(f));
    }

    return when_all(futures.begin(), futures.end()).discard_result();
}

sstring context_service::build_json_body_for_disk(std::set<sstring>& disk_ids) {
    sstring build_str = "{\"disk_ids\":[";
    int count = 0;
    for(auto disk_id : disk_ids) {
        if( 0 == count) {
            build_str += "\"" + disk_id + "\"";
        } else {
            build_str += ",\"" + disk_id + "\"";
        }

        ++count;
    }

    build_str += "]}";
    std::string err;
    auto post_json = hive::Json::parse(build_str, err);
    auto json_body = post_json.dump();
    return json_body;
}

std::shared_ptr<context_entry> context_service::parse_disk_context_json(hive::Json disk_context) {
    sstring disk_id         = disk_context["id"].string_value();
    sstring disk_host_ip    = disk_context["cvm_ip_address"].string_value();
    sstring disk_mount_path = disk_context["mount_path"].string_value();
    sstring disk_type       = disk_context["storage_tier_name"].string_value();
    
    if(disk_id.empty() || disk_host_ip.empty() || disk_mount_path.empty()){
        std::ostringstream out;
        out << "[parse_disk_context_json] error context params maybe empty";
        out << ", disk_id:" << disk_id;
        out << ", disk_host_ip:" << disk_host_ip;
        out << ", disk_mount_path:" << disk_mount_path;
        out << ", disk_type:" << disk_type;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }

    auto context_ptr = std::make_shared<disk_context_entry>(disk_id, disk_host_ip, disk_mount_path, disk_type);
    return context_ptr;
}


bool context_service::disk_context_exist(std::set<sstring> disk_ids) {
    for(auto disk_id : disk_ids) {
        auto itor = _context_map.find(disk_id);
	      if(itor == _context_map.end()) {
            return false; 
        }
    }
    return true;
}

future<bool> context_service::ensure_disk_context(std::set<sstring> disk_ids) {
    if(disk_context_exist(disk_ids)) {
        return make_ready_future<bool>(true);
    }
    std::vector<future<disk_context_entry>> futures;
    for(auto disk_id : disk_ids) {
        auto f = get_or_pull_disk_context(disk_id); 
        futures.push_back(std::move(f));
    }

    return when_all(futures.begin(), futures.end()).then([](auto ret){
        try {
            for(auto& f : ret) {
                f.get(); 
            }
            return make_ready_future<bool>(true);
        }catch(...) {
            logger.error("error, ensure_disk_context, exception:{}", std::current_exception());
            return make_ready_future<bool>(false);
        }
    });
}
// ==============================================================
// for extent group context
// ==============================================================
future<extent_group_context_entry> 
context_service::get_or_pull_extent_group_context(sstring owner_id, sstring extent_group_id) {
    logger.debug("[{}] start, owner_id:{}, extent_group_id:{}", __func__, owner_id, extent_group_id); 
    auto itor = _context_map.find(extent_group_id);
	  if(itor != _context_map.end()) {
        logger.debug("[get_or_pull_extent_group_context] done, owner_id:{}, extent_group_id:{}"
            , owner_id, extent_group_id); 
        auto extent_group_context = std::dynamic_pointer_cast<extent_group_context_entry>(itor->second);
        return make_ready_future<extent_group_context_entry>(*extent_group_context);
	  }

    std::set<sstring> extent_group_ids;
    extent_group_ids.insert(extent_group_id);
    logger.debug("[{}] need pull from metadata, owner_id:{}, extent_group_id:{}"
        , __func__, owner_id, extent_group_id);
    return pull_extent_group_context(std::move(owner_id), std::move(extent_group_ids))
          .then([this, extent_group_id](auto contexts_json){
       return this->save_extent_group_context(contexts_json).then([this, extent_group_id](){
           auto context = this->get_extent_group_context(extent_group_id);
           logger.debug("[get_or_pull_extent_group_context] done, extent_group_id:{}", extent_group_id); 
           return make_ready_future<extent_group_context_entry>(context);  
       }); 
    });
}

extent_group_context_entry context_service::get_extent_group_context(sstring extent_group_id) {
    logger.debug("[{}] start, extent_group_id:{}", __func__, extent_group_id);
    auto itor = _context_map.find(extent_group_id);
	  if(itor == _context_map.end()){
        std::ostringstream out;
        out << "[get_extent_group_context] error, can not find context, extent_group_id:" << extent_group_id;
        sstring error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
	  }
    logger.debug("[{}] done, extent_group_id:{}", __func__, extent_group_id);
    auto extent_group_context = std::dynamic_pointer_cast<extent_group_context_entry>(itor->second);
    return *extent_group_context;
}

future<sstring> context_service::pull_extent_group_context(sstring owner_id, std::set<sstring> extent_group_ids) {
    logger.debug("[{}] start, extent_group_ids:", build_extent_group_ids_str(extent_group_ids));
    if(extent_group_ids.size() == 0){
        logger.warn("[{}] do nothing, because extent_group_ids.size()=0", __func__); 
        return make_ready_future<sstring>("[]");
    }

    return get_or_pull_volume_context(owner_id).then([this, extent_group_ids](auto volume_context) mutable {
        sstring container_name = volume_context.get_container_name();
           
        sstring server_url = _config->pithos_server_url();
        sstring uri = server_url + "/v1/actions/resolve_extent_group_info";
        sstring request_body = this->build_request_body_for_extent_group(extent_group_ids, container_name);
        logger.debug("[pull_extent_group_context] uri:{}, request_body:{}", uri, request_body);

        return this->do_pull_context(uri, request_body);
    });
}


future<> context_service::save_extent_group_context(sstring contexts_json) {
    logger.debug("[{}] start, contexts_json:{}", __func__, contexts_json);
    std::vector<future<>> futures;
    std::string err;
    auto contexts = hive::Json::parse(contexts_json, err);
    for(auto& context_json: contexts["extent_group_contexts"].array_items()) {
        auto context_ptr = parse_extent_group_context_json(context_json);

        auto extent_group_context_ptr = std::dynamic_pointer_cast<extent_group_context_entry>(context_ptr);
        auto extent_group_id = extent_group_context_ptr->get_extent_group_id();
        auto& local_context_service = hive::get_local_context_service();
        auto fut = local_context_service.set_all_shard(context_type::EXTENT_GROUP_CONTEXT, extent_group_id, context_ptr);
        futures.push_back(std::move(fut));
    }

    return when_all(futures.begin(), futures.end()).then([contexts_json](auto futs){
        try{
            for(auto& fut : futs){
                fut.get(); 
            }
            logger.debug("[save_extent_group_context done] contexts_json:{}", contexts_json); 
            return make_ready_future<>();
        }catch(...){
            throw std::runtime_error(sprint("error,save_extent_group_context failed, contexts_json:%s", contexts_json)); 
        }
    });
}

sstring context_service::build_extent_group_ids_str(std::set<sstring> extent_group_ids){
    sstring str_ids = "";
    int count = 0;
    for(auto id : extent_group_ids){
        str_ids += (0==count++) ? id : ("," + id);
    }
    return str_ids;
}

sstring context_service::build_request_body_for_extent_group(std::set<sstring>& extent_group_ids, sstring container_name) {

    sstring build_str = "{\"extent_group_ids\":[";
    int count = 0;
    for(auto extent_group_id : extent_group_ids) {
        if( 0 == count) {
            build_str += "\"" + extent_group_id + "\"";
        } else {
            build_str += ",\"" + extent_group_id + "\"";
        }
        ++count;
    }

    build_str += "],";
    sstring body_str =  build_str + "\"container_name\":\"" + container_name + "\"}";
    std::string err;
    auto post_json = hive::Json::parse(body_str, err);
    auto json_body = post_json.dump();
    return json_body;
}

std::shared_ptr<context_entry> 
context_service::parse_extent_group_context_json(hive::Json context_json) {
    sstring extent_group_id = context_json["extent_group_id"].string_value();
    sstring disk_ids        = context_json["replicas"].string_value();
    uint64_t vclock         = context_json["vclock"].uint64_value();

    if(extent_group_id.empty() || disk_ids.empty()){
        std::ostringstream out;
        out << "[parse_extent_group_context_json] error context params maybe empty";
        out << ", extent_group_id:" << extent_group_id;
        out << ", disk_ids:" << disk_ids;
        out << ", vclock:" << vclock;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }

    auto context_ptr = std::make_shared<extent_group_context_entry>(extent_group_id, disk_ids, vclock);
    return context_ptr;
}

future<> context_service::set_extent_group_context(extent_group_context_entry context_entry){
    auto extent_group_id = context_entry.get_extent_group_id();
    auto context_ptr = std::make_shared<extent_group_context_entry>(context_entry);
    return set_all_shard(context_type::EXTENT_GROUP_CONTEXT, extent_group_id, context_ptr);
}

future<> context_service::update_extent_group_context(sstring extent_group_id
                                                    , sstring disk_ids
                                                    , int64_t vclock){
    auto context_ptr = std::make_shared<extent_group_context_entry>(extent_group_id, disk_ids, vclock);
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.set_all_shard(context_type::EXTENT_GROUP_CONTEXT, extent_group_id, context_ptr);
}
// ==============================================================
// common
// ==============================================================
future<sstring> context_service::do_pull_context(sstring uri, sstring json_body) {
    hive::HttpClient client;
    bool retry = true;
    int64_t retry_count = 0;
    sstring contexts_json = "";
    
    return do_with(std::move(client), std::move(retry), std::move(retry_count), std::move(contexts_json), 
            [this, uri, json_body](auto& client, auto& retry, auto& retry_count, auto& contexts_json) {
        return do_until([&retry](){return !retry;},[this, uri, json_body, &client, &retry, &retry_count, &contexts_json]() mutable {
            return client.post(uri, json_body).then_wrapped([this, uri, json_body, &retry, &retry_count, &contexts_json]
                    (auto f) mutable {
                try {
                    auto response = f.get0();
                    if(!response.is_success()){
                        auto error_info = sprint("http response error code:", response.status());
                        throw std::runtime_error(error_info); 
                    }
                    retry = false;
                    contexts_json = response.data();
                    return make_ready_future<>();
                }catch (...) {
                    if (retry_count++ < int64_t(_config->pull_context_retry_times())) {
                        retry = true;
                        auto retry_interval_in_ms = int64_t(_config->pull_context_retry_interval_in_ms());
                        return sleep(std::chrono::milliseconds(retry_interval_in_ms));
                    }else {
                        retry = false;
                        std::ostringstream out;
                        out << "error, do_pull_context failed, retry_count:" << retry_count;
                        out << ", uri:" << uri;
                        out << ", body:" << json_body;
                        out << ", exception:" << std::current_exception();
                        sstring error_info = out.str();
                        logger.error(error_info.c_str());
                        return make_exception_future<>(std::runtime_error(error_info));
                    }
                }
            });
        }).then_wrapped([&contexts_json](auto f){
            try{
                f.get();
                return make_ready_future<sstring>(contexts_json);
            } catch(...) {
                return make_exception_future<sstring>(std::current_exception());
            }
        });
    });
}

} // namespace hive
