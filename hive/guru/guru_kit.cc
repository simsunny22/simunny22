#include "guru_kit.hh"
#include "log.hh"
#include "seastar/core/distributed.hh"
#include "seastar/core/print.hh"
#include "seastar/core/thread.hh"
#include "seastar/core/sleep.hh"

#include "hive/hive_tools.hh"
#include "hive/hive_service.hh"
namespace hive {

static logging::logger logger("guru_kit");

guru_kit::guru_kit() {
}

guru_kit::~guru_kit() {
}              

future<write_plan> guru_kit::get_write_plan(sstring server_url, sstring volume_id, uint64_t offset, uint64_t length) {
    sstring uri = server_url + "/v1/actions/get_write_volume_plan";
    sstring json_body = build_request_body_for_get_write_plan(volume_id, offset, length);
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto plan = this->parse_write_plan(response_body); 
        return make_ready_future<write_plan>(std::move(plan));
    });
}

future<> guru_kit::commit_write_plan(sstring server_url, write_plan wplan) {
    sstring uri = server_url + "/v1/actions/get_write_plan";
    sstring json_body = build_request_body_for_commit_write_plan(wplan);
    return request_from_guru(uri, json_body).then([](auto result){
        return make_ready_future<>();
    });
}

future<> guru_kit::commit_create_group(sstring server_url, std::vector<create_action> create_actions) {
    sstring uri = server_url + "/v1/actions/get_write_plan";
    sstring json_body = build_request_body_for_commit_create_group(create_actions);
    return request_from_guru(uri, json_body).then([](auto result){
        return make_ready_future<>();
    });
}

future<read_plan> guru_kit::get_read_plan(sstring server_url, sstring volume_id, uint64_t offset, uint64_t length) {
    sstring uri = server_url + "/v1/actions/get_read_volume_plan";
    sstring json_body = build_request_body_for_get_read_plan(volume_id, offset, length);
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto plan = this->parse_read_plan(response_body); 
        return make_ready_future<read_plan>(std::move(plan));
    });
}

future<volume_entity> guru_kit::get_volume_entity(sstring server_url, sstring volume_id) {
    auto config = hive::get_local_hive_service().get_hive_config();
    //if(config->auto_inject_context_when_start()){
    if(config->auto_test_hive()){
       return get_volume_entity_for_test(volume_id);
    }
    sstring uri = server_url + "/v1/actions/volumes/"+volume_id;
    sstring json_body = "";
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto entity = this->parse_volume_entity(response_body); 
        return make_ready_future<volume_entity>(std::move(entity));
    });
}

future<object_entity> guru_kit::get_object_entity(sstring server_url, sstring object_id) {
    auto config = hive::get_local_hive_service().get_hive_config();
    sstring uri = server_url + "/v1/actions/objects/"+object_id;
    sstring json_body = "";
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto entity = this->parse_object_entity(response_body); 
        return make_ready_future<object_entity>(std::move(entity));
    });
}


future<volume_entity> guru_kit::get_volume_entity_for_test(sstring volume_id) {
    auto config = hive::get_local_hive_service().get_hive_config();
    sstring primary_journal_node = config->ctx_volume_driver_node_ip();
    sstring cluster_uuid = "default";
    sstring cluster_name = "default";
    sstring storage_pool_uuid = "default";
    sstring container_name = "default";
    sstring vclock = "1";
    sstring last_extent_group = "";
    sstring marked_for_removal = "false";
    int extent_group_count = config->ctx_extent_group_count_per_volume();
    uint64_t max_capacity_bytes = extent_group_count * 4*1024*1024;
    sstring parent_volume = "";
    sstring snapshot = "false";
   
    sstring volume_string = "{\"volume_uuid\":\""
                          + volume_id
                          + "\", \"primary_journal_node\":\""
                          + primary_journal_node
                          + "\",\"vclock\":"
                          + vclock
                          + ", \"cluster_uuid\":\""
                          + cluster_uuid
                          + "\", \"storage_pool_uuid\":\""
                          + storage_pool_uuid
                          + "\", \"container_name\":\""
                          + container_name
                          + "\", \"container_uuid\":\""
                          + cluster_uuid
                          + "\", \"last_extent_group\":\""
                          + last_extent_group
                          + "\", \"marked_for_removal\":"
                          + marked_for_removal
                          + ", \"max_capacity_bytes\":"
                          + to_sstring(max_capacity_bytes)
                          + ",\"name\":\""
                          + volume_id
                          + "\", \"parent_volume\":\""
                          + parent_volume
                          + "\", \"snapshot\":"
                          + snapshot
                          + "}";
    auto entity = this->parse_volume_entity(volume_string); 
    return make_ready_future<volume_entity>(std::move(entity));
}


future<std::vector<sstring>> guru_kit::allocate_write_disks(sstring server_url, sstring container_uuid) {
    auto config = hive::get_local_hive_service().get_hive_config();

    if(config->auto_test_hive()){
    //if(config->auto_inject_context_when_start()){
       return allocate_write_disks_for_test(container_uuid);
    }
    sstring uri = server_url + "/v1/actions/allocate_disks";
    sstring json_body = build_request_body_for_allocate_disks(container_uuid);
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto disks = this->parse_allocate_disks(response_body); 
        return make_ready_future<std::vector<sstring>>(disks);
    });
}

future<sstring> guru_kit::allocate_disk_for_extent_group(sstring server_url, sstring volume_id, sstring extent_group_id, std::vector<sstring> disk_ids, sstring error_disk_id) {
    auto config = hive::get_local_hive_service().get_hive_config();
    //if(config->auto_inject_context_when_start()){
    if(config->auto_test_hive()){
       return allocate_disk_for_extent_group_for_test(disk_ids, error_disk_id);
    }
 
    sstring uri = server_url + "/v1/actions/allocate_disk_for_extent_group";
    sstring json_body = build_request_body_for_allocate_disk_for_extent_group(volume_id, extent_group_id, disk_ids, error_disk_id);
    return request_from_guru(uri, json_body).then([this](auto response_body){
        auto disk_id = this->parse_allocate_disk_for_extent_group(response_body); 
        return make_ready_future<sstring>(disk_id);
    });

}

future<std::vector<sstring>> guru_kit::allocate_write_disks_for_test(sstring volume_id) {
    auto config = hive::get_local_hive_service().get_hive_config();
    sstring disk_ids = config->ctx_extent_group_target_disk_ids();
    std::vector<sstring> disks = hive_tools::split_to_vector(disk_ids, ":");
    return make_ready_future<std::vector<sstring>>(disks);
}

future<sstring> guru_kit::allocate_disk_for_extent_group_for_test(std::vector<sstring> disk_ids_, sstring error_disk_id_) {
    auto config = hive::get_local_hive_service().get_hive_config();
    sstring disk_ids = config->ctx_extent_group_target_disk_ids();
    std::vector<sstring> disks = hive_tools::split_to_vector(disk_ids, ":");
    return make_ready_future<sstring>(disks[0]);
}


sstring guru_kit::build_request_body_for_get_write_plan(sstring volume_id, uint64_t offset, uint64_t length) {
    hive::Json json = hive::Json::object {
        {"volume_id", volume_id.c_str()},
        {"offset",    to_sstring(offset).c_str()},
        {"length",    to_sstring(length).c_str()},
    };
    auto body = json.dump();
    return body;
}


sstring guru_kit::build_request_body_for_get_read_plan(sstring volume_id, uint64_t offset, uint64_t length) {
    hive::Json json = hive::Json::object {
        {"volume_id", volume_id.c_str()},
        {"offset",    to_sstring(offset).c_str()},
        {"length",    to_sstring(length).c_str()},
    };
    auto body = json.dump();
    return body;
}


sstring guru_kit::build_request_body_for_commit_write_plan(write_plan wplan) {
    //hive::Json json = hive::Json::object {
    //    {"volume_id", volume_id.c_str()},
    //    {"offset",    to_sstring(offset).c_str()},
    //    {"length",    to_sstring(length).c_str()},
    //};
    //auto body = json.dump();
    //return body;
    return "";
}


sstring guru_kit::build_request_body_for_commit_create_group(std::vector<create_action> create_actions) {
    //hive::Json json = hive::Json::object {
    //    {"volume_id", volume_id.c_str()},
    //    {"offset",    to_sstring(offset).c_str()},
    //    {"length",    to_sstring(length).c_str()},
    //};
    //auto body = json.dump();
    //return body;
    return "";
}

sstring guru_kit::build_request_body_for_get_volume_entity(sstring volume_id) {
    hive::Json json = hive::Json::object {
        {"volume_id", volume_id.c_str()}
    };
    auto body = json.dump();
    return body;
}

sstring guru_kit::build_request_body_for_allocate_disks(sstring container_uuid) {
    hive::Json json = hive::Json::object {
        {"container_uuid", container_uuid.c_str()},
    };
    auto body = json.dump();
    return body;
}

sstring guru_kit::build_request_body_for_allocate_disk_for_extent_group(sstring volume_id, sstring extent_group_id, std::vector<sstring> disks, sstring error_disk_id) {
    hive::Json json = hive::Json::object {
        {"disks", disks},
        {"volume_id", volume_id.c_str()},
        {"extent_group_id", extent_group_id.c_str()},
        {"error_disk_id", error_disk_id.c_str()},
    };
    auto body = json.dump();
    return body;
}


write_plan guru_kit::parse_write_plan(sstring wplan_string) {
    std::string err;
    auto write_plan_json = hive::Json::parse(wplan_string, err);
    write_plan wplan;

    sstring owner_id = write_plan_json["owner_id"].string_value();
    wplan.owner_id   = owner_id;

    //create_actions
    std::vector<create_action> create_actions;
    for(auto& c_action : write_plan_json["create_actions"].array_items()) {
        sstring extent_group_id = c_action["extent_group_id"].string_value();
        sstring disk_ids        = c_action["disks"].string_value();
        create_action action;
        action.extent_group_id = extent_group_id;
        action.disk_ids        = disk_ids;
        create_actions.push_back(action);
    }

    //write_actions
    std::vector<write_action> write_actions;
    for(auto& w_action : write_plan_json["write_actions"].array_items()) {
        sstring extent_group_id = w_action["extent_group_id"].string_value();
        sstring extent_id       = w_action["extent_id"].string_value();
        uint64_t extent_offset_in_group = w_action["extent_offset_in_group"].uint64_value();
        uint64_t data_offset_in_extent  = w_action["data_offset_in_extent"].uint64_value();
        uint64_t length                 = w_action["length"].uint64_value();
        uint64_t data_offset            = w_action["data_offset"].uint64_value();

        write_action action;
        action.extent_offset_in_group = extent_offset_in_group;
        action.extent_id = extent_id;
        action.extent_group_id = extent_group_id;
        action.data_offset_in_extent  = data_offset_in_extent;
        action.length = length;
        action.data_offset = data_offset;
        write_actions.push_back(action);
    }
    
    wplan.create_actions = create_actions;
    wplan.write_actions  = write_actions;
    return std::move(wplan);
}


read_plan guru_kit::parse_read_plan(sstring rplan_string) {
    std::string err;
    auto read_plan_json = hive::Json::parse(rplan_string, err);

    read_plan rplan;

    sstring owner_id = read_plan_json["owner_id"].string_value();
    rplan.owner_id   = owner_id;

    //read_actions
    std::vector<read_action> read_actions;
    for(auto& r_action : read_plan_json["read_actions"].array_items()) {
        sstring disks           = r_action["disks"].string_value();
        sstring extent_group_id = r_action["extent_group_id"].string_value();
        sstring extent_id       = r_action["extent_id"].string_value();
        sstring md5             = r_action["md5"].string_value();
        uint64_t extent_offset_in_group = r_action["extent_offset_in_group"].uint64_value();
        uint64_t data_offset_in_extent  = r_action["data_offset_in_extent"].uint64_value();
        uint64_t length                 = r_action["length"].uint64_value();

        read_action action;
        action.extent_offset_in_group = extent_offset_in_group;
        action.extent_id              = extent_id;
        action.disk_ids               = disks;
        action.md5                    = md5;
        action.extent_group_id        = extent_group_id;
        action.data_offset_in_extent  = data_offset_in_extent;
        action.length                 = length;
        read_actions.push_back(action);
    }
    
    rplan.read_actions = read_actions;

    return rplan;
}

volume_entity guru_kit::parse_volume_entity(sstring volume_string) {
    std::string err;
    auto volume_json = hive::Json::parse(volume_string, err);

    volume_entity entity;

    entity._volume_id            = volume_json["volume_uuid"].string_value();
    entity._driver_node_id       = volume_json["primary_journal_node"].string_value();
    entity._vclock               = volume_json["vclock"].uint64_value();
    entity._cluster_uuid         = volume_json["cluster_uuid"].string_value();
    entity._storage_pool_uuid    = volume_json["storage_pool_uuid"].string_value();
    entity._container_name       = volume_json["container_name"].string_value();
    entity._container_uuid       = volume_json["container_uuid"].string_value();
    entity._last_extent_group_id = volume_json["last_extent_group"].string_value();
    entity._marked_for_removal   = volume_json["marked_for_removal"].bool_value();
    entity._max_capacity_bytes   = volume_json["max_capacity_bytes"].uint64_value();
    entity._name                 = volume_json["name"].string_value();
    //entity._on_disk_dedup        = volume_json["on_disk_dedup"].bool_value();
    entity._parent_volume        = volume_json["parent_volume"].string_value();
    entity._snapshot             = volume_json["snapshot"].bool_value();

    // journal nodes
    std::vector<sstring> journal_nodes;
    //for(auto& journal_node : volume_json["journal_nodes"].array_items()) {
    //    journal_nodes.push_back(journal_node.string_value());
    //}

    // snapshots 
    std::vector<sstring> snapshots;
    //for(auto& snapshot: volume_json["snapshot"].array_items()) {
    //    snapshots.push_back(snapshot.string_value());
    //}

    entity._journal_nodes = journal_nodes;
    entity._snapshots = snapshots;

    return entity;
}


object_entity guru_kit::parse_object_entity(sstring object_string) {
    std::string err;
    auto object_json = hive::Json::parse(object_string, err);

    object_entity entity;

    entity._object_id            = object_json["object_uuid"].string_value();
    entity._vclock               = object_json["vclock"].uint64_value();
    entity._cluster_uuid         = object_json["cluster_uuid"].string_value();
    entity._storage_pool_name    = object_json["storage_pool_name"].string_value();
    entity._storage_pool_uuid    = object_json["storage_pool_uuid"].string_value();
    entity._container_name       = object_json["container_name"].string_value();
    entity._container_uuid       = object_json["container_uuid"].string_value();
    entity._name                 = object_json["name"].string_value();
    entity._size                 = object_json["size"].uint64_value();
    entity._vclock               = object_json["vclock"].uint64_value();
    entity._marked_for_removal   = object_json["marked_for_removal"].bool_value();

    return entity;
}



std::vector<sstring> guru_kit::parse_allocate_disks(sstring body_string) {
    std::string err;
    auto disks_json = hive::Json::parse(body_string, err);

    std::vector<sstring> disks;
    for(auto& disk: disks_json["disks"].array_items()) {
        disks.push_back(disk.string_value());
    }

    return disks;
}


sstring guru_kit::parse_allocate_disk_for_extent_group(sstring body_string) {
    std::string err;
    auto disk_json = hive::Json::parse(body_string, err);
    auto disk_id = disk_json["disk_id"].string_value();

    return disk_id;
}



// ==============================================================
// common
// ==============================================================
future<sstring> guru_kit::request_from_guru(sstring uri, sstring json_body) {
    hive::HttpClient client;
    bool retry = true;
    int64_t retry_count = 0;
    sstring contexts_json = "";
    
    return do_with(std::move(client), std::move(retry), std::move(retry_count), std::move(contexts_json), 
            [this, uri, json_body](auto& client, auto& retry, auto& retry_count, auto& contexts_json) {
        return do_until([&retry](){return !retry;},[this, uri, json_body, &client, &retry, &retry_count, &contexts_json]() mutable {
            return client.post(uri, json_body).then_wrapped([this, uri, json_body, &retry, &retry_count, &contexts_json]
                    (auto f) mutable {

                int64_t config_retry_count = 5;
                int64_t config_retry_interval = 5;
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
                    if (retry_count++ < config_retry_count) {
                        retry = true;
                        auto retry_interval_in_ms = config_retry_interval;
                        return sleep(std::chrono::milliseconds(retry_interval_in_ms));
                    }else {
                        retry = false;
                        std::ostringstream out;
                        out << "error, request_guru failed, retry_count:" << retry_count;
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
