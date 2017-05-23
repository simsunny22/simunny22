#include "hive/context/inject_context_helper.hh"
#include "seastar/core/distributed.hh"
#include "seastar/core/print.hh"
#include "seastar/core/thread.hh"
#include "log.hh"

#include "hive/hive_tools.hh"
#include "hive/context/context_service.hh"

namespace hive {

static logging::logger  logger("inject_context_helper");

inject_context_helper::inject_context_helper(){

}

inject_context_helper::~inject_context_helper(){

}

future<> inject_context_helper::inject_context_by_config(lw_shared_ptr<db::config> config){
    std::vector<future<>> futures;
    auto fut_disk = inject_disk_context_by_config(config);
    futures.push_back(std::move(fut_disk));

    auto fut_volume = inject_volume_context_by_config(config);
    futures.push_back(std::move(fut_volume));

    auto fut_group = inject_extent_group_context_by_config(config);
    futures.push_back(std::move(fut_group));

    return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> ret){
        try {
            for(auto& f : ret){
                f.get();
            } 
            return make_ready_future<>();
        }catch (...){
            std::ostringstream out;
            out << "[inject_context_by_config] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
} 

future<> inject_context_helper::inject_context(sstring disks
                                             , uint32_t volume_count
                                             , sstring volume_driver_node_ip
                                             , sstring volume_replica_node_ips
                                             , uint32_t extent_group_count_per_volume
                                             , sstring extent_group_target_disk_ids){
    std::vector<future<>> futures;
    auto fut_disk = inject_disk_context(disks);
    futures.push_back(std::move(fut_disk));

    auto fut_volume = inject_volume_context(volume_count, volume_driver_node_ip, volume_replica_node_ips);
    futures.push_back(std::move(fut_volume));

    auto fut_group = inject_extent_group_context(volume_count, extent_group_count_per_volume, extent_group_target_disk_ids);
    futures.push_back(std::move(fut_group));

    return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> ret){
        try {
            for(auto& f : ret){
                f.get();
            } 
            return make_ready_future<>();
        }catch (...){
            std::ostringstream out;
            out << "[inject_context_by_config] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

//////////////////////////
// disk context
/////////////////////////
future<> inject_context_helper::inject_disk_context_by_config(lw_shared_ptr<db::config> config){
    auto ctx_disks = config->ctx_disks();
    logger.debug("[{}] start, ctx_disks:{}", __func__, ctx_disks);
    return inject_disk_context(ctx_disks);
}

future<> inject_context_helper::inject_disk_context(sstring disks){
    auto disk_context_json = build_context_json_for_disk(disks);
    logger.debug("[{}] disk_context_json:{}", __func__, disk_context_json);
    auto& context_service = hive::get_local_context_service();
    return context_service.save_disk_context(disk_context_json);
}

sstring inject_context_helper::build_context_json_for_disk(sstring ctx_disks){
    auto vt_disk_contexts = hive_tools::split_to_vector(ctx_disks, ":");
    sstring ret_json = "{\"disk_entries\":[";
  
    int32_t count = 0;
    for(auto disk_context : vt_disk_contexts ){
        auto vt_context_unit = hive_tools::split_to_vector(disk_context, ","); 
        if(4 != vt_context_unit.size()){
            throw std::runtime_error("disk_contexts config error"); 
        }

        sstring disk_id = vt_context_unit.at(0);
        sstring disk_ip = vt_context_unit.at(1); 
        sstring disk_mount_path = vt_context_unit.at(2);
        sstring disk_type = vt_context_unit.at(3);
        (0==count++) ? (ret_json += "{") : (ret_json += ",{");
        ret_json += "\"id\":\"" + disk_id + "\"";
        ret_json += ",\"cvm_ip_address\":\"" + disk_ip + "\"";
        ret_json += ",\"mount_path\":\"" + disk_mount_path + "\"";
        ret_json += ",\"storage_tier_name\":\"" + disk_type + "\"";
        ret_json += "}";
    }
      
    ret_json += "]}";
    return ret_json;
}


//////////////////////////
// volume context
/////////////////////////
future<> inject_context_helper::inject_volume_context_by_config(lw_shared_ptr<db::config> config){
    auto ctx_volume_count = config->ctx_volume_count();
    auto ctx_volume_driver_node_ip = config->ctx_volume_driver_node_ip();
    auto ctx_volume_replica_node_ips = config->ctx_volume_replica_node_ips();

    logger.debug("[{}] start, ctx_volume_count:{}, ctx_volume_driver_node_ip:{}, ctx_volume_replica_node_ips:{}"
        , __func__, ctx_volume_count, ctx_volume_driver_node_ip, ctx_volume_replica_node_ips);

    return inject_volume_context(ctx_volume_count, ctx_volume_driver_node_ip, ctx_volume_replica_node_ips);
}

future<> inject_context_helper::inject_volume_context(uint32_t volume_count, sstring volume_driver_node_ip, sstring volume_replica_node_ips){
    auto volume_context_json = build_context_json_for_volume(volume_count
                                                           , volume_driver_node_ip
                                                           , volume_replica_node_ips);
    logger.debug("[{}] volume_context_json:{}", __func__, volume_context_json);
    auto& context_service = hive::get_local_context_service();
    return context_service.save_volume_context(volume_context_json);
}

sstring inject_context_helper::build_context_json_for_volume(uint32_t volume_count
                                                           , sstring dirver_node_ip
                                                           , sstring replica_node_ips){
    auto vt_replica_ips = hive_tools::split_to_vector(replica_node_ips, ":");
    sstring ret_json = "{\"volume_contexts\":[";
    
    for(uint32_t i = 0; i < volume_count; i++){
        sstring volume_id = "volume_" + to_sstring(i+1);
        (0==i)? (ret_json += "{") : (ret_json += ",{");
        ret_json += "\"volume_uuid\":\"" + volume_id + "\"";
        ret_json += ", \"vclock\":0";
        ret_json += ", \"cluster_uuid\":\"default_cluster_uuid\"";
        ret_json += ", \"storage_pool_uuid\":\"default_storage_pool_uuid\"";
        ret_json += ", \"container_uuid\":\"default_container_uuid\"";
        ret_json += ", \"container_name\":\"default_container_name\"";
        ret_json += ", \"last_extent_group_id\":\"\"";
        ret_json += ", \"volume_driver_node\":{\"node_id\":\"cee0078e-630d-11e4-b03b-70e2840bafc8\", \"node_ip\":\"" + dirver_node_ip + "\"}";
        ret_json += ", \"node_map\":[";

        int32_t replica_count = 0;
        for(auto replica_ip : vt_replica_ips){ //replica node info
            (0==replica_count++) ? (ret_json += "{") : (ret_json +=",{");
            ret_json += "\"node_id\":\"default_replica_node_id\"";
            ret_json += ", \"node_ip\":\"" + replica_ip + "\"";
            ret_json += "}";
        }
        
        ret_json += "]}";
    }

    ret_json += "]}";
    return ret_json;
}


//////////////////////////
// extent group context
/////////////////////////
future<> inject_context_helper::inject_extent_group_context_by_config(lw_shared_ptr<db::config> config){
    auto ctx_volume_count = config->ctx_volume_count();
    auto ctx_extent_group_count_per_volume = config->ctx_extent_group_count_per_volume();
    auto ctx_extent_group_target_disk_ids = config->ctx_extent_group_target_disk_ids();

    logger.debug("[{}] start, ctx_volume_count:{}, ctx_extent_group_count_per_volume:{}, ctx_extent_group_target_disk_ids:{}"
        , __func__, ctx_volume_count, ctx_extent_group_count_per_volume, ctx_extent_group_target_disk_ids);
    
    return inject_extent_group_context(ctx_volume_count
                              , ctx_extent_group_count_per_volume
                              , ctx_extent_group_target_disk_ids);
}

future<> inject_context_helper::inject_extent_group_context(uint32_t volume_count
                                                   , uint32_t extent_group_count_per_volume
                                                   , sstring extent_group_target_disk_ids){
    auto extent_group_context_json = build_context_json_for_extent_group(volume_count
                                                                       , extent_group_count_per_volume
                                                                       , extent_group_target_disk_ids);
    logger.debug("[{}] extent_group_context_json:{}", __func__, extent_group_context_json);
    auto& context_service = hive::get_local_context_service();
    return context_service.save_extent_group_context(extent_group_context_json);
}

sstring inject_context_helper::build_context_json_for_extent_group(uint32_t volume_count
                                              , uint32_t extent_group_count_per_volume
                                              , sstring replica_disk_ids){
    sstring ret_json = "{\"extent_group_contexts\":[";
    uint32_t count = 0;
    for(uint32_t i= 0; i < volume_count; i++){
        sstring volume_id = "volume_" + to_sstring(i+1); 
        for(uint32_t j = 0; j < extent_group_count_per_volume; j++){
            uint32_t group_id_num = j+1;
            uint32_t dir_num = group_id_num % 10;
            sstring sub_dir_name = sprint("%s%s", dir_num, dir_num);
            sstring extent_group_id = volume_id + "_extent_group_" + to_sstring(group_id_num) + "_" + sub_dir_name;
            
            (0==count++) ? (ret_json += "{") : (ret_json += ",{");
            ret_json += "\"volume_id\":\"" + volume_id + "\"";
            ret_json += ", \"extent_group_id\":\"" + extent_group_id + "\"";
            ret_json += ", \"replicas\":\"" + replica_disk_ids+ "\"";
            ret_json += ", \"vclock\":0";
            ret_json += "}";
        }
    }
      
    ret_json += "]}"; 
    return ret_json;
}


} //namespace hive
