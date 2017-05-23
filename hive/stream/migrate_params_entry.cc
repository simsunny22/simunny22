#include "hive/stream/migrate_params_entry.hh"
#include <ostream>
#include <seastar/core/sstring.hh>

namespace hive {

static sstring get_scene_str(migrate_scene scene){
    switch(scene){
        case migrate_scene::PRIMARY_TO_PRIMARY:
            return "PRIMARY_TO_PRIMARY";
        case migrate_scene::PRIMARY_TO_SECONDARY:
            return "PRIMARY_TO_SECONDARY";
        case migrate_scene::SECONDARY_TO_SECONDARY:
            return "SECONDARY_TO_SECONDARY";
        case migrate_scene::SECONDARY_TO_PRIMARY:
            return "SECONDARY_TO_PRIMARY";
        default:
            return "UNKNOWN SCENE TYPE";
    }
}

std::ostream& operator<<(std::ostream& os, const migrate_params_entry& params) {
    if(migrate_type::MIGRATE_EXTENT_GROUP == params.type){
        os << "[";
        os << " type = " << "MIGRATE_EXTENT_GROUP"; 
        os << ", driver_node_ip = " << params.driver_node_ip;
        os << ", container_name = " << params.container_name;
        os << ", intent_id = "      << params.intent_id;
        os << ", volume_id = "      << params.volume_id;
        os << ", extent_group_id = "<< params.extent_group_id;
        os << ", offset = "         << params.offset;
        os << ", length = "         << params.length;
        os << ", src_disk_id = "    << params.src_disk_id;
        os << ", dst_disk_id = "    << params.dst_disk_id;
        os << "]";
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == params.type){
        os << "[";
        os << " type = " << "MIGRATE_EXTENT_JOURNAL"; 
        os << " scene = " << get_scene_str(params.scene); 
        os << ", src_node_ip = " << params.src_node_ip;
        os << ", dst_node_ip = " << params.dst_node_ip;
        os << ", commitlog_file_name = " << params.commitlog_file_name;
        os << ", commitlog_file_path = " << params.commitlog_file_path;
        os << ", offset = " << params.offset;
        os << ", length = " << params.length;
        os << "]";
    }else if(migrate_type::REPLICATE_EXTENT_GROUP == params.type){
        os << "[";
        os << " type = " << "REPLICATE_EXTENT_GROUP"; 
        os << ", volume_id = "      << params.volume_id;
        os << ", extent_group_id = "<< params.extent_group_id;
        os << ", src_disk_id = "    << params.src_disk_id;
        os << ", dst_disk_id = "    << params.dst_disk_id;
        os << "]";     
    }else{
        os << "unknown migrate type:";
    }
    
    return os;
}

}//namespace hive
