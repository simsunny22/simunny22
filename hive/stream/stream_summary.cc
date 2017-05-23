#include "hive/stream/stream_summary.hh"
#include "types.hh"
#include "utils/serialization.hh"

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
        default :
            return "UNKNOWN MIGRATE SCENE TYPE";
    }
}

std::ostream& operator<<(std::ostream& os, const stream_summary& summary) {
    if(migrate_type::MIGRATE_EXTENT_GROUP == summary.type) {
        os << "[ extent_group_id =" << summary.extent_group_id; 
        os << ", dst_disk_id = " << summary.dst_disk_id;
        os << ", total_size = " << summary.total_size;
        os<< "]";
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == summary.type){
        os << "[ migrate_scene = " << get_scene_str(summary.scene); 
        os << ", commitlog_file_name =" << summary.commitlog_file_name; 
        os << ", total_size =" << summary.total_size; 
        os<< "]";
    }else{
        os << "error, unknown migrate type";
    }
    return os;
}

} // namespace hive 
