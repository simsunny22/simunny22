#include "hive/stream/migrate_chunk.hh"
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


std::ostream& operator<<(std::ostream& os, const migrate_chunk& chunk) {
    if(migrate_type::MIGRATE_EXTENT_GROUP == chunk.type){
        os << "[type = MIGRATE_EXTENT_GROUP";
        os << ", volume_id = " << chunk.volume_id;
        os << ", extent_group_id = " << chunk.extent_group_id;
        os << ", offset = " << chunk.offset;
        os << ", length = " << chunk.length;
        os << ", src_disk_id = " << chunk.src_disk_id;
        os << ", dst_disk_id = " << chunk.dst_disk_id;
        os << "]";
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == chunk.type){
        os << "[type = MIGRATE_EXTENT_JOURNAL";
        os << ", scene = " << get_scene_str(chunk.scene);
        os << ", volume_id = " << chunk.volume_id;
        os << ", offset = " << chunk.offset;
        os << ", length = " << chunk.length;
        os << ", commitlog_file_name = " << chunk.commitlog_file_name;
        os << "]";
    }else{
        os << "error, unknown migrate type";
    }
    
    return os;
}

}//namespace hive
