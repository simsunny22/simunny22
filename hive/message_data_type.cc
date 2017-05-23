#include "hive/message_data_type.hh"
#include "hive/hive_tools.hh"

#include "bytes.hh"
#include "types.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const smd_init_segment& data) {
    return out << "{smd_init_segment:["
               << "volume_id:" << data.volume_id 
               << ", commit_log_id:" << data.segment_id 
               << ", size:" << data.size
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_sync_segment& data) {
    return out << "{smd_sync_segment:["
               << "volume_id:" << data.volume_id 
               << ", commit_log_id:" << data.segment_id 
               << ", offset:" << data.offset_in_segment 
               << ", data_length:" << data.data_length
               << ", serialize_length:" << data.serialize_length
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_discard_segment& data) {
    return out << "{smd_discard_segment:["
               << "volume_id:" << data.volume_id 
               << ", commit_log_id:" << data.segment_id 
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_get_journal_data& data) {
    return out << "{smd_get_journal_data:["
               << "volume_id:" << data.volume_id 
               << ", segment_id:" << data.segment_id
               << ", shard:" << data.shard
               << ", revisions.size:" << data.revisions.size()
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const rmd_get_journal_data& data) {
    return out << "{rmd_get_journal_data:["
               << "volume_id:" << data.volume_id 
               << ", segment_id:" << data.segment_id
               << ", revision_datas.size:" << data.revision_datas.size()
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_create_extent_group& data) {
    return out << "{smd_create_extent_group:["
               << "extent_group_id:" << data.extent_group_id
               << ", disk_ids:" << hive_tools::format(data.disk_ids)
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_delete_extent_group& data) {
    return out << "{smd_delete_extent_group:["
               << "extent_group_id:" << data.extent_group_id
               << ", disk_ids:" << hive_tools::format(data.disk_ids) 
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_rwrite_extent_group& data) {
    return out << "{smd_rwrite_extent_group:["
               << "extent_group_id:" << data.extent_group_id
               << ", disk_ids:" << hive_tools::format(data.disk_ids)
               << ", revisons.size:" << data.revisions.size()
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_read_extent_group& data) {
    return out << "{smd_read_extent_group:["
               << "owner_id:" << data.owner_id
               << ", extent_group_id:" << data.extent_group_id
               << ", extent_id:" << data.extent_id
               << ", extent_offset_in_group:" << data.extent_offset_in_group
               << ", data_offset_in_extent:" << data.data_offset_in_extent
               << ", length:" << data.length
               << ", disk_id:" << data.disk_id
               << ", only_need_digest:" << data.only_need_digest
               << "]}";

}

std::ostream& operator<<(std::ostream& out, const rmd_read_extent_group& data) {
    return out << "{rmd_read_extent_group:["
               << "owner_id:" << data.owner_id
               << ", extent_group_id:" << data.extent_group_id
               << ", extent_id:" << data.extent_id
               << ", extent_offset_in_group:" << data.extent_offset_in_group
               << ", data_offset_in_extent:" << data.data_offset_in_extent
               << ", length:" << data.length
               << ", hit_disk_id:" << data.hit_disk_id
               << ", data.size:" << data.data.size()
               << ", digest:" << data.digest
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_migrate_extent_group& data) {
    return out << "{smd_migrate_extent_group:["
               << "driver_node_ip:" << data.driver_node_ip
               << ", container_name:" << data.container_name
               << ", intent_id:" << data.intent_id
               << ", owner_id:" << data.owner_id
               << ", extent_group_id:" << data.extent_group_id
               << ", src_disk_id:" << data.src_disk_id
               << ", dst_disk_id:" << data.dst_disk_id
               << ", offset:" << data.offset
               << ", length:" << data.length
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const smd_get_extent_group& data) {
    return out << "{smd_get_extent_group:["
               << "volume_id:" << data.volume_id 
               << ", extent_group_id:" << data.extent_group_id
               << "]}";
}

std::ostream& operator<<(std::ostream& out, const rmd_get_extent_group& data) {
    return out << "{smd_get_extent_group:["
               << "volume_id:" << data.volume_id 
               << ", extent_group_id:" << data.extent_group_id
               << ", data.size:" << data.data.size()
               << "]}";
}


} //namespace hive
