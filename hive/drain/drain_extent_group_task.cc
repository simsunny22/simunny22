#include "hive/drain/drain_extent_group_task.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const extent_group_revision& revision) {
    return out << "{extent_group_revision:["
               << "vclock:" << revision.vclock
               << ", offset_in_journal_segment:" << revision.offset_in_journal_segment 
               << ", offset_in_extent_group:" << revision.offset_in_extent_group
               << ", length:" << revision.length
               << "]}";

}

std::ostream& operator<<(std::ostream& out, const drain_extent_group_task& task) {
    return out << "{drain_extent_group_task:["
               << "volume_id:" << task.volume_id
               << ", journal_segment_id:" << task.journal_segment_id
               << ", journal_nodes.size:" << task.journal_nodes.size()
               << ", extents.size:" << task.extents.size()
               << ", extent_group_id:" << task.extent_group_id
               << ", need_create_extent_group:" << task.need_create_extent_group
               << ", need_replicate_extent_group:" << task.need_replicate_extent_group
               << ", version:" << task.version
               << ", node_ip:" << task.node_ip
               << ", disk_id:" << task.disk_id
               << ", replica_disk_ids:" << task.replica_disk_ids
               << ", extent_group_revisions:" << task.extent_group_revisions
               << ", status:" << task.status
               << "]}";
}

} //namespace hive
