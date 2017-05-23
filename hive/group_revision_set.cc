#include "hive/group_revision_set.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

static logging::logger logger("group_revision_set");

std::ostream& operator<<(std::ostream& out, group_revision_set& revision_set) {
    out << "{revisions:[";
    for(auto& revision : revision_set.get_revisions()) {
        out << "{"
            << ", owner_id:" << revision.owner_id
            << ", extent_group_id:" << revision.extent_group_id
            << ", extent_id:" << revision.extent_id
            << ", extent_offset_in_group:" << revision.extent_offset_in_group
            << ", data_offset_in_extent:" << revision.data_offset_in_extent
            << ", length:" << revision.length
            << ", vclock:" << revision.vclock
            << ", disk_ids:" << revision.disk_ids
            << "}"; 
    }
    out << "]}";
    return out;
}


} //namespace hive
