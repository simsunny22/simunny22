#include "hive/journal_revision_set.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

static logging::logger logger("journal_revision_set");

std::ostream& operator<<(std::ostream& out, journal_revision_set& revision_set) {
    out << "{revisions:[";
    for(auto& revision : revision_set.get_revisions()) {
        out << "{"
            << ", owner_id:" << revision.owner_id
            << ", extent_group_id:" << revision.extent_group_id
            << ", extent_id:" << revision.extent_id
            << ", data_offset_in_extent:" << revision.data_offset_in_extent
            << ", length:" << revision.length
            << ", vclock:" << revision.vclock
            << "}"; 
    }
    out << "]}";
    return out;
}


} //namespace hive
