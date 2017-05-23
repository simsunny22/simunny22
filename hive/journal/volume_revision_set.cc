#include "hive/journal/volume_revision_set.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

static logging::logger logger("volume_revision_set");

std::ostream& operator<<(std::ostream& out, volume_revision_set& revision_set) {
    out << "{revisions:[";
    for(auto& revision : revision_set.revisions) {
        out << "{"
            << ", owner_id:" << revision.owner_id
            << ", offset_in_volume:" << revision.offset_in_volume
            << ", extent_id:" << revision.extent_id
            << ", offset_in_extent:" << revision.offset_in_extent
            << ", length:" << revision.length
            << ", vclock:" << revision.vclock
            << "}"; 
    }
    out << "]}";
    return out;
}


} //namespace hive
