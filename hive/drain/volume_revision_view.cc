#include "hive/drain/volume_revision_view.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const volume_revision_view& v_revision) {
    return out << "{volume_revision_view:["
               << "volume_id:" << v_revision.volume_id
               << ", offset_in_volume:" << v_revision.offset_in_volume
               << ", extent_id:" << v_revision.extent_id
               << ", offset_in_extent:" << v_revision.offset_in_extent
               << ", journal_segment_id:" << v_revision.journal_segment_id
               << ", offset_in_journal_segment:" << v_revision.offset_in_journal_segment
               << ", length:" << v_revision.length
               << ", vclock:" << v_revision.vclock
               << "}]";
}

} //namespace hive
