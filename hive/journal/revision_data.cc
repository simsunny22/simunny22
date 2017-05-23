#include "revision_data.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

static logging::logger logger("revision_data");
namespace hive {

std::ostream& operator<<(std::ostream& out, const revision_data& data) {
    return out << "{revision_data:["
               << "vclock:" << data.vclock
               << ", offset_in_journal_segment:" << data.offset_in_journal_segment
               << ", offset_in_extent_group:" << data.offset_in_extent_group
               << ", length:" << data.length
               << "}]";
}

} //namespace hive
