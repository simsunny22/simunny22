#include "hint_entry.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"
#include "hive/hive_tools.hh"

static logging::logger logger("hint_entry");
namespace hive {

std::ostream& operator<<(std::ostream& out, const hint_entry& hint) {
    return out << "{hint_entry:["
               << "owner_id:" << hint.owner_id
               << ", extent_group_id:" << hint.extent_group_id
               << ", failed_disk_ids:" << hive_tools::format(hint.failed_disk_ids)
               << "}]";
}

} //namespace hive
