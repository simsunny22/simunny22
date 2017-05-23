#include "hive/drain/drain_plan.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

#include "hive/drain/drain_entry_cell.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const drain_plan& plan) {
    return out << "{drain_plan:["
               << ", tasks.size:" << plan.tasks.size()
               << "}]";
}

} //namespace hive
