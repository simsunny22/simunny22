#include "hive/drain/drain_task_group.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const drain_task_group& group) {
    return out << "{drain_task_group:["
               << "volume_id:" << group.volume_id
               << ", job_id:" << group.job_id
               << ", group_id:" << group.group_id
               << ", job_generation:" << group.job_generation
               << ", target:" << group.target
               << ", tasks.size:" << group.tasks.size()
               << ", retry_drain_num:" << group.retry_drain_num
               << ", group_type:" << group.group_type
               << ", tasks:[" << group.tasks
               << "]}]";
}

} //namespace hive
