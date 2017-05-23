#include "hive/drain/drain_job.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const drain_job& job) {
    return out << "{drain_job:["
               << "job_id:" << job.job_id
               << ", job_generation:" << job.job_generation
               << ", task_groups.size:" << job.task_groups.size()
               << "}]";
}

} //namespace hive
