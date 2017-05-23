#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>

#include "drain_task_group.hh"

namespace hive {

class drain_job {
public:

    uint64_t job_id;
    uint64_t job_generation;
    std::vector<drain_task_group> task_groups;

public:
    drain_job(uint64_t job_id_, 
               uint64_t job_generation_)
    : job_id(job_id_)
    , job_generation(job_generation_)
    {
    }


    drain_job(uint64_t job_id_, 
               uint64_t job_generation_, 
               std::vector<drain_task_group> task_groups_)
    : job_id(job_id_)
    , job_generation(job_generation_)
    , task_groups(std::move(task_groups_))
    {
    }

    friend std::ostream& operator<<(std::ostream& out, const drain_job& job);
}; 

} //namespace hive

