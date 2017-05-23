#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>

#include "hive/drain/drain_extent_group_task.hh"

namespace hive {

enum drain_task_group_type{NORMAL_TASK_GROUP, RESCUE_TASK_GROUP};

class drain_task_group {
public:
    sstring volume_id;
    uint64_t job_id;
    uint64_t group_id;
    uint64_t job_generation;
    sstring target;
    sstring segment_id;
    uint64_t shard;
    std::vector<sstring> journal_nodes;
    std::vector<drain_extent_group_task> tasks;
    
    uint64_t retry_drain_num = 0;
    drain_task_group_type group_type;

public:
    drain_task_group(
               sstring volume_id_,
               uint64_t job_id_, 
               uint64_t group_id_, 
               uint64_t job_generation_, 
               sstring target_,
               sstring segment_id_,
               uint64_t shard_,
               std::vector<sstring> journal_nodes_,
               drain_task_group_type group_type_)
    : volume_id(volume_id_)
    , job_id(job_id_)
    , group_id(group_id_)
    , job_generation(job_generation_)
    , target(target_)
    , segment_id(segment_id_)
    , shard(shard_)
    , journal_nodes(journal_nodes_)
    , group_type(group_type_){
    }


    drain_task_group(
               sstring volume_id_,
               uint64_t job_id_, 
               uint64_t group_id_, 
               uint64_t job_generation_, 
               sstring target_,
               sstring segment_id_,
               uint64_t shard_,
               std::vector<sstring> journal_nodes_,
               std::vector<drain_extent_group_task> tasks_,
               drain_task_group_type group_type_)
    : volume_id(volume_id_)
    , job_id(job_id_)
    , group_id(group_id_)
    , job_generation(job_generation_)
    , target(target_)
    , segment_id(segment_id_)
    , shard(shard_)
    , journal_nodes(journal_nodes_)
    , tasks(std::move(tasks_))
    , group_type(group_type_){
    }

    bool success(){
        bool ret = true;
        for(auto& entry:tasks){
            if(entry.status != drain_task_status::SUCCESS){
                ret = false;
                break;
            }
        }
        return ret;
    }

    friend std::ostream& operator<<(std::ostream& out, const drain_task_group& group);
}; 

} //namespace hive

