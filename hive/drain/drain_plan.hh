#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>

#include "drain_extent_group_task.hh" 

namespace hive {

class drain_plan {
public:
    sstring type;
    std::vector<drain_extent_group_task> tasks;

    drain_plan(){
    }

    drain_plan(
        sstring type_
        , std::vector<drain_extent_group_task> tasks) 
        : type(type_)
        , tasks(std::move(tasks))
    {}
    
    friend std::ostream& operator<<(std::ostream& out, const drain_plan& plan);
}; 

} //namespace hive

