#pragma once
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/lexical_cast.hpp>
#include "types.hh" 


#include "exceptions/exceptions.hh"

#include "hive/hive_plan.hh"

namespace hive {

const static uint64_t extent_size         = 1024*1024;
const static uint64_t extent_num_in_group = 4;
const static uint64_t extent_group_size   = extent_size*extent_num_in_group;

class io_planner {
public:
    io_planner(){}

    std::vector<rw_split_item> split_data(sstring volume_id, uint64_t offset, uint64_t length);
};

} //namespace hive
