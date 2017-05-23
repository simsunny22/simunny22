#include "hive/hive_plan.hh"
#include <limits>
#include "to_string.hh"
#include "bytes.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const write_plan& plan) {
    out << "{write_plan:[";
    out << "owner_id:" << plan.owner_id;
    for (auto& write_action : plan.write_actions){
      out << "extent_group_id:" << write_action.extent_group_id;
      out << ", extent_id:" << write_action.extent_id;
      out << ", extent_offset_in_group:" << write_action.extent_offset_in_group;
      out << ", data_offset_in_extent:" << write_action.data_offset_in_extent;
      out << ", data_offset:" << write_action.data_offset;
      out << ", length:" << write_action.length;
    }

    out << "]}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const read_plan& plan) {
    out << "{read_plan:[";
    out << "owner_id:" << plan.owner_id;
    for (auto& read_action : plan.read_actions){
      out << "extent_group_id:" << read_action.extent_group_id;
      out << ", extent_id:" << read_action.extent_id;
      out << ", extent_offset_in_group:" << read_action.extent_offset_in_group;
      out << ", data_offset_in_extent:" << read_action.data_offset_in_extent;
      out << ", length:" << read_action.length;
    }
    out << "]}";
    return out;
}

} //namespace hive 
