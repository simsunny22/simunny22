#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include <limits>
#include "to_string.hh"
#include "bytes.hh"

namespace hive {

std::ostream& operator<<(std::ostream& out, const hive_read_command& read_cmd) {
    out << "{hive_read_command:[";
    out << "owner_id:" << read_cmd.owner_id;
    out << ", offset:" << read_cmd.offset;
    out << ", length:" << read_cmd.length;
    out << "]}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const hive_read_subcommand& read_cmd) {
    out << "{hive_read_subcommand:[";
    out << "owner_id:" << read_cmd.owner_id;
    out << ", extent_group_id:" << read_cmd.extent_group_id;
    out << ", extent_id:" << read_cmd.extent_id;
    out << ", extent_offset_in_group:" << read_cmd.extent_offset_in_group;
    out << ", data_offset_in_extent:" << read_cmd.data_offset_in_extent;
    out << ", length:" << read_cmd.length;
    out << ", disk_ids:" << read_cmd.disk_ids;
    out << ", md5:" << read_cmd.md5;
    out << "]}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const hive_write_command& write_cmd) {
    out << "{hive_write_command:[";
    out << "owner_id:" << write_cmd.owner_id;
    out << ", offset:" << write_cmd.offset;
    out << ", length:" << write_cmd.length;
    out << ", data.size:" << write_cmd.data.size();
    out << "]}";
    return out;
}

std::ostream& operator<<(std::ostream& out, const hive_access_trail_command& access_trail_cmd) {
    out << "{hive_trail_access_command:[";
    out << "trail_disk_id:"   << access_trail_cmd.disk_id;
    out << ", trail_node_id:" << access_trail_cmd.node_id;
    out << "]}";
    return out;
}

} //namespace hive 
