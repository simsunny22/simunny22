#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"

namespace hive {

using namespace hive;

hive_result::hive_result(hive::extent_datum&& extent) {
    _w.write(bytes_view(extent.data));
    _hit_disk = extent.hit_disk_ids;
};


} //namespace hive 
