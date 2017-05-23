#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include <set>


namespace hive {

class hint_entry {
public:
    sstring  owner_id;  //volume_id or object_id
    sstring  extent_group_id;
    std::set<sstring> failed_disk_ids;

public:
    hint_entry(sstring owner_id_ 
             , sstring extent_group_id_
             , std::set<sstring> failed_disk_ids_)
             : owner_id(owner_id_)
             , extent_group_id(extent_group_id_)
             , failed_disk_ids(failed_disk_ids_)
    {}

    friend std::ostream& operator<<(std::ostream& out, const hint_entry& hint);
}; 

} //namespace hive

