#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>


namespace hive {

class volume_revision_view {
public:
    sstring  volume_id;
    uint64_t offset_in_volume = 0;

    sstring  extent_id;
    uint64_t offset_in_extent = 0;

    sstring  journal_segment_id;
    uint64_t offset_in_journal_segment = 0;
    uint64_t length = 0;

    uint64_t vclock = 0;

public:
    volume_revision_view(){
    }

    friend std::ostream& operator<<(std::ostream& out, const volume_revision_view& v_revision);
}; 

} //namespace hive

