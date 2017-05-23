#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include "hive/hive_result.hh"
#include "hive/hive_request.hh"


namespace hive {

class revision_data{
public:
    uint64_t vclock;
    uint64_t offset_in_journal_segment;
    uint64_t offset_in_extent_group;
    uint64_t length;
    bytes data;

public:
    revision_data(uint64_t vclock 
                  , uint64_t offset_in_journal_segment
                  , uint64_t offset_in_extent_group
                  , uint64_t length 
                  , bytes&&  data)
                  : vclock(vclock)
                  , offset_in_journal_segment(offset_in_journal_segment)
                  , offset_in_extent_group(offset_in_extent_group)
                  , length(length)
                  , data(std::move(data))
    {}
    
    friend std::ostream& operator<<(std::ostream& out, const revision_data& data);
}; 

} //namespace hive

