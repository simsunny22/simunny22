#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include <map>
#include "hive/hive_result.hh"
#include "hive/extent_revision.hh"


namespace hive {

class group_revision_set {
public:
    sstring owner_id;  //volume_id or object_id
    sstring extent_group_id;
    std::vector<extent_revision> revisions;  
public:
    group_revision_set(sstring owner_id
                      , sstring extent_group_id
                      , std::vector<extent_revision> revisions)
                          : owner_id(owner_id)
                          , extent_group_id(extent_group_id)
                          , revisions(std::move(revisions))
    {}
    
    group_revision_set(group_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            revisions = std::move(revision_set.revisions);
        }
    }

    group_revision_set& operator=(group_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            revisions = std::move(revision_set.revisions);
        }
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& out, group_revision_set& revision_set);
}; 


} //namespace hive

