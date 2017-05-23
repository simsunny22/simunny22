#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include <map>
#include "hive/hive_result.hh"
#include "hive/journal/volume_revision.hh"


namespace hive {

class volume_revision_set {
public:
    sstring owner_id;  //volume_id or object_id
    sstring extent_group_id;
    std::vector<volume_revision> revisions;  
public:
    volume_revision_set(sstring owner_id
                      , sstring extent_group_id
                      , std::vector<volume_revision> revisions)
                          : owner_id(owner_id)
                          , extent_group_id(extent_group_id)
                          , revisions(std::move(revisions))
    {}
    
    volume_revision_set(volume_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            revisions = std::move(revision_set.revisions);
        }
    }

    volume_revision_set& operator=(volume_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            revisions = std::move(revision_set.revisions);
        }
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& out, volume_revision_set& revision_set);
}; 


} //namespace hive

