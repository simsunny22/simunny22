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

class extent_revision_set {
public:
    sstring owner_id;  //volume_id or object_id
    sstring extent_group_id;
    sstring extent_id;
    int64_t extent_offset_in_group;
    sstring disk_ids;
    int64_t vclock;
    std::vector<extent_revision> revisions;  
public:
    extent_revision_set(sstring owner_id_
                      , sstring extent_group_id_
                      , sstring extent_id_
                      , int64_t extent_offset_in_group_
                      , sstring disk_ids_
                      , int64_t vclock_)
                          : owner_id(owner_id_)
                          , extent_group_id(extent_group_id_)
                          , extent_id(extent_id_)
                          , extent_offset_in_group(extent_offset_in_group_)
                          , disk_ids(disk_ids_)
                          , vclock(vclock_){}
    //for serialize 
    extent_revision_set(sstring owner_id_
                      , sstring extent_group_id_
                      , sstring extent_id_
                      , int64_t extent_offset_in_group_
                      , sstring disk_ids_
                      , int64_t vclock_
                      , std::vector<hive::extent_revision> revisions_)
                          : owner_id(owner_id_)
                          , extent_group_id(extent_group_id_)
                          , extent_id(extent_id_)
                          , extent_offset_in_group(extent_offset_in_group_)
                          , disk_ids(disk_ids_)
                          , vclock(vclock_)
                          , revisions(std::move(revisions_)){}

    ~extent_revision_set(){}

    extent_revision_set(extent_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            extent_id = revision_set.extent_id;
            extent_offset_in_group = revision_set.extent_offset_in_group;
            disk_ids = revision_set.disk_ids;
            vclock = revision_set.vclock;
            revisions = std::move(revision_set.revisions);
        }
    }

    extent_revision_set& operator=(extent_revision_set&& revision_set) noexcept  {
        if(this != &revision_set){
            owner_id = revision_set.owner_id;
            extent_group_id = revision_set.extent_group_id;
            extent_id = revision_set.extent_id;
            extent_offset_in_group = revision_set.extent_offset_in_group;
            disk_ids = revision_set.disk_ids;
            vclock = revision_set.vclock;
            revisions = std::move(revision_set.revisions);
        }
        return *this;
    }

    void add_revision(hive::extent_revision revision);
    std::vector<hive::extent_revision>& get_revisions();
    sstring get_disk_ids();
    int64_t get_vclock();

    friend std::ostream& operator<<(std::ostream& out, extent_revision_set& revision_set);
}; 


} //namespace hive

