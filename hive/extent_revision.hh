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

class extent_revision {
public:
    sstring  owner_id;  //volume_id or object_id
    sstring  extent_group_id;
    sstring  extent_id;
    uint64_t extent_offset_in_group;
    uint64_t data_offset_in_extent;  
    uint64_t length;
    bytes    data;
    uint64_t vclock;
    sstring  disk_ids;
    sstring  options;
    sstring  intent_id;

public:
    extent_revision();
    extent_revision(sstring owner_id_ 
                  , sstring extent_group_id_
                  , sstring extent_id_
                  , uint64_t extent_offset_in_group_
                  , uint64_t data_offset_in_extent_
                  , uint64_t length_ 
                  , bytes&&  data_
                  , uint64_t vclock_
                  , sstring  disk_ids_ 
                  , sstring options_ 
                  , sstring intent_id_ = "default_test_intent_id") //wztest
                      : owner_id(owner_id_)
                      , extent_group_id(extent_group_id_)
                      , extent_id(extent_id_)
                      , extent_offset_in_group(extent_offset_in_group_)
                      , data_offset_in_extent(data_offset_in_extent_)
                      , length(length_)
                      , data(std::move(data_))
                      , vclock(vclock_) 
                      , disk_ids(disk_ids_)
                      , options(options_)
                      , intent_id(intent_id_)
    {
        assert(length == data.size());
    }

    //for read
    extent_revision(uint64_t data_offset_in_extent, uint64_t length, bytes data, uint64_t vclock)
        :data_offset_in_extent(data_offset_in_extent)
        ,length(length)
        ,data(std::move(data))
        ,vclock(vclock)
    {}

    ~extent_revision();

    void set_data(temporary_buffer<char> data);

    extent_revision(extent_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            extent_group_id = revision.extent_group_id;
            extent_id = revision.extent_id;     
            extent_offset_in_group = revision.extent_offset_in_group;
            data_offset_in_extent = revision.data_offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
            disk_ids = revision.disk_ids;
            intent_id = revision.intent_id;
            options = revision.options;
        }
    }

    extent_revision& operator=(extent_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            extent_group_id = revision.extent_group_id;
            extent_id = revision.extent_id;     
            extent_offset_in_group = revision.extent_offset_in_group;
            data_offset_in_extent = revision.data_offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
            disk_ids = revision.disk_ids;
            intent_id = revision.intent_id;
            options = revision.options;
        }

        return *this;
    }

    static extent_revision from_raw_result(const hive_result& result);
    uint64_t serialize_size();
    void serialize( std::unique_ptr<char[], free_deleter>& bufptr);
    void deserialize( std::unique_ptr<char[], free_deleter>& bufptr, uint64_t pos = 0);
    friend std::ostream& operator<<(std::ostream& out, const extent_revision& revision);

private:
    uint64_t serialize_sstring(char* buf, uint64_t pos, sstring& value, sstring description = "");
    uint64_t serialize_uint64(char* buf, uint64_t pos, uint64_t& value, sstring description = "");
    sstring deserialize_sstring(char* buf, uint64_t pos, sstring description = "");
    uint64_t deserialize_uint64(char* buf, uint64_t pos, sstring description = "");
}; 

} //namespace hive

