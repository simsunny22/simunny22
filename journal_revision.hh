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

class journal_revision {
public:
    sstring  owner_id;  //volume_id or object_id
    sstring  extent_id;
    uint64_t data_offset_in_extent;  
    uint64_t length;
    bytes    data;
    uint64_t vclock;

public:
    journal_revision();
    ~journal_revision();

    journal_revision(sstring owner_id_ 
                  , sstring extent_id_
                  , uint64_t data_offset_in_extent_
                  , uint64_t length_ 
                  , bytes&&  data_
                  , uint64_t vclock_)
                      : owner_id(owner_id_)
                      , extent_id(extent_id_)
                      , data_offset_in_extent(data_offset_in_extent_)
                      , length(length_)
                      , data(std::move(data_))
                      , vclock(vclock_){
        assert(length == data.size());
    }
    //for_read
    journal_revision(uint64_t data_offset_in_extent, 
                     uint64_t length, 
                     bytes data, 
                     uint64_t vclock)
        :data_offset_in_extent(data_offset_in_extent)
        ,length(length)
        ,data(std::move(data))
        ,vclock(vclock)
    {}

    journal_revision(journal_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            extent_id = revision.extent_id;     
            data_offset_in_extent = revision.data_offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
        }
    }

    journal_revision& operator=(journal_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            extent_id = revision.extent_id;     
            data_offset_in_extent = revision.data_offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
        }

        return *this;
    }
    
    static journal_revision from_raw_result(const hive_result& result);
    void set_data(temporary_buffer<char> data);

    uint64_t serialize_size();
    void serialize( std::unique_ptr<char[], free_deleter>& bufptr);
    void deserialize( std::unique_ptr<char[], free_deleter>& bufptr, uint64_t pos = 0);
    friend std::ostream& operator<<(std::ostream& out, const journal_revision& revision);

private:
    uint64_t serialize_sstring(char* buf, uint64_t pos, sstring& value, sstring description = "");
    uint64_t serialize_uint64(char* buf, uint64_t pos, uint64_t& value, sstring description = "");
    sstring deserialize_sstring(char* buf, uint64_t pos, sstring description = "");
    uint64_t deserialize_uint64(char* buf, uint64_t pos, sstring description = "");
}; 

} //namespace hive

