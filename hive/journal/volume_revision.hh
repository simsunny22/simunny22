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

//for extent, not for extent_group
class volume_revision {
public:
    sstring  owner_id;  //volume_id or object_id
    uint64_t offset_in_volume;
    sstring  extent_id;
    uint64_t offset_in_extent;  
    uint64_t length;
    bytes    data;
    uint64_t vclock;

public:
    volume_revision();
    ~volume_revision();

    volume_revision(sstring owner_id 
                  , uint64_t offset_in_volume
                  , sstring extent_id
                  , uint64_t offset_in_extent
                  , uint64_t length 
                  , bytes&&  data
                  , uint64_t vclock)
                      : owner_id(owner_id)
                      , offset_in_volume(offset_in_volume)
                      , extent_id(extent_id)
                      , offset_in_extent(offset_in_extent)
                      , length(length)
                      , data(std::move(data))
                      , vclock(vclock){
        assert(length == data.size());
    }
    //for_read
    volume_revision(
        uint64_t offset_in_extent, 
        uint64_t length, 
        bytes data, 
        uint64_t vclock)
            :offset_in_extent(offset_in_extent)
            ,length(length)
            ,data(std::move(data))
            ,vclock(vclock){}

    volume_revision(volume_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            offset_in_volume = revision.offset_in_volume;
            extent_id = revision.extent_id;     
            offset_in_extent = revision.offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
        }
    }

    volume_revision& operator=(volume_revision&& revision) noexcept  {
        if(this != &revision){
            owner_id = revision.owner_id;
            offset_in_volume = revision.offset_in_volume;
            extent_id = revision.extent_id;     
            offset_in_extent = revision.offset_in_extent;
            length = revision.length;
            data = std::move(revision.data);
            vclock = revision.vclock;
        }

        return *this;
    }
    
    static volume_revision from_raw_result(const hive_result& result);
    void set_data(temporary_buffer<char> data);

    uint64_t serialize_size();
    void serialize( std::unique_ptr<char[], free_deleter>& bufptr);
    void deserialize( std::unique_ptr<char[], free_deleter>& bufptr, uint64_t pos = 0);
    friend std::ostream& operator<<(std::ostream& out, const volume_revision& revision);

private:
    uint64_t serialize_sstring(char* buf, uint64_t pos, sstring& value, sstring description = "");
    uint64_t serialize_uint64(char* buf, uint64_t pos, uint64_t& value, sstring description = "");
    sstring deserialize_sstring(char* buf, uint64_t pos, sstring description = "");
    uint64_t deserialize_uint64(char* buf, uint64_t pos, sstring description = "");
}; 

} //namespace hive

