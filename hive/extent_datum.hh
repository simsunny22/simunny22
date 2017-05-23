#pragma once

#include <iostream>
#include <functional>
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include "hive/hive_result.hh"


namespace hive {

struct report_drain_datum {
    sstring extent_id;
    sstring extent_group_id;
    int64_t extent_offset_in_group;
    sstring disk_ids;
    int64_t version;
    sstring md5;

    report_drain_datum(sstring extent_id
                     , sstring extent_group_id
                     , int64_t extent_offset_in_group
                     , sstring disk_ids
                     , int64_t version
                     , sstring md5)
                         :extent_id(extent_id)
                         ,extent_group_id(extent_group_id)
                         ,extent_offset_in_group(extent_offset_in_group)
                         ,disk_ids(disk_ids)
                         ,version(version)
                         ,md5(md5){};
};

class extent_datum {
public:
    sstring  extent_id;
    sstring  extent_group_id;
    uint64_t extent_offset_in_group;
    sstring candidate_disk_ids;
    sstring hit_disk_ids;
    uint64_t version;
    sstring md5;
    bytes data;
public:
    extent_datum();
    extent_datum(sstring extent_id
               , sstring extent_group_id
               , uint64_t extent_offset_in_group
               , sstring candidate_disk_ids
               , uint64_t version
               , sstring md5 
               , bytes data)
                   : extent_id(extent_id)
                   , extent_group_id(extent_group_id)
                   , extent_offset_in_group(extent_offset_in_group)
                   , candidate_disk_ids(candidate_disk_ids)
                   , version(version)
                   , md5(md5)
                   , data(std::move(data)){}

    extent_datum(sstring extent_id
               , sstring extent_group_id
               , uint64_t extent_offset_in_group
               , sstring candidate_disk_ids
               , sstring md5
               , bytes data)
                   : extent_id(extent_id)
                   , extent_group_id(extent_group_id)
                   , extent_offset_in_group(extent_offset_in_group)
                   , candidate_disk_ids(candidate_disk_ids)
                   , version(0)
                   , md5(md5)
                   , data(std::move(data)){}

    extent_datum(sstring extent_id
               , sstring extent_group_id
               , uint64_t extent_offset_in_group
               , sstring candidate_disk_ids
               , sstring hit_disk_ids
               , uint64_t version
               , sstring md5 
               , bytes data)
                   : extent_id(extent_id)
                   , extent_group_id(extent_group_id)
                   , extent_offset_in_group(extent_offset_in_group)
                   , candidate_disk_ids(candidate_disk_ids)
                   , hit_disk_ids(hit_disk_ids)
                   , version(version)
                   , md5(md5)
                   , data(std::move(data)){}

    extent_datum(sstring extent_id
               , sstring extent_group_id
               , uint64_t extent_offset_in_group
               , sstring candidate_disk_ids
               , sstring hit_disk_ids
               , sstring md5
               , bytes data)
                   : extent_id(extent_id)
                   , extent_group_id(extent_group_id)
                   , extent_offset_in_group(extent_offset_in_group)
                   , candidate_disk_ids(candidate_disk_ids)
                   , hit_disk_ids(hit_disk_ids)
                   , version(0)
                   , md5(md5)
                   , data(std::move(data)){}
    ~extent_datum();

    uint64_t get_length(){
        return data.size();
    }

    void set_data(temporary_buffer<char> data);

    extent_datum(extent_datum&& datum) noexcept  {
        if(this != &datum){
            extent_id = datum.extent_id;     
            extent_group_id = datum.extent_group_id;
            extent_offset_in_group = datum.extent_offset_in_group;
            candidate_disk_ids = datum.candidate_disk_ids;
            hit_disk_ids = datum.hit_disk_ids;
            version  = datum.version;
            md5  = datum.md5;
            data = std::move(datum.data);
        }
    }

    extent_datum& operator=(extent_datum&& datum) noexcept  {
        if(this != &datum){
         extent_id = datum.extent_id;     
         extent_group_id = datum.extent_group_id;
         extent_offset_in_group = datum.extent_offset_in_group;
         candidate_disk_ids = datum.candidate_disk_ids;
         hit_disk_ids = datum.hit_disk_ids;
         version = datum.version;
         md5 = datum.md5;
         data = std::move(datum.data);
        }

        return *this;
    }

    bool is_extent_datum_exist(bytes& header_digest);
    void init_empty_datum();

    void serialize(std::unique_ptr<char[], free_deleter>& bufptr);
    void deserialize(std::unique_ptr<char[], free_deleter> bufptr);

    static extent_datum from_raw_result(const hive_result& r);
    friend std::ostream& operator<<(std::ostream& out, const extent_datum& datum);
    friend inline bool operator==(const extent_datum& x, const extent_datum& y);
    friend inline bool operator!=(const extent_datum& x, const extent_datum& y);

}; 

inline bool operator==(const extent_datum& x, const extent_datum& y) {
    return x.data== y.data;
}

inline bool operator!=(const extent_datum& x, const extent_datum& y) {
    return !(x == y);
}

} //namespace hive

