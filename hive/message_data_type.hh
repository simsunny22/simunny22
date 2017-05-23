#pragma once

#include "seastar/core/sstring.hh"
#include "bytes.hh"

#include "hive/extent_revision.hh"
#include "hive/group_revision_set.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/journal/revision_data.hh"

namespace hive{

class abstract_md {
public:
    virtual sstring desc() { return "abstract_md"; }
};


//for journal
class smd_init_segment : public abstract_md {
public:
    unsigned cpu_id;
    sstring volume_id;
    sstring segment_id;
    uint64_t size;
    
    smd_init_segment(
        unsigned cpu_id,
        sstring volume_id, 
        sstring segment_id,
        uint64_t size )
        : cpu_id(cpu_id)
        , volume_id(volume_id)
        , segment_id(segment_id)
        , size(size)
    {}

    sstring desc() { return "smd_init_segment"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_init_segment& data);
};

class smd_sync_segment : public abstract_md {
public:
    unsigned cpu_id;
    sstring segment_id;
    uint64_t offset_in_segment;
    sstring volume_id;
    uint64_t offset_in_volume;
    sstring extent_id;
    uint64_t offset_in_extent;
    uint64_t data_length;
    uint64_t serialize_length;
    uint64_t vclock;
    bytes content;
    smd_sync_segment(
        unsigned cpu_id,
        sstring segment_id, 
        uint64_t offset_in_segment,
        sstring volume_id, 
        uint64_t offset_in_volume,
        sstring extent_id,
        uint64_t offset_in_extent,
        uint64_t data_length,
        uint64_t serialize_length,
        uint64_t vclock,
        bytes content)
        : cpu_id(cpu_id)
        , segment_id(segment_id)
        , offset_in_segment(offset_in_segment)
        , volume_id(volume_id)
        , offset_in_volume(offset_in_volume)
        , extent_id(extent_id)
        , offset_in_extent(offset_in_extent)
        , data_length(data_length)
        , serialize_length(serialize_length)
        , vclock(vclock)
        , content(content)
    {}

    sstring desc() { return "smd_sync_segment"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_sync_segment& data);
};

class smd_discard_segment : public abstract_md {
public:
    unsigned cpu_id;
    sstring volume_id;
    sstring segment_id;
    smd_discard_segment(
        unsigned cpu_id,
        sstring volume_id, 
        sstring segment_id)
        : cpu_id(cpu_id)
        , volume_id(volume_id)
        , segment_id(segment_id) 
    {}

    sstring desc() { return "smd_discard_segment"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_discard_segment& data);
};

class smd_get_journal_data: public abstract_md {
public:
    sstring volume_id;
    sstring segment_id;
    uint64_t shard;
    std::map<sstring, std::vector<extent_group_revision>> revisions;

    smd_get_journal_data(
        sstring volume_id
        , sstring segment_id
        , uint64_t shard
        , std::map<sstring, std::vector<extent_group_revision>> revisions)
        : volume_id(volume_id)
        , segment_id(segment_id)
        , shard(shard)
        , revisions(std::move(revisions))
    {}

    sstring desc() { return "smd_get_journal_data"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_get_journal_data& data);
};

class rmd_get_journal_data{
public:
    sstring volume_id;
    sstring segment_id;
    std::map<sstring, std::vector<revision_data>> revision_datas;

    rmd_get_journal_data(
        sstring volume_id
        , sstring segment_id
        , std::map<sstring, std::vector<revision_data>> revision_datas)
        : volume_id(volume_id)
        , segment_id(segment_id)
        , revision_datas(std::move(revision_datas))
   {}

   friend std::ostream& operator<<(std::ostream& out, const rmd_get_journal_data& data);
};


//for extent_store
class smd_create_extent_group: public abstract_md {
public:
    sstring extent_group_id;
    std::vector<sstring> disk_ids;

    smd_create_extent_group(
        sstring extent_group_id,
        std::vector<sstring> disk_ids)
        : extent_group_id(extent_group_id)
        , disk_ids(disk_ids)
    {}

    sstring desc() { return "smd_create_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_create_extent_group& data);
};

class smd_delete_extent_group: public abstract_md {
public:
    sstring extent_group_id;
    std::vector<sstring> disk_ids; //TODO:red need only one
    
    smd_delete_extent_group(
        sstring extent_group_id,
        std::vector<sstring> disk_ids)
        : extent_group_id(extent_group_id)
        , disk_ids(disk_ids)
    {}

    sstring desc() { return "smd_delete_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_delete_extent_group& data);
};

class smd_rwrite_extent_group: public abstract_md {
public:
    sstring extent_group_id;
    std::vector<sstring> disk_ids;
    std::vector<extent_revision> revisions;

    smd_rwrite_extent_group(
        sstring extent_group_id,
        std::vector<sstring> disk_ids,
        std::vector<extent_revision> revisions)
        : extent_group_id(extent_group_id)
        , disk_ids(disk_ids)
        , revisions(std::move(revisions))
    {}

    sstring desc() { return "smd_rwrite_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_rwrite_extent_group& data);
};

class smd_read_extent_group {
public:
    sstring owner_id;
    sstring extent_group_id;
    sstring extent_id;
    uint64_t extent_offset_in_group;
    uint64_t data_offset_in_extent;
    uint64_t length;
    sstring disk_id;
    bool only_need_digest;

    smd_read_extent_group(
        sstring owner_id,
        sstring extent_group_id,
        sstring extent_id,
        uint64_t extent_offset_in_group,
        uint64_t data_offset_in_extent,
        uint64_t length,
        sstring disk_id,
        bool only_need_digest = false)
        : owner_id(owner_id)
        , extent_group_id(extent_group_id)
        , extent_id(extent_id)
        , extent_offset_in_group(extent_offset_in_group)
        , data_offset_in_extent(data_offset_in_extent)
        , length(length)
        , disk_id(disk_id)
        , only_need_digest(only_need_digest)
    {}

    sstring desc() { return "smd_read_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_read_extent_group& data);
};

class rmd_read_extent_group {
public:
    sstring owner_id;
    sstring extent_group_id;
    sstring extent_id;
    uint64_t extent_offset_in_group;
    uint64_t data_offset_in_extent;
    uint64_t length;
    sstring hit_disk_id;
    bytes data;
    sstring digest;

    rmd_read_extent_group(
        sstring owner_id,
        sstring extent_group_id,
        sstring extent_id,
        uint64_t extent_offset_in_group,
        uint64_t data_offset_in_extent,
        uint64_t length,
        sstring hit_disk_id,
        bytes data,
        sstring digest = "")
        : owner_id(owner_id)
        , extent_group_id(extent_group_id)
        , extent_id(extent_id)
        , extent_offset_in_group(extent_offset_in_group)
        , data_offset_in_extent(data_offset_in_extent)
        , length(length)
        , hit_disk_id(hit_disk_id)
        , data(std::move(data))
        , digest(digest)
    {}

    sstring desc() { return "rmd_read_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const rmd_read_extent_group& data);
};

class smd_migrate_extent_group: public abstract_md {
public:
    sstring driver_node_ip;
    sstring container_name;
    sstring intent_id;  

    sstring owner_id;
    sstring extent_group_id;
    sstring src_disk_id;
    sstring dst_disk_id;
    uint64_t offset;
    uint64_t length;

    smd_migrate_extent_group(
        sstring driver_node_ip,
        sstring container_name,
        sstring intent_id,
        sstring owner_id,
        sstring extent_group_id,
        sstring src_disk_id,
        sstring dst_disk_id,
        uint64_t offset,
        uint64_t length)
        : driver_node_ip(driver_node_ip)
        , container_name(container_name)
        , intent_id(intent_id)
        , owner_id(owner_id)
        , extent_group_id(extent_group_id)
        , src_disk_id(src_disk_id)
        , dst_disk_id(dst_disk_id)
        , offset(offset)
        , length(length)
    {}

    sstring desc() { return "smd_migrate_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_migrate_extent_group& data);
};

//TODO:yellow use union type replace the struct type
struct union_md{
   std::shared_ptr<smd_init_segment> init_segment = nullptr;
   std::shared_ptr<smd_sync_segment> sync_segment = nullptr;
   std::shared_ptr<smd_discard_segment> discard_segment = nullptr;
   std::shared_ptr<smd_create_extent_group> create_extent_group = nullptr;
   std::shared_ptr<smd_delete_extent_group> delete_extent_group = nullptr;
   std::shared_ptr<smd_rwrite_extent_group> rwrite_extent_group = nullptr;
   std::shared_ptr<smd_migrate_extent_group> migrate_extent_group = nullptr;
};

class smd_get_extent_group: public abstract_md {
public:
    sstring volume_id; //get context need
    sstring extent_group_id;

    smd_get_extent_group(
        sstring volume_id
        , sstring extent_group_id)
        : volume_id(volume_id)
        , extent_group_id(extent_group_id)
    {}

    sstring desc() { return "smd_get_extent_group"; }
    friend std::ostream& operator<<(std::ostream& out, const smd_get_extent_group& data);
};

class rmd_get_extent_group {
public:
    sstring volume_id;
    sstring extent_group_id;
    bytes data;

    rmd_get_extent_group(
        sstring volume_id
        , sstring extent_group_id
        , bytes data)
        : volume_id(volume_id)
        , extent_group_id(extent_group_id)
        , data(std::move(data))
    {}

    friend std::ostream& operator<<(std::ostream& out, const rmd_get_extent_group& data);
};

// drain task
class smd_drain_task_group : public abstract_md {
public:
    sstring volume_id; 
    uint64_t job_id;
    uint64_t group_id;
    uint64_t job_generation;
    sstring target;
    sstring segment_id;
    uint64_t shard;
    std::vector<sstring> journal_nodes;
    std::vector<hive::drain_extent_group_task> tasks;
    hive::drain_task_group_type group_type;

public:
    smd_drain_task_group(sstring volume_id_,
               uint64_t job_id_,
               uint64_t group_id_,
               uint64_t job_generation_,
               sstring target_,
               sstring segment_id_,
               uint64_t shard_,
               std::vector<sstring> journal_nodes_,
               std::vector<hive::drain_extent_group_task> tasks_,
               hive::drain_task_group_type group_type_)
    : volume_id(volume_id_)
    , job_id(job_id_)
    , group_id(group_id_)
    , job_generation(job_generation_)
    , target(target_)
    , segment_id(segment_id_)
    , shard(shard_)
    , journal_nodes(journal_nodes_)
    , tasks(std::move(tasks_))
    , group_type(group_type_){
    }
    sstring desc() { return "smd_drain_task_group"; }
};


} //namespace hive
