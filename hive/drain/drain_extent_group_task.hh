#pragma once

#include <iostream>
#include <functional>
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>


namespace hive {

enum drain_task_status{
    INIT = 0, 
    SUCCESS = 1, 
    ERROR = 10,
    CREATE_EXTENT_GROUP_ERROR = 11,
    COMMIT_CREATE_EXTENT_GROUP_ERROR = 12,
    REPLICATE_EXTENT_GROUP_ERROR = 13,
    GET_JOURNAL_DATA_ERROR = 14,
    WRITE_EXTENT_GROUP_ERROR = 15,
    COMMIT_WRITE_EXTENT_GROUP_ERROR = 16,
};

struct extent_group_revision {
    uint64_t vclock = 0;
    uint64_t offset_in_journal_segment = 0;
    uint64_t offset_in_extent_group = 0;
    uint64_t length = 0;
    
    friend std::ostream& operator<<(std::ostream& out, const extent_group_revision& revision);
};

class drain_extent_group_task {
public:

    sstring  volume_id;
    sstring  container_name;
    sstring  journal_segment_id;

    std::vector<sstring> journal_nodes;
    std::vector<sstring> extents;

    sstring  extent_group_id;
    bool     need_create_extent_group = false;
    bool     need_replicate_extent_group = false;
    uint64_t version = 0;
    sstring  node_ip;
    sstring  disk_id;
    sstring  replica_disk_ids;

    std::vector<extent_group_revision> extent_group_revisions;
    drain_task_status status;

public:

    drain_extent_group_task(){
        status = drain_task_status::INIT;
    }

    drain_extent_group_task(
        sstring volume_id_
        , sstring container_name_
        , sstring journal_segment_id_
        , std::vector<sstring> journal_nodes_
        , std::vector<sstring> extents_
        , sstring extent_group_id_
        , bool need_create_extent_group_
        , bool need_replicate_extent_group_
        , uint64_t version_
        , sstring node_ip_
        , sstring disk_id_
        , sstring replica_disk_ids_
        , std::vector<extent_group_revision> revisions_
        , drain_task_status status_
        )
        : volume_id(volume_id_)
        , container_name(container_name_)
        , journal_segment_id(journal_segment_id_)
        , journal_nodes(journal_nodes_)
        , extents(extents_)
        , extent_group_id(extent_group_id_)
              , need_create_extent_group(need_create_extent_group_)
        , need_replicate_extent_group(need_replicate_extent_group_)
        , version(version_)
              , node_ip(node_ip_)
        , disk_id(disk_id_)
        , replica_disk_ids(replica_disk_ids_)
        , extent_group_revisions(std::move(revisions_))
        , status(status_)
    {
	    status = drain_task_status::INIT;
    }

    friend std::ostream& operator<<(std::ostream& out, const drain_extent_group_task& task);
}; 

} //namespace hive

