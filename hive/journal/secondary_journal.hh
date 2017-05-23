#pragma once
#include "journal_segment.hh"
#include "db/config.hh"
#include "hive/group_revision_set.hh"
#include "hive/journal/revision_data.hh"
#include "hive/drain/drain_extent_group_task.hh"

namespace hive {

class secondary_journal {
private:
    sstring _volume_id;
    lw_shared_ptr<db::config>  _config;
    semaphore _create_semaphore;
    
    std::map<sstring, lw_shared_ptr<journal_segment>> _segments; //segment_id 
    lw_shared_ptr<journal_segment> get_segment(sstring segment_id);
public:
    secondary_journal(sstring volume_id, lw_shared_ptr<db::config>  cfg);
    secondary_journal(secondary_journal&&) = delete; 
    ~secondary_journal();

    future<> create_segment(
        sstring volume_id, 
        sstring segment_id, 
        uint64_t max_size
    );
    future<> sync_segment(
        sstring segment_id,
        uint64_t offset_in_segment,
        uint64_t offset_in_volume, 
        sstring extent_id,
        uint64_t offset_in_extent, 
        uint64_t data_length, 
        uint64_t serialize_length, 
        uint64_t vclock, 
        bytes data
    );
                                                                    
    future<> sync_segment(
        sstring segment_id, 
        sstring extent_id, 
        uint64_t offset, 
        uint64_t size, 
        uint64_t vclock, 
        bytes content
    );
    future<> discard_segment(sstring segment_id);

    std::vector<lw_shared_ptr<journal_segment>> get_journal_segments(uint64_t vclock);

    future<std::map<sstring, std::vector<revision_data>>>
    get_journal_data(
        sstring segment_id, 
        std::map<sstring, std::vector<extent_group_revision>> revision_map
    );
};

} //namespace hive
