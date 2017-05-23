#pragma once
#include "journal_segment.hh"
#include "db/config.hh"
namespace hive {

class secondary_journal_new {
private:
    lw_shared_ptr<db::config>  _config;
    semaphore _create_semaphore;
    std::map<sstring, lw_shared_ptr<journal_segment>> _segments; //commitlog_id 
public:
    secondary_journal_new(lw_shared_ptr<db::config>  cfg);
    secondary_journal_new(secondary_journal_new&&) = delete; 
    ~secondary_journal_new();

    future<> create_segment(
        sstring volume_id, 
        sstring commitlog_id, 
        uint64_t max_size
    );
    future<> sync_segment(
        sstring commitlog_id, 
        sstring extent_group_id, 
        sstring extent_id, 
        uint64_t offset, 
        uint64_t size, 
        uint64_t vclock, 
        bytes content
    );
    future<> discard_segment(sstring commitlog_id);

    std::vector<lw_shared_ptr<journal_segment>> get_journal_segments(uint64_t vclock);
    //future<> stop();
};

} //namespace hive
