#pragma once
#include "core/distributed.hh"
#include "utils/histogram.hh"

#include "hive/drain/drain_manager.hh"
#include "hive/volume_stream.hh"
#include "hive/journal/primary_journal.hh"
//#include "hive/secondary_journal.hh"
#include "hive/hive_request.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/journal/journal_segment.hh"
#include "hive/journal/secondary_journal.hh"
#include "hive/journal/volume_revision.hh"
#include "hive/group_revision_set.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/journal/revision_data.hh"

namespace db{
    class config;
}

namespace hive {

using primary_journal_ptr = lw_shared_ptr<primary_journal>;
using secondary_journal_ptr = lw_shared_ptr<secondary_journal>;

enum class en_data_location {
    IN_PRIMARY_JOURNAL = 0,
    IN_SECONDARY_JOURNAL = 1,
};

class journal_service: public seastar::async_sharded_service<journal_service> {
private:
    lw_shared_ptr<db::config>             _config;
    std::map<sstring, primary_journal_ptr> _primary_journals;
    //secondary_journal_ptr _secondary_journal;
    std::map<sstring , secondary_journal_ptr> _secondary_journals;
    schema_ptr _schema;
    uint64_t _current_segment_num_per_cpu = 0;
    semaphore _total_segments_token;
private:
    journal_config _build_journal_config();
    schema_ptr     _build_schema(sstring volume_id, bool is_primary_journal);
    schema_ptr     _init_schema(sstring volume_id, bool is_primary_journal);
    future<>       _init_primary_journal(sstring volume_id);
    future<>       _rebuild_primary_journal(sstring volume_id, uint64_t vclock);
    future<std::vector<lw_shared_ptr<journal_segment>>>   
    _rebuild_primary_journal_segments(sstring volume_id, uint64_t vclock);

    //for get_journal_data
    future<en_data_location> get_journal_data_location(sstring volume_id);

    future<std::map<sstring, std::vector<revision_data>>>
        get_journal_data_from_primary(
            sstring volume_id,
            sstring segment_id,
            std::map<sstring, std::vector<extent_group_revision>> revision_map
        );

    future<std::map<sstring, std::vector<revision_data>>>
    get_journal_data_from_second(
        sstring volume_id,
        sstring segment_id,
        std::map<sstring, std::vector<extent_group_revision>> revision_map
    );


public:
    journal_service(const db::config& config, uint64_t total_segments_token);
    ~journal_service();

    future<> stop();

    const db::config& get_config() const {
        return *_config;
    }

    schema_ptr get_schema(){
        return _schema;
    }
    
    primary_journal_ptr get_primary_journal(sstring volume_id);

    secondary_journal_ptr get_or_create_secondary_journal(sstring volume_id);
    secondary_journal_ptr get_secondary_journal(sstring volume_id);
    future<std::vector<lw_shared_ptr<journal_segment>>> sync_secondary_segments(sstring volume_id);

    unsigned shard_of(const sstring extent_group_id);
    future<> start_primary_journal(sstring volume_id);
    future<> rebuild_primary_journal(sstring volume_id, uint64_t vclock);

    future<> stop_primary_journal(sstring volume_id);
    
    //future<> sync_secondary_commitlog(sstring commitlog_id, sstring segment_file_path, uint64_t offset, uint64_t length, bytes& content);
    //future<> discard_secondary_commitlog(sstring commitlog_id, replay_position rp);

    future<> force_drain(std::vector<sstring> volume_ids);
    future<> force_drain_all();
    
    future<int> check_force_drain(sstring volume_id);
    future<int> check_force_drain_all();
    
    bool cache_enabled(){
        return _config->hive_cache_enabled();
    }

    future<> get_total_segments_token();
    future<> release_total_segments_token(uint64_t i = 1);

    future<std::vector<volume_revision>>
    get_segment_extent(sstring volume_id, sstring segment_id, sstring extent_id);

    future<std::vector<volume_revision_view>>
    scan_journal_segment(sstring volume_id, sstring segment_id);

    future<std::map<sstring, std::vector<revision_data>>>
    get_journal_data(
        sstring volume_id,
        sstring segment_id,
        std::map<sstring, std::vector<extent_group_revision>> revision_map
    );
};

extern distributed<journal_service> _the_journal_service;

inline distributed<journal_service>& get_journal_service() {
    return _the_journal_service;
}

inline journal_service& get_local_journal_service() {
    return _the_journal_service.local();
}

inline shared_ptr<journal_service> get_local_shared_journal_service() {
    return _the_journal_service.local_shared();
}

}//namespace hive
