#pragma once
#include "dht/i_partitioner.hh"
#include "locator/abstract_replication_strategy.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/range/algorithm/find.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include "cql3/column_specification.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "keys.hh"
#include "mutation.hh"
#include "memtable.hh"
#include <list>
#include "mutation_reader.hh"
#include "row_cache.hh"
#include "compaction_strategy.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/histogram.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/rwlock.hh>
#include "core/shared_mutex.hh"
#include <mutex>
#include "database.hh"
#include "mutation.hh"

#include "hive/commitlog/hive_commitlog.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/extent_datum.hh"
#include "hive/drain/drain_manager.hh"
#include "hive/drain/volume_drainer.hh"
#include "hive/extent_revision.hh"
#include "hive/extent_revision_set.hh"
#include "hive/group_revision_set.hh"
#include "hive/hive_request.hh"
#include "hive/journal/journal_segment.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/store/hint_entry.hh"
#include "hive/journal/volume_revision.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/journal/revision_data.hh"

class frozen_mutation;

namespace hive {

class journal_segment_list {
    using shared_segment = lw_shared_ptr<journal_segment>;
    std::vector<shared_segment> _segments;
    std::function<future<> ()> _seal_fn;
    std::function<future<shared_segment> ()> _new_segment;
public:
    journal_segment_list(
            std::function<future<> ()> seal_fn
            , std::function<future<shared_segment> ()> new_segment)
        : _segments({})
        , _seal_fn(seal_fn)
        , _new_segment(new_segment){}

    journal_segment_list(
            std::function<future<> ()> seal_fn
            , std::function<future<shared_segment> ()> new_segment
            , std::vector<shared_segment> segments)
        : _segments(std::move(segments))
        , _seal_fn(seal_fn)
        , _new_segment(new_segment){}

    shared_segment front() {
        return _segments.front();
    }

    shared_segment back() {
        return _segments.back();
    }

    // The caller has to make sure the element exist before calling this.
    void erase(const shared_segment& element) {
        _segments.erase(boost::range::find(_segments, element));
    }
    void clear() {
        _segments.clear();
    }

    size_t size() const {
        return _segments.size();
    }

    //future<> seal_active_memtable() {
    //    return _seal_fn();
    //}

    auto begin() noexcept {
        return _segments.begin();
    }

    auto begin() const noexcept {
        return _segments.begin();
    }

    auto end() noexcept {
        return _segments.end();
    }

    auto end() const noexcept {
        return _segments.end();
    }

    journal_segment& active_segment() {
        if( 0 == _segments.size() ){
            assert(false); 
        }
        return *_segments.back();
    }

    std::vector<shared_segment> all_segments(){
        if( 0 == _segments.size()){
            assert(false);
        }
        return _segments;
    }

    future<> add_segment() {
        return _new_segment().then([this](auto&& segment_ptr){
           _segments.emplace_back(segment_ptr);
           return make_ready_future<>();
        });
        //_segments.emplace_back(_new_segment());
    }

    bool should_flush() {
        return active_segment().should_flush();
    }

    future<> seal_on_overflow() {
        if (should_flush()) {
            return _seal_fn();
        }
        return make_ready_future<>();
    }

    future<> discard_all_segments(){
        std::vector<future<>> futs;
        for(auto& segment: _segments){
            auto fut = segment->discard_primary_segment();
            futs.push_back(std::move(fut));
        }
        
        return when_all(futs.begin(), futs.end()).discard_result();
    }

    lw_shared_ptr<journal_segment> get_segment(sstring segment_id){
        for(auto& segment : _segments){
            if(segment->get_id() == segment_id){
                return segment; 
            }
        } 
        //TODO:yellow need use exception class
        auto error_info = sprint("not found segment(%s)", segment_id);
        throw std::runtime_error(error_info);
    }

}; //class journal_segment_list


struct hive_stats;
struct journal_config {
    sstring datadir;
    bool enable_disk_writes = true;
    bool enable_disk_reads = true;
    bool enable_cache = true;
    bool enable_commitlog = true;
    size_t max_memtable_size = 5'000'000;
    logalloc::region_group* dirty_memory_region_group = nullptr;
    logalloc::region_group* streaming_dirty_memory_region_group = nullptr;
    hive::hive_stats* hive_stats = nullptr;
    sstring pithos_server_url = "";
    bool debug_mode = false;
    uint32_t write_debug_level = 0;
    bool commit_to_pithos_for_drain_done = false;
    bool disable_memtable = true;
    uint64_t total_segments_token = 80;
    float volume_segments_token_ratio = 0.5f;
    uint64_t timing_drain_interval_in_seconds = 900;
};

struct journal_stats {
    int64_t force_pending_drains = 0;
    int64_t pending_drains = 0;
    utils::ihistogram reads{256};
    utils::ihistogram writes{256};
    //int64_t memtable_switch_count = 0;
    //int64_t live_disk_space_used = 0;
    //int64_t total_disk_space_used = 0;
    //utils::ihistogram tombstone_scanned;
    //utils::ihistogram live_scanned;
    //int64_t draining_count = 0;
};

class primary_journal {
private:
    sstring _volume_id;
    schema_ptr _schema;
    journal_config _config;
    journal_stats _stats;
    lw_shared_ptr<journal_segment_list> _segments;
    
    //for limit rwrite
    semaphore _rwrite_semaphore;
    semaphore _volume_segments_token;

    //for force_drain
    bool _force_drain = false;
    promise<> _force_drain_promise;

    //for timing_drain
    timer<lowres_clock> _timer;
    std::chrono::system_clock::time_point _write_tp;

    //for rebuild_driver
    bool _rebuild_journal = false;

    //for stop_driver
    bool stopping_volume_driver = false;
   
    uint64_t _write_order_id; //must start from 1 in the construct function
    struct write_context {
        uint64_t write_order_id; 
        replay_position allocated_rp;
        promise<> pr;
        bool active = true;
        bool has_exception = false;
        sstring exception_info = ""; 

        void set_value(){
            if(active){
                pr.set_value(); 
                active = false;
            }
        }
        future<> wait(){
            return pr.get_future(); 
        }
        void mark_exception(sstring exception){
            has_exception = true;
            exception_info = exception;
        }
    };
    std::map<uint64_t, write_context, std::less<uint64_t>>  _write_context_queue;

private:
    future<lw_shared_ptr<journal_segment>> new_segment();
    future<> seal_active_segment();
    future<> try_drain_segment(lw_shared_ptr<journal_segment> old_segment);

    future<> get_volume_segments_token();
    future<> release_volume_segments_token();


public:
    primary_journal(
        sstring volume_id
        , journal_config cfg
        , uint64_t volume_segments_token
        , std::vector<lw_shared_ptr<journal_segment>> segments);
    primary_journal(
        sstring volume_id
        , journal_config cfg
        , uint64_t volume_segments_token);
    primary_journal(primary_journal&&) = delete; // 'this' is being captured during construction
    ~primary_journal();
    
    future<> write_journal(volume_revision revision);
    future<> do_write_journal(volume_revision revision);
    future<std::vector<volume_revision>> read_journal(hive_read_subcommand read_subcmd);

    lw_shared_ptr<journal_segment_list> get_segments(){
        return _segments;
    }

    future<> stop();
    future<> stop_driver();
    const schema_ptr& schema() const { return _schema; }
    const journal_stats& get_stats() const {
        return _stats;
    }
   

    //wztest
    future<> init_for_rebuild();
    void init();
    future<> timing_drain(uint64_t interval);
    future<> get_segments_token();
    future<> release_segments_token(uint64_t i = 1);
    bool is_stopping_driver();

    ///////////////
    // for drain
    ///////////////
    future<> force_drain();
    future<> force_drain_for_rebuild(uint64_t size);
    future<> waiting_force_drain(uint64_t waiting);
    void maybe_force_drain_done();
    void maybe_force_drain_exception();

    ////////////////////////////////////////////////
    //for apply memtable in order by write_order_id 
    ////////////////////////////////////////////////
    uint64_t register_write_context(replay_position rp);
    replay_position get_replay_position(uint64_t write_order_id);
    void mark_write_context_exception(uint64_t write_order_id, sstring exception_info);
    void mark_all_write_context_exception(sstring exception);
    void maybe_has_exception(uint64_t write_order_id);

    template <typename Func>
    futurize_t<std::result_of_t<Func ()>> apply_in_order(uint64_t write_order_id, Func&& func);

    void trigger_apply_in_order(uint64_t write_order_id);
    void erase_inactive_before(uint64_t write_order_id);

    future<std::vector<volume_revision_view>>
    get_volume_revision_view(sstring segment_id);

    future<std::map<sstring, std::vector<revision_data>>>
    get_journal_data(
       sstring segment_id, 
       std::map<sstring, std::vector<extent_group_revision>> revision_map
    );

    friend std::ostream& operator<<(std::ostream& out, const primary_journal& cf);
};

} //namespace hive
