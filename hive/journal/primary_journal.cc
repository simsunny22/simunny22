#include "hive/journal/primary_journal.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "hive/stream_service.hh"
#include "hive/volume_stream.hh"
#include "hive/extent_revision.hh"
#include "hive/extent_store.hh"
#include "hive/http/json11.hh"
#include "hive/http/seawreck.hh"
#include "hive/token_service.hh"
#include "hive/journal/journal_service.hh"
#include "hive/file_store.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/extent_revision_set.hh"
#include "hive/store/extent_store_proxy.hh"

#include "log.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "query-result-writer.hh"
#include "nway_merger.hh"
#include "cql3/column_identifier.hh"
#include "core/seastar.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"
#include "sstables/compaction.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "locator/simple_snitch.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "mutation_query.hh"
#include "sstable_mutation_readers.hh"
#include <core/fstream.hh>
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "schema_registry.hh"
#include "service/priority_manager.hh"
#include "schema_builder.hh"
#include "checked-file-impl.hh"
#include "disk-error-handler.hh"
#include <msgpack.hpp>
#include "hive/hive_service.hh"
#include "hive/system_service/system_service.hh"

#include "hive/http/json11.hh"
using namespace std::chrono_literals;

namespace hive {

static logging::logger logger("primary_journal");


primary_journal::primary_journal(sstring volume_id
        , journal_config config
        , uint64_t volume_segments_token)
    : _volume_id(volume_id)
    , _schema(hive::get_local_journal_service().get_schema())
    , _config(config)
    , _segments(make_lw_shared<journal_segment_list>([this] {return seal_active_segment(); }
                , [this]() {return new_segment(); }))
    , _rwrite_semaphore(1)
    , _volume_segments_token(volume_segments_token) 
    , _write_order_id(1)
{
    if (!_config.enable_disk_writes) {
        logger.warn("Writes disabled, column family no durable.");
    }

    init();
}


primary_journal::primary_journal(sstring volume_id
        , journal_config config
        , uint64_t volume_segments_token
        , std::vector<lw_shared_ptr<journal_segment>> segments)
    : _volume_id(volume_id)
    , _schema(hive::get_local_journal_service().get_schema())
    , _config(config)
    , _segments(make_lw_shared<journal_segment_list>([this] {return seal_active_segment(); }
                , [this]() {return new_segment(); }, segments))
    , _rwrite_semaphore(1)
    , _volume_segments_token(volume_segments_token) 
    , _write_order_id(1)
{
    if (!_config.enable_disk_writes) {
        logger.warn("Writes disabled, column family no durable.");
    }

    init();
}

primary_journal::~primary_journal() {
}

std::ostream& operator<<(std::ostream& out, const primary_journal& cf) {
    return fprint(out, "{primary_journal:[volume_id:%s]}", cf._volume_id);
}

void primary_journal::init(){
    uint64_t timing_drain_interval = _config.timing_drain_interval_in_seconds;         
    _timer.set_callback(std::bind(&primary_journal::timing_drain, this, timing_drain_interval));
    _timer.arm(
        lowres_clock::now() + std::chrono::seconds(timing_drain_interval)
        , std::experimental::optional<lowres_clock::duration>{std::chrono::seconds(timing_drain_interval)}
    );
}


future<> primary_journal::timing_drain(uint64_t interval){
    if(_rebuild_journal)
        return make_ready_future<>();

    if(!_segments->size())
        return make_ready_future<>();

    if( _segments->active_segment().empty())
        return make_ready_future<>();

    auto& system_service = get_local_system_service();
    auto now_tp = system_service.get_system_time_ptr()->get_now_tp();
    auto diff = system_time::get_seconds_diff(now_tp, _write_tp);

    if(diff <= interval)
        return make_ready_future<>();

    return _rwrite_semaphore.wait(1).then([this](){
        return get_segments_token().then([this](){
            return _segments->add_segment().then([this](){
                auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);
                drainer_ptr->drain();
                return make_ready_future<>();
            });
        }).finally([this](){
            _rwrite_semaphore.signal(1); 
        });
    });
}

future<> primary_journal::init_for_rebuild(){
    if(_rebuild_journal){
        sstring error_info = "volume_id:" + _volume_id + " is rebuilding"; 
        return make_exception_future<>(std::runtime_error(error_info));
    }

    auto size = _segments->size();
    if(size){
        return force_drain_for_rebuild(size);
    }
    return make_ready_future<>();
}



future<lw_shared_ptr<journal_segment>> primary_journal::new_segment() {
    return journal_segment::make_primary_segment(_volume_id);
}

future<> primary_journal::seal_active_segment(){
    logger.debug("[{}] start, volume_id:{}, segments.size:{}", __func__, _volume_id, _segments->size());
    auto latest_segment = _segments->back();
    if (latest_segment->empty()) {
        logger.debug("[{}] do nothing, because latest_segment is empty", __func__);
        return make_ready_future<>();
    }

    return get_segments_token().then([this](){
        return _segments->add_segment().then([this](){
            auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);
            drainer_ptr->drain();  //tododl:red if add_segment error will not call drain() ?
            return make_ready_future<>(); 
        });

    });
}


future<> primary_journal::write_journal(volume_revision revision){
    if(stopping_volume_driver){
        sstring error_info = "stopping volume driver, so can not write";
        error_info += ", volume_id:" + _volume_id; 
        throw std::runtime_error(error_info);
    }
       
    return _rwrite_semaphore.wait(1).then([revision=std::move(revision), this]() mutable{
        if(_segments->size() == 0){
            return get_segments_token().then([this, revision=std::move(revision)]()mutable{
                return _segments->add_segment().then([this, revision=std::move(revision)]()mutable{
                    return do_write_journal(std::move(revision));
                });
            }).finally([this](){
                _rwrite_semaphore.signal(1);
            });
        }
        return do_write_journal(std::move(revision)).finally([this](){
            _rwrite_semaphore.signal(1);
        });
    });
}

future<> primary_journal::do_write_journal(volume_revision revision){
    auto& segment = _segments->active_segment();
    return segment.write_primary_segment(std::move(revision)).then([this](){
        auto& system_service = hive::get_local_system_service();
        _write_tp = system_service.get_system_time_ptr()->get_now_tp();
        return _segments->seal_on_overflow();
    });
}

future<std::vector<volume_revision>> primary_journal::read_journal(hive_read_subcommand read_subcmd){
    logger.debug("[{}] start, read_subcmd:{}", __func__, read_subcmd);
    if(_segments->size() == 0){
        logger.warn("primary_journal::read_data _segments is empty, perphers not write");
        std::vector<volume_revision> revisions;
        return make_ready_future<std::vector<volume_revision>>(std::move(revisions));
    } 

    auto segments = _segments->all_segments();
    return do_with(std::move(segments), [this, read_subcmd](auto& segments){
        std::vector<future<std::vector<volume_revision>>> futs;
        logger.debug("[{}] segments.size:{}", __func__, segments.size());
        for(auto segment : segments){
            auto fut = segment->read_segment(read_subcmd);
            futs.push_back(std::move(fut));
        }

        
        return when_all(futs.begin(), futs.end()).then([](auto futs){
            std::vector<volume_revision> revisions;
            for(auto& fut: futs){
                auto fut_revisions = fut.get0();
                for(auto& revision: fut_revisions){
                    revisions.push_back(std::move(revision));
                }
            }

            logger.debug("[primary_journal::read_journal] done");
            return make_ready_future<std::vector<volume_revision>>(std::move(revisions));
        });
    });
}


future<> primary_journal::stop_driver() {
    logger.debug("primary_journal::stop .... stopping_state:{}", stopping_volume_driver);
    if(stopping_volume_driver){
        sstring error_info = "volume_id:" + _volume_id + "is stopping";
        throw std::runtime_error(error_info);
    }
    #if 0
    return _commitlog->shutdown().then([this](){
        //tododl:yellow how to do this???
        //seal_active_memtable();
        return make_ready_future<>();
    });
    #endif
    stopping_volume_driver = true;
    
    return repeat([this](){
        return sleep(1s).then([this](){
            
            auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);
            return drainer_ptr->check_drain_task_over().then([this, drainer_ptr](auto finish){
                if(finish){
                    return _segments->discard_all_segments().then([this, drainer_ptr](){
                        uint64_t pending_drains = (uint64_t)drainer_ptr->pending_drains();
                        logger.error("primary_journal::stop....pendding_drians:{}", pending_drains);
                        return this->release_segments_token(pending_drains).then([this](){
                            stopping_volume_driver = false;
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        });
                    });
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }); 
        });
    });
}

future<> primary_journal::stop(){
    return make_ready_future<>();
}


void primary_journal::maybe_force_drain_done() {
    _stats.force_pending_drains ? _stats.force_pending_drains-- : _stats.force_pending_drains;
    if(_force_drain && _stats.force_pending_drains == 0) {
        _rebuild_journal = false;
        _force_drain = false;   
        _force_drain_promise.set_value(); 
    }
}

void primary_journal::maybe_force_drain_exception() {
    if(_force_drain) {
       _force_drain = false;   
       _force_drain_promise.set_exception(std::current_exception());
    }
}


future<> primary_journal::force_drain() {

    auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);

    if(_force_drain || drainer_ptr->get_force_drain_stats()){
        logger.error("[{}] start, _force_drain:{}, config for force_drain_status:{}"
            , __func__, _force_drain, drainer_ptr->get_force_drain_stats());
        sstring error_info = "volume_id:" + _volume_id + " is drianning"; 
        return make_exception_future<>(std::runtime_error(error_info));
    }
    
    if(_segments->size() <= 0){
        logger.debug("[{}] do nothing, because _segments.size = 0, volume_id:{}", __func__, _volume_id);
        return make_ready_future<>();
    }
    
    auto old_segment = _segments->front(); 
    if (old_segment->empty()) {
        logger.debug("[{}] do nothing, because old_segment is empty, volume_id:{}", __func__, _volume_id);
        return make_ready_future<>();
    }

    auto new_segment = _segments->back();
    _force_drain = true;
    if(!new_segment->empty()){
        return get_segments_token().then([this, drainer_ptr](){
            return _segments->add_segment().then([this, drainer_ptr](){
               //auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);
               drainer_ptr->drain();
               return make_ready_future<>();
            }).then([this, drainer_ptr](){
                return drainer_ptr->waiting_force_drain().then([this](){
                    _force_drain = false;
                    return make_ready_future<>();
                });
            });
        });
    }

    return drainer_ptr->waiting_force_drain().then([this](){
        _force_drain = false;
        return make_ready_future<>();
    });

}


future<> primary_journal::force_drain_for_rebuild(uint64_t size){
    _rebuild_journal = true;
    auto drainer_ptr = hive::get_local_stream_service().find_drainer(_volume_id);
    drainer_ptr->drain(size);
    return drainer_ptr->waiting_force_drain().then([this](){
        _rebuild_journal = false;
        return make_ready_future<>();
    });
}

future<> primary_journal::waiting_force_drain(uint64_t waiting) {
    assert(waiting != 0);
    
    _force_drain_promise = std::move(promise<>());
    _stats.force_pending_drains += waiting;
    return _force_drain_promise.get_future();
}

uint64_t primary_journal::register_write_context(replay_position rp){
    ++_write_order_id;
    if(_write_order_id <= 0){
        //if inner here at least proved this program is very strong
        //_write_order_id type is uint64_t, max value is: 18446744073709551615 
        //if _write_order_id is 18446744073709551615 , then ++_write_order_id will is 0
        //to avoid error we set exception to writes that already allocated rp 
        mark_all_write_context_exception("start the new round");
        _write_order_id = 1;
    }
    write_context context;
    context.write_order_id = _write_order_id;
    context.allocated_rp = rp;
    _write_context_queue.insert(std::make_pair(context.write_order_id, std::move(context)));
    return _write_order_id;
}   

replay_position primary_journal::get_replay_position(uint64_t write_order_id){
    auto itor = _write_context_queue.find(write_order_id);
    if(itor != _write_context_queue.end()){
        return itor->second.allocated_rp;
    }else {
        throw std::runtime_error("can not find replay positon , write_order_id:" 
            + std::to_string(write_order_id));
    }
}

void primary_journal::mark_write_context_exception(uint64_t write_order_id, sstring exception_info){
    auto itor = _write_context_queue.find(write_order_id);
    if(itor != _write_context_queue.end()){
        itor->second.mark_exception(exception_info);
    }else{
        logger.error("[{}] warn, can not find write_order_id:{}", __func__, write_order_id); 
    }
}

void primary_journal::mark_all_write_context_exception(sstring exception_info){
    for(auto& pair : _write_context_queue){
        pair.second.mark_exception(exception_info); 
    }
}

void primary_journal::maybe_has_exception(uint64_t write_order_id){
    auto itor = _write_context_queue.find(write_order_id);
    if(itor != _write_context_queue.end()){
        if(itor->second.has_exception){
            throw std::runtime_error(itor->second.exception_info);
        }
    }else {
        logger.error("[{}] warn, can not find write_order_id:{}", __func__, write_order_id); 
    }
}

template <typename Func> 
futurize_t<std::result_of_t<Func ()>>
primary_journal::apply_in_order(uint64_t write_order_id, Func&& func){
    auto itor = _write_context_queue.find(write_order_id);
    if(itor == _write_context_queue.begin() || itor == _write_context_queue.end()){
        return func();
    }else{
        return (--itor)->second.wait().then([func = std::forward<Func>(func)](){
            return func();
        });
    }
}

void primary_journal::trigger_apply_in_order(uint64_t write_order_id){
    auto itor = _write_context_queue.find(write_order_id);
    if(itor != _write_context_queue.end()){
        itor->second.set_value();
        erase_inactive_before(write_order_id);
    }else{
        logger.error("[{}] error, can not find write_order_id:{}", __func__, write_order_id); 
    }
}

void primary_journal::erase_inactive_before(uint64_t write_order_id){
    for(auto& pair : _write_context_queue){
        if(pair.first == write_order_id || pair.second.active){
            break; 
        }
        pair.second.set_value();
        _write_context_queue.erase(pair.first);
    }
}

future<> primary_journal::get_segments_token(){
    return _volume_segments_token.wait(1).then([this](){
        auto& journal_service = get_local_journal_service();
        return journal_service.get_total_segments_token();
    });
}

future<> primary_journal::release_segments_token(uint64_t i){
    if(!_rebuild_journal){
        _volume_segments_token.signal(i);
        auto& journal_service = get_local_journal_service();
        return journal_service.release_total_segments_token(i);
    }
    return make_ready_future<>();
}

bool primary_journal::is_stopping_driver(){
    return stopping_volume_driver;
}


future<std::vector<volume_revision_view>>
primary_journal::get_volume_revision_view(sstring segment_id){
    auto segment_ptr = _segments->get_segment(segment_id);
    return segment_ptr->scan();
}

future<std::map<sstring, std::vector<revision_data>>>
primary_journal::get_journal_data(
    sstring segment_id, 
    std::map<sstring, std::vector<extent_group_revision>> revision_map){
    auto segment_ptr = _segments->get_segment(segment_id);
    return segment_ptr->get_revision_data(std::move(revision_map));
}

} //namespace hive
