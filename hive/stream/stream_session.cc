#include "log.hh"
#include "hive/messaging_service.hh"
#include "message/messaging_service.hh"
#include "hive/stream/stream_session.hh"
#include "hive/stream/prepare_message.hh"
#include "hive/stream/stream_result_future.hh"
#include "hive/stream/stream_manager.hh"
#include "mutation_reader.hh"
#include "dht/i_partitioner.hh"
#include "database.hh"
#include "utils/fb_utilities.hh"
#include "hive/stream/stream_plan.hh"
#include "core/sleep.hh"
#include "service/storage_service.hh"
#include "core/thread.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/priority_manager.hh"
#include "query-request.hh"
#include "schema_registry.hh"
#include "hive/stream/stream_state.hh"
#include "hive/stream/stream_session_state.hh"
#include "hive/stream/stream_exception.hh"
#include "hive/stream/migrate_chunk.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/migrate_extent_journal_task.hh"
#include "hive/stream/test_message.hh"
#include "hive/extent_store.hh"
#include "hive/migration_manager.hh"
#include "seastar/core/sleep.hh"

using namespace std::chrono_literals;
using response_id_type = uint64_t;
namespace hive {

logging::logger logger("stream_session");

static auto get_stream_result_future(utils::UUID plan_id) {
    auto& sm = get_local_stream_manager();
    auto f = sm.get_sending_stream(plan_id);
    if (!f) {
        f = sm.get_receiving_stream(plan_id);
    }
    return f;
}

static auto get_session(utils::UUID plan_id, gms::inet_address from, const char* verb) {
    logger.debug("get_session start, plan_id:{}, got {} from {}, local_cpu_id:{}"
        , plan_id, verb, from, engine().cpu_id());
    auto sr = get_stream_result_future(plan_id);
    if (!sr) {
        auto err = sprint("get_session plan_id:%s, got %s from %s, can not find stream_result_future[maybe_timeout]"
            , plan_id, verb, from);
        logger.warn(err.c_str());
        throw std::runtime_error(err);
    }
    auto coordinator = sr->get_coordinator();
    if (!coordinator) {
        auto err = sprint("get_session, plan_id:%s, got %s from %s,can not find coordinator", plan_id, verb, from);
        logger.warn(err.c_str());
        throw std::runtime_error(err);
    }
    return coordinator->get_or_create_session(from);
}



future<> stream_session::receive_extent_group_chunk(migrate_chunk chunk){
    assert(migrate_type::MIGRATE_EXTENT_GROUP == chunk.type);
    sstring extent_group_id = chunk.extent_group_id;
    auto shard_id = hive::get_local_extent_store().shard_of(extent_group_id);
    auto& extent_store = hive::get_extent_store();
    return extent_store.invoke_on(shard_id, [chunk=std::move(chunk)](auto& shard_extent_store)mutable{
        return shard_extent_store.write_extent_group_exactly(chunk.extent_group_id
                                                           , chunk.offset
                                                           , chunk.length
                                                           , std::move(chunk.data)
                                                           , chunk.dst_disk_id); 
    }); 
}

future<> stream_session::receive_extent_journal_chunk(migrate_chunk chunk){
    assert(migrate_type::MIGRATE_EXTENT_JOURNAL == chunk.type);
    return get_local_migration_manager().write_extent_journal_chunk(std::move(chunk));    
}

future<> stream_session::receive_chunk(migrate_chunk chunk){
    if(migrate_type::MIGRATE_EXTENT_GROUP == chunk.type
        || migrate_type::REPLICATE_EXTENT_GROUP == chunk.type){
        return receive_extent_group_chunk(std::move(chunk)); 
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == chunk.type){
        return receive_extent_journal_chunk(std::move(chunk)); 
    }else{
        auto error_info = "[receive_chunk]error, unkown chunk type";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info)); 
    }
}

void stream_session::init_messaging_service_handler() {
    ms().register_hive_prepare_message([] (const rpc::client_info& cinfo, prepare_message msg, UUID plan_id, sstring description) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto dst_cpu_id = engine().cpu_id();
        logger.debug("register_hive_prepare_message,receive msg from:{}, src_cpu_id:{}, dst_cpu_id:{}"
            , from, src_cpu_id, dst_cpu_id);
        return smp::submit_to(dst_cpu_id, [msg=std::move(msg), plan_id, description=std::move(description)
                , from, src_cpu_id] () mutable {
            auto sr = stream_result_future::init_receiving_side(plan_id, description, from);
            auto session = get_session(plan_id, from, "PREPARE_MESSAGE");
            session->init(sr);
            session->dst_cpu_id = src_cpu_id;
            return session->prepare(std::move(msg.requests), std::move(msg.summaries));
        });
    });
    ms().register_hive_prepare_message_done([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("register_hive_prepare_message_done, receive msg from:{}, src_cpu_id:{}, dst_cpu_id:{}"
            , from, src_cpu_id, dst_cpu_id);
        return smp::submit_to(dst_cpu_id, [plan_id, from] () mutable {
            auto session = get_session(plan_id, from, "PREPARE_DONE_MESSAGE");
            session->follower_start_sent();
            return make_ready_future<>();
        });
    });
    ms().register_stream_chunk([] (const rpc::client_info& cinfo, UUID plan_id, migrate_chunk chunk, unsigned dst_cpu_id) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& gms_from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        auto from = net::messaging_service::get_source(cinfo);
        logger.debug("register_stream_chunk, receive msg from:{}, src_cpu_id:{}, dst_cpu_id:{}, plan_id:{}, migrate_chunk:{}"
            , gms_from, src_cpu_id, dst_cpu_id, plan_id, chunk);

        return smp::submit_to(dst_cpu_id, [plan_id, chunk=std::move(chunk), gms_from, from]()mutable {
            auto session = get_session(plan_id, gms_from, "HIVE_STREAM_CHUNK"); //for check session time out
            get_local_stream_manager().update_progress(plan_id, from.addr, progress_info::direction::IN, chunk.length);
            return session->receive_chunk(std::move(chunk));
        });
    });

    ms().register_stream_chunk_done([] (const rpc::client_info& cinfo, UUID plan_id, sstring extent_group_id, unsigned dst_cpu_id) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("register_stream_chunk_done, receive msg from:{}, src_cpu_id:{}, dst_cpu_id:{}"
            , from, src_cpu_id, dst_cpu_id);
        return smp::submit_to(dst_cpu_id, [plan_id, extent_group_id, from] () mutable {
            auto session = get_session(plan_id, from, "STREAM_CHUNK_DONE");
            session->receive_task_completed(extent_group_id);
        });
    });

    ms().register_hive_complete_message([] (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id) {
        const auto& src_cpu_id = cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id");
        const auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("register_hive_complete_message, receive msg from:{}, src_cpu_id:{}, dst_cpu_id:{}"
            , from, src_cpu_id, dst_cpu_id);
        // Be compatible with old version. Do nothing but return a ready future.
        return make_ready_future<>();
    });

    ms().register_test_message([] (const rpc::client_info& cinfo, test_message msg) {
        logger.debug("receive msg[TEST_MESSAGE]");
        auto from = hive::messaging_service::get_source(cinfo);
        logger.debug("receive data size {}, from {}", msg.data.size(), from);
        return sleep(1s).then([](){
            make_ready_future<>();
        });
    });
}

stream_session::stream_session() = default;

stream_session::stream_session(inet_address peer_)
    : peer(peer_) {
    //this.metrics = StreamingMetrics.get(connecting);
}

stream_session::~stream_session() = default;

future<> stream_session::init_streaming_service(uint32_t stream_chunk_send_limit) {
    // #293 - do not stop anything
    // engine().at_exit([] {
    //     return get_stream_manager().stop();
    // });
    return get_stream_manager().start(stream_chunk_send_limit).then([] {
        gms::get_local_gossiper().register_(get_local_stream_manager().shared_from_this());
        return get_stream_manager().invoke_on_all([] (auto& manager) {
            init_messaging_service_handler();
        });
    });
}

future<> stream_session::on_initialization_complete() {
    // send prepare message
    set_state(stream_session_state::PREPARING);
    auto prepare = prepare_message();
    std::copy(_requests.begin(), _requests.end(), std::back_inserter(prepare.requests));
    for (auto& transfer : _transfers) {
        prepare.summaries.emplace_back(transfer.second->get_summary());
    }
    
    auto peer_id = msg_addr{this->peer, 0};
    logger.debug("on_initialization_complete, plan_id:{}, will send HIVE_PREPARE_MESSAGE to {}", plan_id(), peer_id);
    return ms().send_hive_prepare_message(peer_id, std::move(prepare), plan_id(), description()).then_wrapped([this, peer_id] (auto&& f) {
        try {
            auto msg = f.get0();
            this->dst_cpu_id = msg.dst_cpu_id;
            logger.debug("on_initialization_complete, plan_id:{} got HIVE_PREPARE_MESSAGE reply from {}, dst_cpu_id:{}"
                , this->plan_id(), this->peer, this->dst_cpu_id);
            for (auto& summary : msg.summaries) {
                this->prepare_receiving(summary);
            }
            
            _stream_result->handle_session_prepared(this->shared_from_this());
        } catch (...) {
            logger.error("on_initialization_complete, plan_id:{}, fail to send HIVE_PREPARE_MESSAGE to {}, exception:{}"
                , this->plan_id(), peer_id, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([this, peer_id] {
        auto plan_id = this->plan_id();
        logger.debug("[on_initialization_complete, plan_id:{}, will send HIVE_PREPARE_MESSAGE_DONE to {}", plan_id, peer_id);
        return ms().send_hive_prepare_message_done(peer_id, plan_id, this->dst_cpu_id).then([this] {
            logger.debug("[on_initialization_complete, plan_id:{}, got HIVe_PREPARE_MESSAGE_DONE reply from {}, dst_cpu_id:{}"
                , this->plan_id(), this->peer, this->dst_cpu_id);
        }).handle_exception([peer_id, plan_id] (auto ep) {
            logger.error("[on_initialization_complete, plan_id:{}, fail to send HIVE_PREPARE_MESSAGE_DONE to {}, exception:{}"
                , plan_id, peer_id, ep);
            std::rethrow_exception(ep);
        });
    }).then([this] {
        logger.debug("on_initialization_complete, plan_id:{}, initiator starts will to sent", this->plan_id());
        this->start_streaming_files();
    });
}

void stream_session::on_error() {
    logger.error("{} start, plan_id:{} streaming error occurred", __func__, plan_id());
    // fail session
    close_session(stream_session_state::FAILED);
}

future<> stream_session::prepare_for_summary(std::vector<stream_summary> summaries){
    std::vector<future<>> futures;
    for (auto& summary : summaries) {
        logger.debug("[{}], plan_id:{} prepare stream_summary={}", __func__, this->plan_id(), summary);
        if(migrate_type::MIGRATE_EXTENT_GROUP == summary.type
            || migrate_type::REPLICATE_EXTENT_GROUP == summary.type ){

            //tododl:red check file exist???
            sstring extent_group_id = summary.extent_group_id;
            sstring dst_disk_id = summary.dst_disk_id;
            size_t total_size = summary.total_size;
            auto fut = touch_extent_group_file(extent_group_id, dst_disk_id, total_size).then(
                    [this, summary](){
                prepare_receiving(summary);
            });

            futures.push_back(std::move(fut));
        }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == summary.type){
            sstring commitlog_file_name = summary.commitlog_file_name;
            migrate_scene scene = summary.scene;
            touch_commitlog_file(scene, commitlog_file_name, summary.total_size);
        }else {
            logger.error("[{}]error, unknown migrate type", __func__); 
        }
    }

    return when_all(futures.begin(), futures.end()).then([](std::vector<future<>> rets){
        try{
            for(auto& f : rets){
                f.get(); 
            }
        } catch(...) {
            throw; 
        }
    });
}

// Only follower calls this function upon receiving of prepare_message from initiator
future<prepare_message> stream_session::prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries) {
    auto plan_id = this->plan_id();
    logger.debug("{} start, plan_id:{}, prepare requests requests_num:{}, summaries_num:{}"
        , __func__, plan_id, requests.size(), summaries.size());

    set_state(stream_session_state::PREPARING);
    
    //tododl:yellow not support pull file now
    //for (auto& request : requests) {
        //add_stream_transfer(migrate_params_entry);
    //}

    return prepare_for_summary(summaries).then([this, requests](){
        // Always send a prepare_message back to follower
        prepare_message prepare;
        //tododl:yellow not support pull file now
        //if (!requests.empty()) {
        //    for (auto& transfer : _transfers) {
        //        auto& task = transfer.second;
        //        prepare.summaries.emplace_back(task->get_summary());
        //    }
        //}
        prepare.dst_cpu_id = engine().cpu_id();;
        _stream_result->handle_session_prepared(shared_from_this());
        return make_ready_future<prepare_message>(std::move(prepare));
    });
}

future<> stream_session::touch_extent_group_file(sstring extent_group_id
                                               , sstring dst_disk_id
                                               , size_t truncate_size){
    auto& local_extent_store = hive::get_local_extent_store();
    auto shard_id = local_extent_store.shard_of(extent_group_id);
    auto& extent_store = hive::get_extent_store();
    
    return extent_store.invoke_on(shard_id, [extent_group_id, dst_disk_id, truncate_size]
            (auto& shard_extent_store){
        return shard_extent_store.touch_extent_group_file(extent_group_id, dst_disk_id, truncate_size);
    });
}

future<> stream_session::touch_commitlog_file(migrate_scene scene, sstring commitlog_file_name, size_t truncate_size){
    return get_local_migration_manager().touch_commitlog_file(scene, commitlog_file_name, truncate_size);    
}

void stream_session::follower_start_sent() {
    logger.debug("{} start, plan_id:{} follower start to sent", __func__, this->plan_id());
    this->start_streaming_files();
}

void stream_session::session_failed() {
    logger.debug("{}, start, streaming error occurred", __func__);
    close_session(stream_session_state::FAILED);
}

session_info stream_session::make_session_info() {
    std::vector<stream_summary> receiving_summaries;
    for (auto& receiver : _receivers) {
        receiving_summaries.emplace_back(receiver.second.get_summary());
    }
    std::vector<stream_summary> transfer_summaries;
    for (auto& transfer : _transfers) {
        transfer_summaries.emplace_back(transfer.second->get_summary());
    }
    return session_info(peer, std::move(receiving_summaries), std::move(transfer_summaries), _state);
}

void stream_session::receive_task_completed(sstring task_id) {
    _receivers.erase(task_id);
    logger.debug("{} plan_id:{}, receive task_completed: task_id:{} done, stream_receive_task.size:{} stream_transfer_task.size:{}"              , __func__, plan_id(), task_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::transfer_task_completed(sstring task_id) {
    _transfers.erase(task_id);
    logger.debug("{} plan_id:{}, transfer task_completed: task_id:{} done, stream_receive_task.size:{} stream_transfer_task.size:{}"
        , __func__, plan_id(), task_id, _receivers.size(), _transfers.size());
    maybe_completed();
}

void stream_session::send_complete_message() {
    logger.debug("{} start", __func__);
    if (!_complete_sent) {
        _complete_sent = true;
    } else {
        return;
    }
    
    auto id = msg_addr{this->peer, this->dst_cpu_id};
    auto plan_id = this->plan_id();
    logger.debug("{} plan_id:{}, send HIVE_COMPLETE_MESSAGE to {}", __func__, plan_id, id);
    auto session = shared_from_this();
    this->ms().send_hive_complete_message(id, plan_id, this->dst_cpu_id).then([session, id, plan_id] {
        logger.debug("send_complete_message, plan_id:{}, got HIVE_COMPLETE_MESSAGE reply from {}", plan_id, id.addr);
    }).handle_exception([session, id, plan_id] (auto ep) {
        logger.warn("send_complete_message, plan_id:{}, error HIVE_COMPLETE_MESSAGE reply from {},exception:{}", plan_id, id.addr, ep);
        session->on_error();
    });
}

bool stream_session::maybe_completed() {
    bool completed = _receivers.empty() && _transfers.empty();
    if (completed) {
        send_complete_message();
        logger.debug("maybe_completed, plan_id:{}, {} -> complete: session={}, peer={}"
            , plan_id(), _state, this, peer);
        close_session(stream_session_state::COMPLETE);
    }
    return completed;
}

void stream_session::prepare_receiving(stream_summary summary) {
    if (summary.total_size > 0) {
        // FIXME: handle when cf_id already exists
        // tododl: yellow handle when extent_group_id already exists
        _receivers.emplace(summary.extent_group_id, stream_receive_task(shared_from_this(), summary.total_size));
    }
}

void stream_session::start_streaming_files() {
    logger.debug("{}, start, plan_id:{}, {} transfers to send", __func__, plan_id(), _transfers.size());
    if (!_transfers.empty()) {
        set_state(stream_session_state::STREAMING);
    }
    for (auto it = _transfers.begin(); it != _transfers.end(); it++) {
        it->second->start();
    }
}

void stream_session::add_stream_transfer(migrate_params_entry params) {
    logger.debug("[{}] start, migrate_params:{}", __func__, params);

    if(migrate_type::MIGRATE_EXTENT_GROUP == params.type
        || migrate_type::REPLICATE_EXTENT_GROUP == params.type ){
        std::shared_ptr<stream_task> task = std::make_shared<stream_transfer_task>(shared_from_this(), params);
        auto inserted = _transfers.emplace(params.extent_group_id, std::move(task)).second;
        assert(inserted);
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == params.type){
        sstring commitlog_file_name = params.commitlog_file_name;
        std::shared_ptr<stream_task> task = std::make_shared<migrate_extent_journal_task>(shared_from_this(), params);
        auto inserted = _transfers.emplace(commitlog_file_name, std::move(task)).second;
        assert(inserted);
    }else{
        auto error_info = "[add_stream_transfer] error, unknown migrate type";
        logger.error(error_info); 
        throw std::runtime_error(error_info); 
    }
}

void stream_session::close_session(stream_session_state final_state) {
    logger.debug("close_session, plan_id:{}, session={}, state={}, is_aborted={}"
        , plan_id(), this, final_state, _is_aborted);
    if (!_is_aborted) {
        _is_aborted = true;
        set_state(final_state);

        if (final_state == stream_session_state::FAILED) {
            for (auto& x : _transfers) {
                logger.debug("close_session, plan_id:{}, session={}, state={}, abort stream_transfer_task"
                    , plan_id(), this, final_state);
                x.second->abort();
            }
            for (auto& x : _receivers) {
                stream_receive_task& task = x.second;
                logger.debug("close_session, plan_id:{}, session={}, state={}, abort stream_receive_task"
                    , plan_id(), this, final_state);
                task.abort();
            }
        }

        // Note that we shouldn't block on this close because this method is called on the handler
        // incoming thread (so we would deadlock).
        //handler.close();
        _stream_result->handle_session_complete(shared_from_this());

        logger.debug("close_session, plan_id:{}, session={}, state={}, cancel keep_alive timer"
            , plan_id(), this, final_state);
        _keep_alive.cancel();
    }
}

void stream_session::start() {
    logger.debug("{} start", __func__);
    if (_requests.empty() && _transfers.empty()) {
        logger.debug("start, plan_id:{}, session does not have any tasks.", plan_id());
        close_session(stream_session_state::COMPLETE);
        return;
    }
    
    auto connecting = net::get_local_messaging_service().get_preferred_ip(peer);
    if (peer == connecting) {
        logger.debug("{}, plan_id:{} starting streaming to {}", __func__, plan_id(), peer);
    } else {
        logger.debug("{}, plan_id:{} starting streaming to {} through {}", __func__, plan_id(), peer, connecting);
    }
    on_initialization_complete().handle_exception([this] (auto ep) {
        logger.debug("start(), error, exception:{}", ep);
        this->on_error();
    });
}

void stream_session::init(shared_ptr<stream_result_future> stream_result_) {
    _stream_result = stream_result_;
    logger.debug("{} start, plan_id:{}", __func__, plan_id());
    _keep_alive.set_callback([this] {
        auto plan_id = this->plan_id();
        auto peer = this->peer;
        get_local_stream_manager().get_progress_on_all_shards(plan_id, peer).then([this, peer, plan_id] (stream_bytes sbytes) {
            if (this->_is_aborted) {
                logger.info("init, plan_id:{}, the session {} is closed, keep alive timer will do nothing", plan_id, this);
                return;
            }
            auto now = lowres_clock::now();
            logger.debug("init, plan_id:{}, keep alive timer callback sbytes old: tx={}, rx={} new: tx={} rx={}",
                    plan_id, this->_last_stream_bytes.bytes_sent, this->_last_stream_bytes.bytes_received,
                    sbytes.bytes_sent, sbytes.bytes_received);
            if (sbytes.bytes_sent > this->_last_stream_bytes.bytes_sent ||
                sbytes.bytes_received > this->_last_stream_bytes.bytes_received) {
                logger.debug("init, plan_id:{}, the session {} made progress with peer {}", plan_id, this, peer);
                // Progress has been made
                this->_last_stream_bytes = sbytes;
                this->_last_stream_progress = now;
                this->start_keep_alive_timer();
            } else if (now - this->_last_stream_progress >= this->_keep_alive_timeout) {
                // Timeout
                logger.debug("init, timeout_error, plan_id:{}, the session {} is idle for {} seconds, the peer {} is probably gone, close it",
                        plan_id, this, this->_keep_alive_timeout.count(), peer);
                this->on_error();
            } else {
                // Start the timer to check again
                logger.info("init, plan_id:{}, the session {} made no progress with peer {}", plan_id, this, peer);
                this->start_keep_alive_timer();
            }
        }).handle_exception([plan_id, peer, session = this->shared_from_this()] (auto ep) {
           logger.info("init, plan_id:{}, keep alive timer callback fails with peer {}: {}", plan_id, peer, ep);
        });
    });
    _last_stream_progress = lowres_clock::now();
    start_keep_alive_timer();
}

utils::UUID stream_session::plan_id() {
    return _stream_result ? _stream_result->plan_id : UUID();
}

sstring stream_session::description() {
    return _stream_result  ? _stream_result->description : "";
}

future<> stream_session::update_progress() {
    return get_local_stream_manager().get_progress_on_all_shards(plan_id(), peer).then([this] (auto sbytes) {
        auto bytes_sent = sbytes.bytes_sent;
        if (bytes_sent > 0) {
            auto tx = progress_info(this->peer, "txnofile", progress_info::direction::OUT, bytes_sent, bytes_sent);
            _session_info.update_progress(std::move(tx));
        }
        auto bytes_received = sbytes.bytes_received;
        if (bytes_received > 0) {
            auto rx = progress_info(this->peer, "rxnofile", progress_info::direction::IN, bytes_received, bytes_received);
            _session_info.update_progress(std::move(rx));
        }
    });
}

} // namespace hive 
