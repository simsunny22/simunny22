#include "hive/stream/stream_result_future.hh"
#include "hive/stream/stream_manager.hh"
#include "hive/stream/stream_exception.hh"
#include "log.hh"

namespace hive {

static logging::logger logger("stream_result_future");

future<stream_state> stream_result_future::init_sending_side(
    UUID plan_id_
    , sstring description_
    , std::vector<stream_event_handler*> listeners_
    , shared_ptr<stream_coordinator> coordinator_) {

    logger.debug("{} start, plan_id:{}, description:{}", __func__, plan_id_,  description_);
    
    auto sr = make_shared<stream_result_future>(plan_id_, description_, coordinator_);
    get_local_stream_manager().register_sending(sr);

    for (auto& listener : listeners_) {
        sr->add_event_listener(listener);
    }

    // Initialize and start all sessions
    for (auto& session : coordinator_->get_all_stream_sessions()) {
        session->init(sr);
    }
    coordinator_->connect_all_stream_sessions();

    return sr->_done.get_future();
}

shared_ptr<stream_result_future> stream_result_future::init_receiving_side(
    UUID plan_id
    , sstring description
    , inet_address from) {

    logger.debug("{} start, plan_id:{}, description:{}", __func__, plan_id,  description);
    auto& sm = get_local_stream_manager();
    auto sr = sm.get_receiving_stream(plan_id);
    if (sr) {
        auto err = sprint("error, {} plan_id:{}, got HIVE_PREPARE_MESSAGE from %s, description=%s,"
                          "stream_plan exists, duplicated message received?", __func__, plan_id, description, from);
        logger.warn(err.c_str());
        throw std::runtime_error(err);
    }
    logger.debug("{}, plan_id:{}, creating new streaming plan for {}, with {}", __func__, plan_id, description, from);
    bool is_receiving = true;
    sr = make_shared<stream_result_future>(plan_id, description, is_receiving);
    sm.register_receiving(sr);
    return sr;
}

void stream_result_future::handle_session_prepared(shared_ptr<stream_session> session) {
    auto si = session->make_session_info();
    logger.debug("{} start, plan_id:{} prepare completed with {}. Receiving {}, sending {}",
               __func__,
               session->plan_id(),
               session->peer,
               si.get_total_files_to_receive(),
               si.get_total_files_to_send());
    auto event = session_prepared_event(plan_id, si);
    session->get_session_info() = si;
    fire_stream_event(std::move(event));
}

void stream_result_future::handle_session_complete(shared_ptr<stream_session> session) {
    logger.info("{} start, plan_id:{} session with {} is complete, state={}"
        , __func__, session->plan_id(), session->peer, session->get_state());
    auto event = session_complete_event(session);
    fire_stream_event(std::move(event));
    auto si = session->make_session_info();
    session->get_session_info() = si;
    maybe_complete();
}

template <typename Event>
void stream_result_future::fire_stream_event(Event event) {
    // delegate to listener
    for (auto listener : _event_listeners) {
        listener->handle_stream_event(std::move(event));
    }
}

void stream_result_future::maybe_complete() {
    auto has_active_sessions = _coordinator->has_active_sessions();
    auto plan_id = this->plan_id;
    logger.debug("[{}] start plan_id:{} stream_result_future: has_active_sessions={}"
        , __func__, plan_id, has_active_sessions);
    if (!has_active_sessions) {
        auto& sm = get_local_stream_manager();
        //tododl: comment out temporarily
        //if (logger.is_enabled(logging::log_level::debug)) {
        //    sm.show_streams();
        //}
        sm.get_progress_on_all_shards(plan_id).then([plan_id] (auto sbytes) {
            logger.debug("[maybe_complete] plan_id:{}, bytes_sent = {}, bytes_received = {}"
                , plan_id, sbytes.bytes_sent, sbytes.bytes_received);
        }).handle_exception([plan_id] (auto ep) {
            std::ostringstream out;
            out << "[maybe_complete] error, fail to get progess on all shards";
            out << ", plan_id:" << plan_id;
            out << ", exception:" << ep;
            auto error_info = out.str();
            logger.warn(error_info.c_str());
        }).finally([this, plan_id, &sm] {
            sm.remove_stream(plan_id);
            auto final_state = get_current_state();
            if (final_state.has_failed_session()) {
                logger.warn("[maybe_complete] plan_id:{}, stream failed, peers={}", plan_id, _coordinator->get_peers());
                _done.set_exception(stream_exception(final_state, "Stream failed"));
            } else {
                logger.debug("[maybe_complete] plan_id:{}, all sessions completed, peers={}", plan_id, _coordinator->get_peers());
                _done.set_value(final_state);
            }
        });
    }
}

stream_state stream_result_future::get_current_state() {
    return stream_state(plan_id, description, _coordinator->get_all_session_info());
}

void stream_result_future::handle_progress(progress_info progress) {
    fire_stream_event(progress_event(plan_id, std::move(progress)));
}

} // namespace hive 
