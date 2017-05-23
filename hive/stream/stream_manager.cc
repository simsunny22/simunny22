#include "core/distributed.hh"
#include "hive/stream/stream_manager.hh"
#include "hive/stream/stream_result_future.hh"
#include "log.hh"
#include "hive/stream/stream_session_state.hh"

namespace hive {

static logging::logger logger("stream_manager");

distributed<stream_manager> _the_stream_manager;

void stream_manager::register_sending(shared_ptr<stream_result_future> result) {
#if 0
    result.addEventListener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            initiatedStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _initiated_streams[result->plan_id] = std::move(result);
}

void stream_manager::register_receiving(shared_ptr<stream_result_future> result) {
#if 0
    result->add_event_listener(notifier);
    // Make sure we remove the stream on completion (whether successful or not)
    result.addListener(new Runnable()
    {
        public void run()
        {
            receivingStreams.remove(result.planId);
        }
    }, MoreExecutors.sameThreadExecutor());
#endif
    _receiving_streams[result->plan_id] = std::move(result);
}

shared_ptr<stream_result_future> stream_manager::get_sending_stream(UUID plan_id) {
    auto it = _initiated_streams.find(plan_id);
    if (it != _initiated_streams.end()) {
        return it->second;
    }
    return {};
}

shared_ptr<stream_result_future> stream_manager::get_receiving_stream(UUID plan_id) {
    auto it = _receiving_streams.find(plan_id);
    if (it != _receiving_streams.end()) {
        return it->second;
    }
    return {};
}

void stream_manager::remove_stream(UUID plan_id) {
    logger.debug("stream_manager: removing plan_id={}", plan_id);
    _initiated_streams.erase(plan_id);
    _receiving_streams.erase(plan_id);
    // FIXME: Do not ignore the future
    remove_progress_on_all_shards(plan_id).handle_exception([plan_id] (auto ep) {
        logger.info("stream_manager: Fail to remove progress for plan_id={}: {}", plan_id, ep);
    });
}

void stream_manager::show_streams() {
    for (auto& x : _initiated_streams) {
        logger.debug("stream_manager:initiated_stream: plan_id={}", x.first);
    }
    for (auto& x : _receiving_streams) {
        logger.debug("stream_manager:receiving_stream: plan_id={}", x.first);
    }
}

std::vector<shared_ptr<stream_result_future>> stream_manager::get_all_streams() const {
    std::vector<shared_ptr<stream_result_future>> result;
    for (auto& x : _initiated_streams) {
        result.push_back(x.second);
    }
    for (auto& x : _receiving_streams) {
        result.push_back(x.second);
    }
    return result;
}

void stream_manager::update_progress(UUID plan_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size) {
    auto& sbytes = _stream_bytes[plan_id];
    if (dir == progress_info::direction::OUT) {
        sbytes[peer].bytes_sent += fm_size;
    } else {
        sbytes[peer].bytes_received += fm_size;
    }
}

future<> stream_manager::update_all_progress_info() {
    return seastar::async([this] {
        for (auto sr: get_all_streams()) {
            for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
                session->update_progress().get();
            }
        }
    });
}

void stream_manager::remove_progress(UUID plan_id) {
    _stream_bytes.erase(plan_id);
}

stream_bytes stream_manager::get_progress(UUID plan_id, gms::inet_address peer) {
    auto& sbytes = _stream_bytes[plan_id];
    return sbytes[peer];
}

stream_bytes stream_manager::get_progress(UUID plan_id) {
    stream_bytes ret;
    for (auto& x : _stream_bytes[plan_id]) {
        ret += x.second;
    }
    return ret;
}

future<> stream_manager::remove_progress_on_all_shards(UUID plan_id) {
    return get_stream_manager().invoke_on_all([plan_id] (auto& sm) {
        sm.remove_progress(plan_id);
    });
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(UUID plan_id, gms::inet_address peer) {
    return get_stream_manager().map_reduce0(
        [plan_id, peer] (auto& sm) {
            return sm.get_progress(plan_id, peer);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(UUID plan_id) {
    return get_stream_manager().map_reduce0(
        [plan_id] (auto& sm) {
            return sm.get_progress(plan_id);
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards(gms::inet_address peer) {
    return get_stream_manager().map_reduce0(
        [peer] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                ret += sbytes.second[peer];
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

future<stream_bytes> stream_manager::get_progress_on_all_shards() {
    return get_stream_manager().map_reduce0(
        [] (auto& sm) {
            stream_bytes ret;
            for (auto& sbytes : sm._stream_bytes) {
                for (auto& sb : sbytes.second) {
                    ret += sb.second;
                }
            }
            return ret;
        },
        stream_bytes(),
        std::plus<stream_bytes>()
    );
}

void stream_manager::fail_sessions(inet_address endpoint) {
    for (auto sr : get_all_streams()) {
        for (auto session : sr->get_coordinator()->get_all_stream_sessions()) {
            if (session->peer == endpoint) {
                session->close_session(stream_session_state::FAILED);
            }
        }
    }
}

void stream_manager::on_remove(inet_address endpoint) {
    logger.info("stream_manager: Close all stream_session with peer = {} in on_remove", endpoint);
    get_stream_manager().invoke_on_all([endpoint] (auto& sm) {
        sm.fail_sessions(endpoint);
    }).handle_exception([endpoint] (auto ep) {
        logger.warn("stream_manager: Fail to close sessions peer = {} in on_remove", endpoint);
    });
}

void stream_manager::on_restart(inet_address endpoint, endpoint_state ep_state) {
    logger.info("stream_manager: Close all stream_session with peer = {} in on_restart", endpoint);
    get_stream_manager().invoke_on_all([endpoint] (auto& sm) {
        sm.fail_sessions(endpoint);
    }).handle_exception([endpoint] (auto ep) {
        logger.warn("stream_manager: Fail to close sessions peer = {} in on_restart", endpoint);
    });
}

} // namespace hive
