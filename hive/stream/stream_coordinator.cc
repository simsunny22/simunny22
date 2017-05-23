#include "hive/stream/stream_detail.hh"
#include "hive/stream/stream_session_state.hh"
#include "hive/stream/stream_coordinator.hh"
#include "log.hh"

namespace hive {

static logging::logger logger("stream_coordinator");

using gms::inet_address;

shared_ptr<stream_session> stream_coordinator::get_or_create_session(inet_address peer) {
    logger.debug("{}, start", __func__);
    auto& session = _peer_sessions[peer];
    if (!session) {
        session = make_shared<stream_session>(peer);
    }
    return session;
}

bool stream_coordinator::has_active_sessions() {
    for (auto& x : _peer_sessions) {
        auto state = x.second->get_state();
        if (state != stream_session_state::COMPLETE && state != stream_session_state::FAILED) {
            return true;
        }
    }
    return false;
}

std::vector<shared_ptr<stream_session>> stream_coordinator::get_all_stream_sessions() {
    std::vector<shared_ptr<stream_session>> results;
    for (auto& x : _peer_sessions) {
        results.push_back(x.second);
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_all_session_info() {
    std::vector<session_info> results;
    for (auto& x : _peer_sessions) {
        auto& session = x.second;
        results.push_back(session->get_session_info());
    }
    return results;
}

std::vector<session_info> stream_coordinator::get_peer_session_info(inet_address peer) {
    std::vector<session_info> results;
    auto it = _peer_sessions.find(peer);
    if (it != _peer_sessions.end()) {
        auto& session = it->second;
        results.push_back(session->get_session_info());
    }
    return results;
}

bool stream_coordinator::is_receiving() {
    return _is_receiving;
}

std::set<inet_address> stream_coordinator::get_peers() {
    std::set<inet_address> results;
    for (auto& x : _peer_sessions) {
        results.insert(x.first);
    }
    return results;
}

void stream_coordinator::connect_all_stream_sessions() {
    for (auto& x : _peer_sessions) {
        auto& session = x.second;
        session->start();
        logger.info("[Stream #{}] Beginning stream session with {}", session->plan_id(), session->peer);
    }
}

} // namespace hive 
