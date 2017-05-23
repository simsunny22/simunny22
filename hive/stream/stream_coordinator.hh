#pragma once

#include "gms/inet_address.hh"
#include "hive/stream/stream_session.hh"
#include "hive/stream/session_info.hh"
#include <map>
#include <algorithm>

namespace hive {

/**
 * {@link StreamCoordinator} is a helper class that abstracts away maintaining multiple
 * StreamSession and ProgressInfo instances per peer.
 *
 * This class coordinates multiple SessionStreams per peer in both the outgoing StreamPlan context and on the
 * inbound StreamResultFuture context.
 */
class stream_coordinator {
public:
    using inet_address = gms::inet_address;

private:
    class host_streaming_data;
    std::map<inet_address, shared_ptr<stream_session>> _peer_sessions;
    bool _is_receiving;

public:
    stream_coordinator(bool is_receiving = false)
        : _is_receiving(is_receiving) {
    }
public:
    /**
     * @return true if any stream session is active
     */
    bool has_active_sessions();

    std::vector<shared_ptr<stream_session>> get_all_stream_sessions();

    bool is_receiving();

    void connect_all_stream_sessions();
    std::set<inet_address> get_peers();

public:
    shared_ptr<stream_session> get_or_create_session(inet_address peer);

    std::vector<session_info> get_all_session_info();
    std::vector<session_info> get_peer_session_info(inet_address peer);
};

} // namespace hive 
