#pragma once
#include "hive/stream/progress_info.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "utils/UUID.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include <seastar/core/semaphore.hh>
#include <map>

namespace hive {

class stream_result_future;

struct stream_bytes {
    int64_t bytes_sent = 0;
    int64_t bytes_received = 0;
    friend stream_bytes operator+(const stream_bytes& x, const stream_bytes& y) {
        stream_bytes ret(x);
        ret += y;
        return ret;
    }
    friend bool operator!=(const stream_bytes& x, const stream_bytes& y) {
        return x.bytes_sent != y.bytes_sent && x.bytes_received != y.bytes_received;
    }
    friend bool operator==(const stream_bytes& x, const stream_bytes& y) {
        return x.bytes_sent == y.bytes_sent && x.bytes_received == y.bytes_received;
    }
    stream_bytes& operator+=(const stream_bytes& x) {
        bytes_sent += x.bytes_sent;
        bytes_received += x.bytes_received;
        return *this;
    }
};

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
class stream_manager : public gms::i_endpoint_state_change_subscriber, public enable_shared_from_this<stream_manager> {
    using UUID = utils::UUID;
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    using application_state = gms::application_state;
    using versioned_value = gms::versioned_value;
    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
private:
    std::unordered_map<UUID, shared_ptr<stream_result_future>> _initiated_streams;
    std::unordered_map<UUID, shared_ptr<stream_result_future>> _receiving_streams;
    std::unordered_map<UUID, std::unordered_map<gms::inet_address, stream_bytes>> _stream_bytes;
    semaphore _chunk_send_limit;
public:
    stream_manager(uint32_t chunk_send_limit)
        : _chunk_send_limit(chunk_send_limit){}

    semaphore& chunk_send_limit() { return _chunk_send_limit; }

    void register_sending(shared_ptr<stream_result_future> result);

    void register_receiving(shared_ptr<stream_result_future> result);

    shared_ptr<stream_result_future> get_sending_stream(UUID plan_id);

    shared_ptr<stream_result_future> get_receiving_stream(UUID plan_id);

    std::vector<shared_ptr<stream_result_future>> get_all_streams() const ;


    const std::unordered_map<UUID, shared_ptr<stream_result_future>>& get_initiated_streams() const {
        return _initiated_streams;
    }

    const std::unordered_map<UUID, shared_ptr<stream_result_future>>& get_receiving_streams() const {
        return _receiving_streams;
    }

    void remove_stream(UUID plan_id);

    void show_streams();

    future<> stop() {
        return make_ready_future<>();
    }

    void update_progress(UUID plan_id, gms::inet_address peer, progress_info::direction dir, size_t fm_size);
    future<> update_all_progress_info();

    void remove_progress(UUID plan_id);

    stream_bytes get_progress(UUID plan_id, gms::inet_address peer);

    stream_bytes get_progress(UUID plan_id);

    future<> remove_progress_on_all_shards(UUID plan_id);

    future<stream_bytes> get_progress_on_all_shards(UUID plan_id, gms::inet_address peer);

    future<stream_bytes> get_progress_on_all_shards(UUID plan_id);

    future<stream_bytes> get_progress_on_all_shards(gms::inet_address peer);

    future<stream_bytes> get_progress_on_all_shards();

public:
    virtual void on_join(inet_address endpoint, endpoint_state ep_state) override {}
    virtual void before_change(inet_address endpoint, endpoint_state current_state, application_state new_state_key, const versioned_value& new_value) override {}
    virtual void on_change(inet_address endpoint, application_state state, const versioned_value& value) override {}
    virtual void on_alive(inet_address endpoint, endpoint_state state) override {}
    virtual void on_dead(inet_address endpoint, endpoint_state state) override {}
    virtual void on_remove(inet_address endpoint) override;
    virtual void on_restart(inet_address endpoint, endpoint_state ep_state) override;

private:
    void fail_sessions(inet_address endpoint);
};

extern distributed<stream_manager> _the_stream_manager;

inline distributed<stream_manager>& get_stream_manager() {
    return _the_stream_manager;
}

inline stream_manager& get_local_stream_manager() {
    return _the_stream_manager.local();
}

} // namespace hive 