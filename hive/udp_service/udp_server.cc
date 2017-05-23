#include "udp_server.hh"

distributed<udp_server> _the_udp_server;

void udp_server::start(uint16_t port) {
    ipv4_addr listen_addr{"0.0.0.0", port};
    _chan = engine().net().make_udp_channel(listen_addr);


    keep_doing([this] {
        return _chan.receive().then([this] (udp_datagram dgram) {
            //_n_sent ++;
            int randomcpu = rand()% smp::count;

            auto& trail_service = hive::get_trail_service();
            return trail_service.invoke_on(randomcpu, [dgram=std::move(dgram)](auto& shard_trail_service) mutable{
                return shard_trail_service.trace(std::move(dgram));
            });
        });
    });
}

// FIXME: we should properly tear down the service here.
future<> udp_server::stop() {
    return make_ready_future<>();
}
