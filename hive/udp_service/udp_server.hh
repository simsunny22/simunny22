#include "core/distributed.hh"
#include "core/future-util.hh"
#include "net/api.hh"
#include "hive/trail/trail_service.hh"
#include <msgpack.hpp>

using namespace net;

class udp_server {
private:
    udp_channel _chan;
    timer<> _stats_timer;
    uint64_t _n_sent=0;
public:
    void start(uint16_t port);
    future<> stop();
};


extern distributed<udp_server> _the_udp_server;

inline distributed<udp_server>& get_udp_server() {
    return _the_udp_server;
}

