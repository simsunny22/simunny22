#include "hive/messaging_service.hh"
#include "hive/commitlog/hive_commitlog_entry.hh"
#include "hive/commitlog/replay_position.hh"
#include "hive/hive_result.hh"
#include "hive/hive_request.hh"
#include "hive/extent_datum.hh"
#include "hive/extent_revision.hh"
#include "hive/extent_revision_set.hh"
#include "hive/group_revision_set.hh"
#include "hive/stream/prepare_message.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/migrate_chunk.hh"
#include "hive/stream/test_message.hh"
#include "hive/hive_request.hh"
#include "hive/hive_data_type.hh"
#include "hive/message_data_type.hh"
#include "hive/drain/drain_extent_group_task.hh"
#include "hive/journal/volume_revision.hh"

#include "core/distributed.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/gossiper.hh"
#include "query-result.hh"
#include "rpc/rpc.hh"
#include "db/config.hh"
#include "dht/i_partitioner.hh"
#include "repair/repair.hh"
#include "utils/UUID.hh"

#include "idl/uuid.dist.hh"
#include "idl/hive.dist.hh"
#include "idl/replay_position.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/result.dist.hh"
#include "idl/gossip_digest.dist.hh"
#include "serializer_impl.hh"         //split include between *.dist.hh and *.dist.impl.hh
#include "serialization_visitors.hh"  //split include between *.dist.hh and *.dist.impl.hh
#include "idl/uuid.dist.impl.hh"
#include "idl/hive.dist.impl.hh"
#include "idl/replay_position.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/result.dist.impl.hh"
#include "idl/gossip_digest.dist.impl.hh"

namespace hive{

// thunk from rpc serializers to generate serializers
template <typename T, typename Output>
void write(serializer, Output& out, const T& data) {
    ser::serialize(out, data);
}
template <typename T, typename Input>
T read(serializer, Input& in, boost::type<T> type) {
    return ser::deserialize(in, type);
}

template <typename Output, typename T>
void write(serializer s, Output& out, const foreign_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
foreign_ptr<T> read(serializer s, Input& in, boost::type<foreign_ptr<T>>) {
    return make_foreign(read(s, in, boost::type<T>()));
}

template <typename Output, typename T>
void write(serializer s, Output& out, const lw_shared_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
lw_shared_ptr<T> read(serializer s, Input& in, boost::type<lw_shared_ptr<T>>) {
    return make_lw_shared(read(s, in, boost::type<T>()));
}

static logging::logger logger("messaging_service");
static logging::logger rpc_logger("rpc");

using inet_address = gms::inet_address;
using gossip_digest_syn = gms::gossip_digest_syn;
using gossip_digest_ack = gms::gossip_digest_ack;
using gossip_digest_ack2 = gms::gossip_digest_ack2;
using rpc_protocol = rpc::protocol<serializer, messaging_verb>;
using namespace std::chrono_literals;

struct messaging_service::rpc_protocol_wrapper : public rpc_protocol { using rpc_protocol::rpc_protocol; };

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
class messaging_service::rpc_protocol_client_wrapper {
    std::unique_ptr<rpc_protocol::client> _p;
public:
    rpc_protocol_client_wrapper(rpc_protocol& proto, rpc::client_options opts, ipv4_addr addr, ipv4_addr local = ipv4_addr())
            : _p(std::make_unique<rpc_protocol::client>(proto, std::move(opts), addr, local)) {
    }
    rpc_protocol_client_wrapper(rpc_protocol& proto
        , rpc::client_options opts
        , ipv4_addr addr
        , ipv4_addr local
        , ::shared_ptr<seastar::tls::server_credentials> c)
        : _p(std::make_unique<rpc_protocol::client>(proto, std::move(opts), seastar::tls::socket(c), addr, local))

    {}
    auto get_stats() const { return _p->get_stats(); }
    future<> stop() { return _p->stop(); }
    bool error() {
        return _p->error();
    }
    operator rpc_protocol::client&() { return *_p; }
};

struct messaging_service::rpc_protocol_server_wrapper : public rpc_protocol::server { using rpc_protocol::server::server; };

constexpr int32_t messaging_service::current_version;

distributed<messaging_service> _the_messaging_service;

bool operator==(const msg_addr& x, const msg_addr& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    return x.addr == y.addr;
}

bool operator<(const msg_addr& x, const msg_addr& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    if (x.addr < y.addr) {
        return true;
    } else {
        return false;
    }
}

std::ostream& operator<<(std::ostream& os, const msg_addr& x) {
    return os << x.addr << ":" << x.cpu_id;
}

size_t msg_addr::hash::operator()(const msg_addr& id) const {
    // Ignore cpu id for now since we do not really support // shard to shard connections
    return std::hash<uint32_t>()(id.addr.raw_addr());
}

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client)
    : rpc_client(std::move(client)) {
}

rpc::stats messaging_service::shard_info::get_stats() const {
    return rpc_client->get_stats();
}

void messaging_service::foreach_client(std::function<void(const msg_addr& id, const shard_info& info)> f) const {
    for (unsigned idx = 0; idx < _clients.size(); idx ++) {
        for (auto i = _clients[idx].cbegin(); i != _clients[idx].cend(); i++) {
            f(i->first, i->second);
        }
    }
}

void messaging_service::foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const {
    _server->foreach_connection([f](const rpc_protocol::server::connection& c) {
        f(c.info(), c.get_stats());
    });
}

void messaging_service::increment_dropped_messages(messaging_verb verb) {
    _dropped_messages[static_cast<int32_t>(verb)]++;
}

uint64_t messaging_service::get_dropped_messages(messaging_verb verb) const {
    return _dropped_messages[static_cast<int32_t>(verb)];
}

const uint64_t* messaging_service::get_dropped_messages() const {
    return _dropped_messages;
}

int32_t messaging_service::get_raw_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return current_version;
}

bool messaging_service::knows_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return true;
}

// Register a handler (a callback lambda) for verb
template <typename Func>
void register_handler(messaging_service* ms, messaging_verb verb, Func&& func) {
    ms->rpc()->register_handler(verb, std::move(func));
}

messaging_service::messaging_service(gms::inet_address ip, uint16_t port)
    : messaging_service(std::move(ip), port, encrypt_what::none, 0, nullptr)
{}

static
rpc::resource_limits
rpc_resource_limits() {
    rpc::resource_limits limits;
    limits.bloat_factor = 3;
    limits.basic_request_size = 1000;
    //chenjl for debug
    //limits.max_memory = std::max<size_t>(0.08 * memory::stats().total_memory(), 1'000'000);
    limits.max_memory = std::max<size_t>(0.08 * memory::stats().total_memory(), 1024*1024*1024);
    return limits;
}

messaging_service::messaging_service(gms::inet_address ip
        , uint16_t port
        , encrypt_what ew
        , uint16_t ssl_port
        , ::shared_ptr<seastar::tls::server_credentials> credentials
        )
    : _listen_address(ip)
    , _port(port)
    , _ssl_port(ssl_port)
    , _encrypt_what(ew)
    , _rpc(new rpc_protocol_wrapper(serializer { }))
    , _server(new rpc_protocol_server_wrapper(*_rpc, ipv4_addr { _listen_address.raw_addr(), _port }, rpc_resource_limits()))
    , _credentials(std::move(credentials))
    , _server_tls([this]() -> std::unique_ptr<rpc_protocol_server_wrapper>{
        if (_encrypt_what == encrypt_what::none) {
            return nullptr;
        }
        listen_options lo;
        lo.reuse_address = true;
        return std::make_unique<rpc_protocol_server_wrapper>(*_rpc,
                        seastar::tls::listen(_credentials
                                        , make_ipv4_address(ipv4_addr {_listen_address.raw_addr(), _ssl_port})
                                        , lo)
        );
    }())
{
    _rpc->set_logger([] (const sstring& log) {
            rpc_logger.info("{}", log);
    });
    register_handler(this, messaging_verb::CLIENT_ID, [] (rpc::client_info& ci, gms::inet_address broadcast_address, uint32_t src_cpu_id) {
        ci.attach_auxiliary("baddr", broadcast_address);
        ci.attach_auxiliary("src_cpu_id", src_cpu_id);
        return rpc::no_wait;
    });
}

msg_addr messaging_service::get_source(const rpc::client_info& cinfo) {
    return msg_addr{
        cinfo.retrieve_auxiliary<gms::inet_address>("baddr"),
        cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id")
    };
}

messaging_service::~messaging_service() = default;

uint16_t messaging_service::port() {
    return _port;
}

gms::inet_address messaging_service::listen_address() {
    return _listen_address;
}

future<> messaging_service::stop() {
    _stopping = true;
    return when_all(
        _server->stop(),
        parallel_for_each(_clients, [] (auto& m) {
            return parallel_for_each(m, [] (std::pair<const msg_addr, shard_info>& c) {
                return c.second.rpc_client->stop();
            });
        })
    ).discard_result();
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}

static unsigned get_rpc_client_idx(messaging_verb verb) {
    unsigned idx = 0;
    // GET_SCHEMA_VERSION is sent from read/mutate verbs so should be
    // sent on a different connection to avoid potential deadlocks
    // as well as reduce latency as there are potentially many requests
    // blocked on schema version request.
    //if (verb == messaging_verb::GOSSIP_DIGEST_SYN ||
    //    verb == messaging_verb::GOSSIP_DIGEST_ACK2 ||
    //    verb == messaging_verb::GOSSIP_SHUTDOWN ||
    //    verb == messaging_verb::GOSSIP_ECHO ||
    //    verb == messaging_verb::GET_SCHEMA_VERSION) {
    //    idx = 1;
    //} else if (verb == messaging_verb::PREPARE_MESSAGE ||
    //           verb == messaging_verb::PREPARE_DONE_MESSAGE ||
    //           verb == messaging_verb::STREAM_MUTATION ||
    //           verb == messaging_verb::STREAM_MUTATION_DONE ||
    //           verb == messaging_verb::COMPLETE_MESSAGE) {
    //    idx = 2;
    //}
    return idx;
}

/**
 * Get an IP for a given endpoint to connect to
 *
 * @param ep endpoint to check
 *
 * @return preferred IP (local) for the given endpoint if exists and if the
 *         given endpoint resides in the same data center with the current Node.
 *         Otherwise 'ep' itself is returned.
 */
gms::inet_address messaging_service::get_preferred_ip(gms::inet_address ep) {
    auto it = _preferred_ip_cache.find(ep);

    if (it != _preferred_ip_cache.end()) {
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        auto my_addr = utils::fb_utilities::get_broadcast_address();

        if (snitch_ptr->get_datacenter(ep) == snitch_ptr->get_datacenter(my_addr)) {
            return it->second;
        }
    }

    // If cache doesn't have an entry for this endpoint - return endpoint itself
    return ep;
}

future<> messaging_service::init_local_preferred_ip_cache() {
    return db::system_keyspace::get_preferred_ips().then([this] (auto ips_cache) {
        _preferred_ip_cache = ips_cache;
        //
        // Reset the connections to the endpoints that have entries in
        // _preferred_ip_cache so that they reopen with the preferred IPs we've
        // just read.
        //
        for (auto& p : _preferred_ip_cache) {
            msg_addr id = {
                .addr = p.first
            };

            this->remove_rpc_client(id);
        }
    });
}

void messaging_service::cache_preferred_ip(gms::inet_address ep, gms::inet_address ip) {
    _preferred_ip_cache[ep] = ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, msg_addr id) {
    assert(!_stopping);
    auto idx = get_rpc_client_idx(verb);
    auto it = _clients[idx].find(id);

    if (it != _clients[idx].end()) {
        auto c = it->second.rpc_client;
        if (!c->error()) {
            return c;
        }
        remove_error_rpc_client(verb, id);
    }

    auto must_encrypt = [&id, this] {
        if (_encrypt_what == encrypt_what::none) {
            return false;
        }
        if (_encrypt_what == encrypt_what::all) {
            return true;
        }

        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        if (_encrypt_what == encrypt_what::dc) {
            return snitch_ptr->get_datacenter(id.addr)
                            != snitch_ptr->get_datacenter(utils::fb_utilities::get_broadcast_address());
        }
        return snitch_ptr->get_rack(id.addr)
                        != snitch_ptr->get_rack(utils::fb_utilities::get_broadcast_address());
    }();

    auto remote_addr = ipv4_addr(get_preferred_ip(id.addr).raw_addr(), must_encrypt ? _ssl_port : _port);
    auto local_addr = ipv4_addr{_listen_address.raw_addr(), 0};

    rpc::client_options opts;
    // send keepalive messages each minute if connection is idle, drop connection after 10 failures
    opts.keepalive = std::experimental::optional<net::tcp_keepalive_params>({60s, 60s, 10});

    auto client = must_encrypt ?
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc, std::move(opts),
                                    remote_addr, local_addr, _credentials) :
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc, std::move(opts),
                                    remote_addr, local_addr);

    it = _clients[idx].emplace(id, shard_info(std::move(client))).first;
    uint32_t src_cpu_id = engine().cpu_id();
    _rpc->make_client<rpc::no_wait_type(gms::inet_address, uint32_t)>(messaging_verb::CLIENT_ID)(*it->second.rpc_client, utils::fb_utilities::get_broadcast_address(), src_cpu_id);
    return it->second.rpc_client;
}

void messaging_service::remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only) {
    if (_stopping) {
        // if messaging service is in a processed of been stopped no need to
        // stop and remove connection here since they are being stopped already
        // and we'll just interfere
        return;
    }

    auto it = clients.find(id);
    if (it != clients.end() && (!dead_only || it->second.rpc_client->error())) {
        auto client = std::move(it->second.rpc_client);
        clients.erase(it);
        //
        // Explicitly call rpc_protocol_client_wrapper::stop() for the erased
        // item and hold the messaging_service shared pointer till it's over.
        // This will make sure messaging_service::stop() blocks until
        // client->stop() is over.
        //
        client->stop().finally([id, client, ms = shared_from_this()] {
            logger.debug("dropped connection to {}", id.addr);
        }).discard_result();
    }
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, msg_addr id) {
    remove_rpc_client_one(_clients[get_rpc_client_idx(verb)], id, true);
}

void messaging_service::remove_rpc_client(msg_addr id) {
    for (auto& c : _clients) {
        remove_rpc_client_one(c, id, false);
    }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_stopping()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// TODO: Remove duplicated code in send_message
template <typename MsgIn, typename Timeout, typename... MsgOut>
auto send_message_timeout(messaging_service* ms, messaging_verb verb, msg_addr id, Timeout timeout, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_stopping()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, timeout, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

template <typename MsgIn, typename... MsgOut>
auto send_message_timeout_and_retry(messaging_service* ms, messaging_verb verb, msg_addr id,
        std::chrono::seconds timeout, int nr_retry, std::chrono::seconds wait, MsgOut... msg) {
    namespace stdx = std::experimental;
    using MsgInTuple = typename futurize_t<MsgIn>::value_type;
    return do_with(int(nr_retry), std::move(msg)..., [ms, verb, id, timeout, wait, nr_retry] (auto& retry, const auto&... messages) {
        return repeat_until_value([ms, verb, id, timeout, wait, nr_retry, &retry, &messages...] {
            return send_message_timeout<MsgIn>(ms, verb, id, timeout, messages...).then_wrapped(
                    [ms, verb, id, timeout, wait, nr_retry, &retry] (auto&& f) mutable {
                auto vb = int(verb);
                try {
                    MsgInTuple ret = f.get();
                    if (retry != nr_retry) {
                        logger.info("Retry verb={} to {}, retry={}: OK", vb, id, retry);
                    }
                    return make_ready_future<stdx::optional<MsgInTuple>>(std::move(ret));
                } catch (rpc::timeout_error) {
                    logger.info("Retry verb={} to {}, retry={}: timeout in {} seconds", vb, id, retry, timeout.count());
                    throw;
                } catch (rpc::closed_error) {
                    logger.info("Retry verb={} to {}, retry={}: {}", vb, id, retry, std::current_exception());
                    // Stop retrying if retry reaches 0 or message service is shutdown
                    // or the remote node is removed from gossip (on_remove())
                    retry--;
                    if (retry == 0) {
                        logger.debug("Retry verb={} to {}, retry={}: stop retrying: retry == 0", vb, id, retry);
                        throw;
                    }
                    if (ms->is_stopping()) {
                        logger.debug("Retry verb={} to {}, retry={}: stop retrying: messaging_service is stopped",
                                     vb, id, retry);
                        throw;
                    }
                    if (!gms::get_local_gossiper().is_known_endpoint(id.addr)) {
                        logger.debug("Retry verb={} to {}, retry={}: stop retrying: node is removed from the cluster",
                                     vb, id, retry);
                        throw;
                    }
                    return sleep(wait).then([] {
                        return make_ready_future<stdx::optional<MsgInTuple>>(stdx::nullopt);
                    });
                } catch (...) {
                    throw;
                }
            });
        }).then([ms = ms->shared_from_this()] (MsgInTuple result) {
            return futurize<MsgIn>::from_tuple(std::move(result));
        });
    });
}

// Send one way message for verb
template <typename... MsgOut>
auto send_message_oneway(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message<rpc::no_wait_type>(ms, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Send one way message for verb
template <typename Timeout, typename... MsgOut>
auto send_message_oneway_timeout(messaging_service* ms, Timeout timeout, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message_timeout<rpc::no_wait_type>(ms, std::move(verb), std::move(id), timeout, std::forward<MsgOut>(msg)...);
}

// Wrappers for verbs

// Retransmission parameters for streaming verbs.
// A stream plan gives up retrying in 10*30 + 10*60 seconds (15 minutes) at
// most, 10*30 seconds (5 minutes) at least.
//static constexpr int streaming_nr_retry = 10;
//static constexpr std::chrono::seconds streaming_timeout{10*60};
//static constexpr std::chrono::seconds streaming_wait_before_retry{30};
static constexpr int streaming_nr_retry = 5;
static constexpr std::chrono::seconds streaming_timeout{30};
static constexpr std::chrono::seconds streaming_wait_before_retry{5};

//Hive Wrapper for DATUM
void messaging_service::register_datum(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::extent_datum d
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DATUM, std::move(func));
}

void messaging_service::unregister_datum() {
    _rpc->unregister_handler(hive::messaging_verb::DATUM);
}

future<> messaging_service::send_datum(msg_addr id
    , clock_type::time_point timeout
    , hive::extent_datum& d
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::DATUM
        , std::move(id), d, std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

//Hive Wrapper for DATUM_DONE
void messaging_service::register_datum_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , sstring disk_id
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DATUM_DONE, std::move(func));
}

void messaging_service::unregister_datum_done() {
    _rpc->unregister_handler(hive::messaging_verb::DATUM_DONE);
}

future<> messaging_service::send_datum_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::DATUM_DONE, std::move(id)
        , std::move(shard), std::move(disk_id), std::move(response_id));
}

//Hive Wrapper for create_extent_group
void messaging_service::register_create_extent_group(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , sstring extent_group_id
    , sstring disk_id
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::CREATE_EXTENT_GROUP, std::move(func));
}

void messaging_service::unregister_create_extent_group() {
    _rpc->unregister_handler(hive::messaging_verb::CREATE_EXTENT_GROUP);
}

future<> messaging_service::create_extent_group(msg_addr id
    , clock_type::time_point timeout
    , sstring extent_group_id
    , sstring disk_id
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::CREATE_EXTENT_GROUP
        , std::move(id), std::move(extent_group_id), std::move(disk_id), std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

void messaging_service::register_create_extent_group_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , sstring disk_id
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::CREATE_EXTENT_GROUP_DONE, std::move(func));
}

void messaging_service::unregister_create_extent_group_done() {
    _rpc->unregister_handler(hive::messaging_verb::CREATE_EXTENT_GROUP_DONE);
}

future<> messaging_service::create_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::CREATE_EXTENT_GROUP_DONE, std::move(id)
        , std::move(shard), disk_id, std::move(response_id));
}

//Hive Wrapper for delete_extent_group
void messaging_service::register_delete_extent_group(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , sstring extent_group_id
    , sstring disk_id
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DELETE_EXTENT_GROUP, std::move(func));
}

void messaging_service::unregister_delete_extent_group() {
    _rpc->unregister_handler(hive::messaging_verb::DELETE_EXTENT_GROUP);
}

future<> messaging_service::delete_extent_group(msg_addr id
    , clock_type::time_point timeout
    , sstring extent_group_id
    , sstring disk_id
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::DELETE_EXTENT_GROUP
        , std::move(id), std::move(extent_group_id), std::move(disk_id), std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

//Hive Wrapper for DATUM_DONE
void messaging_service::register_delete_extent_group_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , sstring disk_id
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DELETE_EXTENT_GROUP_DONE, std::move(func));
}

void messaging_service::unregister_delete_extent_group_done() {
    _rpc->unregister_handler(hive::messaging_verb::DELETE_EXTENT_GROUP_DONE);
}

future<> messaging_service::delete_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::DELETE_EXTENT_GROUP_DONE, std::move(id)
        , std::move(shard), disk_id, std::move(response_id));
}

//START_SLAVE_JOURNAL
void messaging_service::register_start_secondary_journal(std::function<future<> (const rpc::client_info&, sstring volume_id)>&& func) {
    register_handler(this, hive::messaging_verb::START_SLAVE_JOURNAL, std::move(func));
}
void messaging_service::unregister_start_secondary_journal() {
    _rpc->unregister_handler(hive::messaging_verb::START_SLAVE_JOURNAL);
}
future<> messaging_service::send_start_secondary_journal(msg_addr id, clock_type::time_point timeout, const sstring volume_id) {
    return send_message_timeout<void>(this, hive::messaging_verb::START_SLAVE_JOURNAL, std::move(id), timeout, volume_id);
}

//STOP_SLAVE_JOURNAL
void messaging_service::register_stop_secondary_journal(std::function<future<> (const rpc::client_info&, sstring volume_id)>&& func) {
    register_handler(this, hive::messaging_verb::STOP_SLAVE_JOURNAL, std::move(func));
}
void messaging_service::unregister_stop_secondary_journal() {
    _rpc->unregister_handler(hive::messaging_verb::STOP_SLAVE_JOURNAL);
}
future<> messaging_service::send_stop_secondary_journal(msg_addr id, clock_type::time_point timeout, const sstring volume_id) {
    return send_message_timeout<void>(this, hive::messaging_verb::STOP_SLAVE_JOURNAL, std::move(id), timeout, volume_id);
}

//Hive Wrapper for WRITE_REVISIONS
void messaging_service::register_write_revisions(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::extent_revision_set revision_set
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::WRITE_REVISIONS, std::move(func));
}

void messaging_service::unregister_write_revisions() {
    _rpc->unregister_handler(hive::messaging_verb::WRITE_REVISIONS);
}

future<> messaging_service::send_revisions(msg_addr id
    , clock_type::time_point timeout
    , hive::extent_revision_set& revision_set
    , std::vector<sstring> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::WRITE_REVISIONS
        , std::move(id), revision_set, std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

void messaging_service::register_write_revisions_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , sstring disk_id
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::WRITE_REVISIONS_DONE, std::move(func));
}

void messaging_service::unregister_write_revisions_done() {
    _rpc->unregister_handler(hive::messaging_verb::WRITE_REVISIONS_DONE);
}

future<> messaging_service::send_revisions_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::WRITE_REVISIONS_DONE, std::move(id)
        , std::move(shard), disk_id, std::move(response_id));
}

//Hive Wrapper for SYNC_COMMITLOG
void messaging_service::register_sync_commitlog(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::sync_commitlog_data_type sync_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::SYNC_COMMITLOG, std::move(func));
}

void messaging_service::unregister_sync_commitlog() {
    _rpc->unregister_handler(hive::messaging_verb::SYNC_COMMITLOG);
}

future<> messaging_service::send_sync_commitlog(msg_addr id
    , clock_type::time_point timeout
    , hive::sync_commitlog_data_type& sync_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::SYNC_COMMITLOG
        , std::move(id)
        , sync_data
        , std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

void messaging_service::register_sync_commitlog_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::SYNC_COMMITLOG_DONE, std::move(func));
}

void messaging_service::unregister_sync_commitlog_done() {
    _rpc->unregister_handler(hive::messaging_verb::SYNC_COMMITLOG_DONE);
}

future<> messaging_service::send_sync_commitlog_done(msg_addr id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::SYNC_COMMITLOG_DONE, std::move(id)
        , std::move(shard), std::move(response_id));
}

//Hive Wrapper for DISCARD_COMMITLOG
void messaging_service::register_discard_commitlog(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , sstring commitlog_id
    , replay_position rp
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DISCARD_COMMITLOG, std::move(func));
}

void messaging_service::unregister_discard_commitlog() {
    _rpc->unregister_handler(hive::messaging_verb::DISCARD_COMMITLOG);
}

future<> messaging_service::send_discard_commitlog(msg_addr id
    , clock_type::time_point timeout
    , sstring commitlog_id
    , replay_position rp
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::DISCARD_COMMITLOG
        , std::move(id), commitlog_id, rp, std::move(forward),std::move(reply_to)
        , std::move(shard), std::move(response_id));
}

void messaging_service::register_discard_commitlog_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DISCARD_COMMITLOG_DONE, std::move(func));
}

void messaging_service::unregister_discard_commitlog_done() {
    _rpc->unregister_handler(hive::messaging_verb::DISCARD_COMMITLOG_DONE);
}

future<> messaging_service::send_discard_commitlog_done(msg_addr id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::DISCARD_COMMITLOG_DONE, std::move(id)
        , std::move(shard), std::move(response_id));
}


//Hive Wrapper for streaming
//---prepare_message
void messaging_service::register_hive_prepare_message(std::function<future<hive::prepare_message> (
    const rpc::client_info& cinfo
    , hive::prepare_message msg
    , UUID plan_id
    , sstring description)>&& func)
{
    register_handler(this, messaging_verb::HIVE_PREPARE_MESSAGE, std::move(func));
}

future<hive::prepare_message>
messaging_service::send_hive_prepare_message(msg_addr peer_id
                                           , hive::prepare_message msg
                                           , UUID plan_id
                                           , sstring description)
{
    return send_message_timeout_and_retry<hive::prepare_message>(
        this
        , messaging_verb::HIVE_PREPARE_MESSAGE
        , peer_id
        , streaming_timeout
        , streaming_nr_retry
        , streaming_wait_before_retry
        , std::move(msg)
        , plan_id
        , std::move(description));
}

//---prepare_message_done
void messaging_service::register_hive_prepare_message_done(std::function<future<> (
    const rpc::client_info& cinfo
    , UUID plan_id
    , unsigned dst_cpu_id)>&& func)
{
    register_handler(this, messaging_verb::HIVE_PREPARE_MESSAGE_DONE, std::move(func));
}
future<> messaging_service::send_hive_prepare_message_done(
    msg_addr id
    , UUID plan_id
    , unsigned dst_cpu_id)
{
    return send_message_timeout_and_retry<void>(
        this
        , messaging_verb::HIVE_PREPARE_MESSAGE_DONE
        , id
        , streaming_timeout
        , streaming_nr_retry
        , streaming_wait_before_retry
        , plan_id
        , dst_cpu_id);
}

//---stream_chunk
void messaging_service::register_stream_chunk(std::function<future<> (
    const rpc::client_info& cinfo
    , UUID plan_id
    , hive::migrate_chunk chunk
    , unsigned dst_cpu_id)>&& func)
{
    register_handler(this, messaging_verb::HIVE_STREAM_CHUNK, std::move(func));
}

future<> messaging_service::send_stream_chunk(msg_addr id
                                                 , UUID plan_id
                                                 , hive::migrate_chunk chunk
                                                 , unsigned dst_cpu_id)
{
    return send_message_timeout_and_retry<void>(
        this
        , messaging_verb::HIVE_STREAM_CHUNK
        , id
        , streaming_timeout
        , streaming_nr_retry
        , streaming_wait_before_retry
        , plan_id
        , std::move(chunk)
        , dst_cpu_id);
}

//---stream_chunk_done
void messaging_service::register_stream_chunk_done(std::function<future<> (
    const rpc::client_info& cinfo
    , UUID plan_id
    , sstring extent_group_id
    , unsigned dst_cpu_id)>&& func)
{
    register_handler(this, messaging_verb::HIVE_STREAM_CHUNK_DONE, std::move(func));
}

future<> messaging_service::send_stream_chunk_done(msg_addr peer_id
                                                 , UUID plan_id
                                                 , sstring task_id
                                                 , unsigned dst_cpu_id)
{
    return send_message_timeout_and_retry<void>(
        this
        , messaging_verb::HIVE_STREAM_CHUNK_DONE
        , peer_id
        , streaming_timeout
        , streaming_nr_retry
        , streaming_wait_before_retry
        , plan_id
        , task_id
        , dst_cpu_id);
}

//---complete_message
void messaging_service::register_hive_complete_message(std::function<future<> (
    const rpc::client_info& cinfo
    , UUID plan_id
    , unsigned dst_cpu_id)>&& func)
{
    register_handler(this, messaging_verb::HIVE_COMPLETE_MESSAGE, std::move(func));
}

future<> messaging_service::send_hive_complete_message(msg_addr peer_id
                                                     , UUID plan_id
                                                     , unsigned dst_cpu_id)
{
    return send_message_timeout_and_retry<void>(
        this
        , messaging_verb::HIVE_COMPLETE_MESSAGE
        , peer_id
        , streaming_timeout
        , streaming_nr_retry
        , streaming_wait_before_retry
        , plan_id
        , dst_cpu_id);
}

//Hive Wrapper for TEST_MESSAGE
void messaging_service::register_test_message(std::function<future<> (
    const rpc::client_info&
    , hive::test_message test_msg
    )>&& func) {

    register_handler(this, hive::messaging_verb::TEST_MESSAGE, std::move(func));
}

void messaging_service::unregister_test_message() {
    _rpc->unregister_handler(hive::messaging_verb::TEST_MESSAGE);
}

future<> messaging_service::send_test_message(msg_addr id
    , clock_type::time_point timeout
    , hive::test_message test_message) {
    return send_message_timeout<void>(this, hive::messaging_verb::TEST_MESSAGE, std::move(id), timeout, std::move(test_message));
}
//END

//migrate_extent_group
void messaging_service::register_migrate_extent_group(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::migrate_params_entry params
    , inet_address reply_to
    , sstring disk_id
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_EXTENT_GROUP, std::move(func));
}
void messaging_service::unregister_migrate_extent_group() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_EXTENT_GROUP);
}

future<> messaging_service::migrate_extent_group(msg_addr id
    , clock_type::time_point timeout
    , hive::migrate_params_entry params
    , inet_address reply_to
    , sstring disk_id
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::MIGRATE_EXTENT_GROUP
        , std::move(id), std::move(params)
        , std::move(reply_to), std::move(disk_id)
        , std::move(shard), std::move(response_id));
}

void messaging_service::register_migrate_extent_group_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , sstring disk_id
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_EXTENT_GROUP_DONE, std::move(func));
}

void messaging_service::unregister_migrate_extent_group_done() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_EXTENT_GROUP_DONE);
}

future<> messaging_service::migrate_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MIGRATE_EXTENT_GROUP_DONE, std::move(id)
        , std::move(shard), disk_id, std::move(response_id));
}

//migrate forward
void messaging_service::register_migrate_forward(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::migrate_params_entry params
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_FORWARD, std::move(func));
}
void messaging_service::unregister_migrate_forward() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_FORWARD);
}

future<> messaging_service::migrate_forward(msg_addr id
    , clock_type::time_point timeout
    , hive::migrate_params_entry params
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(this, timeout, messaging_verb::MIGRATE_FORWARD
        , std::move(id)
        , std::move(params)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id));
}

//migrate forward done
void messaging_service::register_migrate_forward_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_FORWARD_DONE, std::move(func));
}

void messaging_service::unregister_migrate_forward_done() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_FORWARD_DONE);
}

future<> messaging_service::migrate_forward_done(msg_addr id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MIGRATE_FORWARD_DONE
        , std::move(id)
        , std::move(shard)
        , std::move(response_id));
}

//migrate forward exception
void messaging_service::register_migrate_forward_exception(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , sstring exception
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_FORWARD_EXCEPTION, std::move(func));
}

void messaging_service::unregister_migrate_forward_exception() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_FORWARD_EXCEPTION);
}

future<> messaging_service::migrate_forward_exception(msg_addr id
        , sstring exception, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MIGRATE_FORWARD_EXCEPTION
        , std::move(id)
        , std::move(exception)
        , std::move(shard)
        , std::move(response_id));
}

///////////////////////////////////
// for journal new version start
///////////////////////////////////
//--- init commitlog 
void messaging_service::register_init_segment(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_init_segment init_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::INIT_COMMITLOG, std::move(func));
}

void messaging_service::unregister_init_segment() {
    _rpc->unregister_handler(hive::messaging_verb::INIT_COMMITLOG);
}

future<> messaging_service::send_init_segment(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_init_segment& init_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::INIT_COMMITLOG
        , std::move(id)
        , init_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_init_segment_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::INIT_COMMITLOG_DONE, std::move(func));
}

void messaging_service::unregister_init_segment_done() {
    _rpc->unregister_handler(hive::messaging_verb::INIT_COMMITLOG_DONE);
}

future<> messaging_service::send_init_segment_done(
    msg_addr id
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway(
        this
        , messaging_verb::INIT_COMMITLOG_DONE
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- sync segment 
void messaging_service::register_sync_segment_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_sync_segment sync_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::SYNC_COMMITLOG_EX, std::move(func));
}

void messaging_service::unregister_sync_segment_ex() {
    _rpc->unregister_handler(hive::messaging_verb::SYNC_COMMITLOG_EX);
}

future<> messaging_service::send_sync_segment_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_sync_segment& sync_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::SYNC_COMMITLOG_EX
        , std::move(id)
        , sync_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_sync_segment_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::SYNC_COMMITLOG_DONE_EX, std::move(func));
}

void messaging_service::unregister_sync_segment_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::SYNC_COMMITLOG_DONE_EX);
}

future<> messaging_service::send_sync_segment_done_ex(
    msg_addr id
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway(
        this
        , messaging_verb::SYNC_COMMITLOG_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- discard segment 
void messaging_service::register_discard_segment_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_discard_segment discard_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DISCARD_COMMITLOG_EX, std::move(func));
}

void messaging_service::unregister_discard_segment_ex() {
    _rpc->unregister_handler(hive::messaging_verb::DISCARD_COMMITLOG_EX);
}

future<> messaging_service::send_discard_segment_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_discard_segment& discard_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::DISCARD_COMMITLOG_EX
        , std::move(id)
        , discard_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_discard_segment_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , response_id_type response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DISCARD_COMMITLOG_DONE_EX, std::move(func));
}

void messaging_service::unregister_discard_segment_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::DISCARD_COMMITLOG_DONE_EX);
}

future<> messaging_service::send_discard_segment_done_ex(
    msg_addr id
    , unsigned shard
    , response_id_type response_id) {

    return send_message_oneway(
        this
        , messaging_verb::DISCARD_COMMITLOG_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- get_journal_data
void messaging_service::register_get_journal_data(
    std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_get_journal_data>>> (
        const rpc::client_info&
        , hive::smd_get_journal_data smd_data
        )>&& func) {

    register_handler(this, hive::messaging_verb::GET_SEGMENT_DATA, std::move(func));
}

void messaging_service::unregister_get_journal_data() {
    _rpc->unregister_handler(hive::messaging_verb::GET_SEGMENT_DATA);
}

future<hive::rmd_get_journal_data> messaging_service::send_get_journal_data(
    msg_addr id
    , const hive::smd_get_journal_data& smd_data) {
    return send_message<hive::rmd_get_journal_data>(this, messaging_verb::GET_SEGMENT_DATA, std::move(id), smd_data);
}
///////////////////////////////////
// for journal new version end
///////////////////////////////////

///////////////////////////////////////
// for extent_store new version start
///////////////////////////////////////
//--- create_extent_group
void messaging_service::register_create_extent_group_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_create_extent_group smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::CREATE_EXTENT_GROUP_EX, std::move(func));
}

void messaging_service::unregister_create_extent_group_ex() {
    _rpc->unregister_handler(hive::messaging_verb::CREATE_EXTENT_GROUP_EX);
}

future<> messaging_service::send_create_extent_group_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_create_extent_group& smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::CREATE_EXTENT_GROUP_EX
        , std::move(id)
        , smd_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_create_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::CREATE_EXTENT_GROUP_DONE_EX, std::move(func));
}

void messaging_service::unregister_create_extent_group_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::CREATE_EXTENT_GROUP_DONE_EX);
}

future<> messaging_service::send_create_extent_group_done_ex(
    msg_addr id
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway(
        this
        , messaging_verb::CREATE_EXTENT_GROUP_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- delete_extent_group
void messaging_service::register_delete_extent_group_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_delete_extent_group smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DELETE_EXTENT_GROUP_EX, std::move(func));
}

void messaging_service::unregister_delete_extent_group_ex() {
    _rpc->unregister_handler(hive::messaging_verb::DELETE_EXTENT_GROUP_EX);
}

future<> messaging_service::send_delete_extent_group_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_delete_extent_group& smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::DELETE_EXTENT_GROUP_EX
        , std::move(id)
        , smd_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_delete_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::DELETE_EXTENT_GROUP_DONE_EX, std::move(func));
}

void messaging_service::unregister_delete_extent_group_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::DELETE_EXTENT_GROUP_DONE_EX);
}

future<> messaging_service::send_delete_extent_group_done_ex(
    msg_addr id
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway(
        this
        , messaging_verb::DELETE_EXTENT_GROUP_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- rwrite_extent_group
void messaging_service::register_rwrite_extent_group_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_rwrite_extent_group smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::RWRITE_EXTENT_GROUP_EX, std::move(func));
}

void messaging_service::unregister_rwrite_extent_group_ex() {
    _rpc->unregister_handler(hive::messaging_verb::RWRITE_EXTENT_GROUP_EX);
}

future<> messaging_service::send_rwrite_extent_group_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_rwrite_extent_group& smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::RWRITE_EXTENT_GROUP_EX
        , std::move(id)
        , smd_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_rwrite_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::RWRITE_EXTENT_GROUP_DONE_EX, std::move(func));
}

void messaging_service::unregister_rwrite_extent_group_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::RWRITE_EXTENT_GROUP_DONE_EX);
}

future<> messaging_service::send_rwrite_extent_group_done_ex(
    msg_addr id
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway(
        this
        , messaging_verb::RWRITE_EXTENT_GROUP_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- read_extent_group
void messaging_service::register_read_extent_group_ex(
    std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_read_extent_group>>> (
        const rpc::client_info&
        , hive::smd_read_extent_group smd_data
        )>&& func) {

    register_handler(this, hive::messaging_verb::READ_EXTENT_GROUP_EX, std::move(func));
}

void messaging_service::unregister_read_extent_group_ex() {
    _rpc->unregister_handler(hive::messaging_verb::READ_EXTENT_GROUP_EX);
}

future<hive::rmd_read_extent_group> messaging_service::send_read_extent_group_ex(
    msg_addr id
    , const hive::smd_read_extent_group& smd_data) {
    return send_message<hive::rmd_read_extent_group>(this, messaging_verb::READ_EXTENT_GROUP_EX, std::move(id), smd_data);
}

//--- migrate_extent_group
void messaging_service::register_migrate_extent_group_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_migrate_extent_group smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_EXTENT_GROUP_EX, std::move(func));
}

void messaging_service::unregister_migrate_extent_group_ex() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_EXTENT_GROUP_EX);
}

future<> messaging_service::send_migrate_extent_group_ex(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_migrate_extent_group& smd_data
    , std::vector<gms::inet_address> forward
    , inet_address reply_to
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::MIGRATE_EXTENT_GROUP_EX
        , std::move(id)
        , smd_data
        , std::move(forward)
        , std::move(reply_to)
        , std::move(shard)
        , std::move(response_id)
    );
}

void messaging_service::register_migrate_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
    const rpc::client_info& cinfo
    , unsigned shard
    , uint64_t response_id
    )>&& func) {

    register_handler(this, hive::messaging_verb::MIGRATE_EXTENT_GROUP_DONE_EX, std::move(func));
}

void messaging_service::unregister_migrate_extent_group_done_ex() {
    _rpc->unregister_handler(hive::messaging_verb::MIGRATE_EXTENT_GROUP_DONE_EX);
}

future<> messaging_service::send_migrate_extent_group_done_ex(
    msg_addr id
    , unsigned shard
    , uint64_t response_id) {

    return send_message_oneway(
        this
        , messaging_verb::MIGRATE_EXTENT_GROUP_DONE_EX
        , std::move(id)
        , std::move(shard)
        , std::move(response_id)
    );
}

//--- get_extent_group_data
void messaging_service::register_get_extent_group(
    std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_get_extent_group>>> (
        const rpc::client_info&
        , hive::smd_get_extent_group smd_data
        )>&& func) {

    register_handler(this, hive::messaging_verb::GET_EXTENT_GROUP, std::move(func));
}

void messaging_service::unregister_get_extent_group() {
    _rpc->unregister_handler(hive::messaging_verb::GET_EXTENT_GROUP);
}

future<hive::rmd_get_extent_group> messaging_service::send_get_extent_group(
    msg_addr id
    , const hive::smd_get_extent_group& smd_data) {
    return send_message<hive::rmd_get_extent_group>(this, messaging_verb::GET_EXTENT_GROUP, std::move(id), smd_data);
}
///////////////////////////////////////
// for extent_store new version end
///////////////////////////////////////

///////////////////////////////////////
// for drain tasks start
///////////////////////////////////////
void messaging_service::register_drain_task_group(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_drain_task_group smd_data
    , inet_address reply_to
    , unsigned shard
    )>&& func) {

    register_handler(this, hive::messaging_verb::DRAIN_TASK_GROUP, std::move(func));
}

void messaging_service::unregister_drain_task_group() {
    _rpc->unregister_handler(hive::messaging_verb::DRAIN_TASK_GROUP);
}

future<> messaging_service::send_drain_task_group(
    msg_addr id
    , clock_type::time_point timeout
    , hive::smd_drain_task_group& smd_data
    , inet_address reply_to
    , unsigned shard
    ) {

    return send_message_oneway_timeout(
        this
        , timeout
        , messaging_verb::DRAIN_TASK_GROUP
        , std::move(id)
        , smd_data
        , std::move(reply_to)
        , std::move(shard)
    );
}

void messaging_service::register_drain_task_group_done(std::function<future<rpc::no_wait_type> (
    const rpc::client_info&
    , hive::smd_drain_task_group smd_data
    , unsigned shard
    )>&& func) {

    register_handler(this, hive::messaging_verb::DRAIN_TASK_GROUP_DONE, std::move(func));
}

void messaging_service::unregister_drain_task_group_done() {
    _rpc->unregister_handler(hive::messaging_verb::DRAIN_TASK_GROUP_DONE);
}

future<> messaging_service::send_drain_task_group_done(
    msg_addr id
    , hive::smd_drain_task_group& smd_data
    , unsigned shard
    ) {

    return send_message_oneway(
        this
        , messaging_verb::DRAIN_TASK_GROUP_DONE
        , std::move(id)
        , smd_data
        , std::move(shard)
    );
}
///////////////////////////////////////
// for drain tasks end
///////////////////////////////////////
} // namespace net
