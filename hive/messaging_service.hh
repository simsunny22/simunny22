#pragma once

#include "core/reactor.hh"
#include "core/distributed.hh"
#include "core/sstring.hh"
#include "gms/inet_address.hh"
#include "rpc/rpc_types.hh"
#include <unordered_map>
#include "query-request.hh"
#include "mutation_query.hh"
#include "range.hh"
#include <seastar/net/tls.hh>

namespace hive {
    class prepare_message;
    class send_file_info;
    class hive_result;
    class hive_result_digest;
    class extent_datum;
    class extent_revision;
    class extent_revision_set;
    class migrate_params_entry;
    class migrate_chunk;
    class test_message;
    class replay_position;
    //class hive_read_command;
    class sync_commitlog_data_type;

    //for journal message data type 
    class smd_init_segment;
    class smd_sync_segment;
    class smd_discard_segment; 
    class smd_get_journal_data;
    class rmd_get_journal_data;

    //for extent_store message data type
    class smd_create_extent_group;
    class smd_delete_extent_group;
    class smd_rwrite_extent_group;
    class smd_migrate_extent_group;
    class smd_read_extent_group;
    class rmd_read_extent_group;
    class smd_get_extent_group;
    class rmd_get_extent_group;

    //for drain
    class smd_drain_task_group;
}

namespace utils {
    class UUID;
}

using response_id_type = uint64_t;
namespace hive{

/* All verb handler identifiers */
enum class messaging_verb : int32_t {
    CLIENT_ID = 0,
    LAST = 1,

    ///////////////////////////////////////
    //used by hive
    DATUM              = 23,
    DATUM_DONE         = 24,
    READ_EXTENT        = 25,
    READ_EXTENT_DIGEST = 26,

    CREATE_EXTENT_GROUP      = 27,
    CREATE_EXTENT_GROUP_DONE = 28,

    DELETE_EXTENT_GROUP      = 29,
    DELETE_EXTENT_GROUP_DONE = 30,

    WRITE_JOURNAL      = 31,
    WRITE_JOURNAL_DONE = 32,

    READ_JOURNAL        = 33,
    READ_JOURNAL_DIGEST = 34,

    START_SLAVE_JOURNAL = 35,
    STOP_SLAVE_JOURNAL  = 36,

    WRITE_REVISIONS      = 37,
    WRITE_REVISIONS_DONE = 38,

    SYNC_COMMITLOG         = 39,
    SYNC_COMMITLOG_DONE    = 40,
    DISCARD_COMMITLOG      = 41,
    DISCARD_COMMITLOG_DONE = 42,

    //for hive streaming
    HIVE_PREPARE_MESSAGE      = 43,
    HIVE_PREPARE_MESSAGE_DONE = 44,
    HIVE_STREAM_CHUNK         = 45,
    HIVE_STREAM_CHUNK_DONE    = 46,
    HIVE_COMPLETE_MESSAGE     = 47,

    //for test
    TEST_MESSAGE= 48,

    MIGRATE_EXTENT_GROUP      = 49,
    MIGRATE_EXTENT_GROUP_DONE = 50,

    MIGRATE_FORWARD           = 51,
    MIGRATE_FORWARD_DONE      = 52,
    MIGRATE_FORWARD_EXCEPTION = 53,

    /////////////////////////
    //for journal new version
    INIT_COMMITLOG            = 60,
    INIT_COMMITLOG_DONE       = 61,
    SYNC_COMMITLOG_EX         = 62,
    SYNC_COMMITLOG_DONE_EX    = 63,
    DISCARD_COMMITLOG_EX      = 64,
    DISCARD_COMMITLOG_DONE_EX = 65,
    GET_SEGMENT_DATA          = 66,

    /////////////////////////
    //for extent_store new version
    CREATE_EXTENT_GROUP_EX         = 70,
    CREATE_EXTENT_GROUP_DONE_EX    = 71,

    DELETE_EXTENT_GROUP_EX         = 72,
    DELETE_EXTENT_GROUP_DONE_EX    = 73,

    RWRITE_EXTENT_GROUP_EX         = 74,
    RWRITE_EXTENT_GROUP_DONE_EX    = 75,

    READ_EXTENT_GROUP_EX           = 76,

    MIGRATE_EXTENT_GROUP_EX        = 78,
    MIGRATE_EXTENT_GROUP_DONE_EX   = 79,
    GET_EXTENT_GROUP               = 80,

    /////////////////////////
    //for drain
    DRAIN_TASK_GROUP = 90,
    DRAIN_TASK_GROUP_DONE = 91,

};

} // namespace hive

namespace std {
template <>
class hash<hive::messaging_verb> {
public:
    size_t operator()(const hive::messaging_verb& x) const {
        return hash<int32_t>()(int32_t(x));
    }
};
} // namespace std

namespace hive {

struct serializer {};

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y);
    friend bool operator<(const msg_addr& x, const msg_addr& y);
    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);
    struct hash {
        size_t operator()(const msg_addr& id) const;
    };
};

class messaging_service : public seastar::async_sharded_service<messaging_service> {
public:
    struct rpc_protocol_wrapper;
    struct rpc_protocol_client_wrapper;
    struct rpc_protocol_server_wrapper;
    struct shard_info;

    using msg_addr = hive::msg_addr;
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    using clients_map = std::unordered_map<msg_addr, shard_info, msg_addr::hash>;

    // This should change only if serialization format changes
    static constexpr int32_t current_version = 0;

    struct shard_info {
        shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client);
        shared_ptr<rpc_protocol_client_wrapper> rpc_client;
        rpc::stats get_stats() const;
    };

    void foreach_client(std::function<void(const msg_addr& id, const shard_info& info)> f) const;

    void increment_dropped_messages(messaging_verb verb);

    uint64_t get_dropped_messages(messaging_verb verb) const;

    const uint64_t* get_dropped_messages() const;

    int32_t get_raw_version(const gms::inet_address& endpoint) const;

    bool knows_version(const gms::inet_address& endpoint) const;

    enum class encrypt_what {
        none,
        rack,
        dc,
        all,
    };

private:
    gms::inet_address _listen_address;
    uint16_t _port;
    uint16_t _ssl_port;
    encrypt_what _encrypt_what;
    // map: Node broadcast address -> Node internal IP for communication within the same data center
    std::unordered_map<gms::inet_address, gms::inet_address> _preferred_ip_cache;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::unique_ptr<rpc_protocol_server_wrapper> _server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    std::unique_ptr<rpc_protocol_server_wrapper> _server_tls;
    std::array<clients_map, 3> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
    bool _stopping = false;
public:
    using clock_type = std::chrono::steady_clock;
public:
    messaging_service(gms::inet_address ip = gms::inet_address("0.0.0.0"), uint16_t port = 7000);
    messaging_service(gms::inet_address ip, uint16_t port, encrypt_what,
            uint16_t ssl_port, ::shared_ptr<seastar::tls::server_credentials>);
    ~messaging_service();
public:
    uint16_t port();
    gms::inet_address listen_address();
    future<> stop();
    static rpc::no_wait_type no_wait();
    bool is_stopping() { return _stopping; }
public:
    gms::inet_address get_preferred_ip(gms::inet_address ep);
    future<> init_local_preferred_ip_cache();
    void cache_preferred_ip(gms::inet_address ep, gms::inet_address ip);

    void foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const;
    //Hive Wrapper for DATUM
    void register_datum(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::extent_datum datum, std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id
        )>&& func);

    void unregister_datum();
    future<> send_datum(msg_addr id
        , clock_type::time_point timeout
        , hive::extent_datum& datum
        , std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id);

    //Hive Wrapper for DATUM_DONE
    void register_datum_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard, sstring disk_id, response_id_type response_id
        )>&& func);

    void unregister_datum_done();
    future<> send_datum_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id);

    //Hive Wrapper for create_extent_group
    void register_create_extent_group(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , sstring extent_group_id, sstring disk_id, std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id
        )>&& func);

    void unregister_create_extent_group();
    future<> create_extent_group(msg_addr id
        , clock_type::time_point timeout
        , sstring extent_group_id
        , sstring disk_id
        , std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id);

    void register_create_extent_group_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard, sstring disk_id, response_id_type response_id
        )>&& func);

    void unregister_create_extent_group_done();
    future<> create_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id);

    //Hive Wrapper for delete_extent_group
    void register_delete_extent_group(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , sstring extent_group_id, sstring disk_id, std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id
        )>&& func);

    void unregister_delete_extent_group();
    future<> delete_extent_group(msg_addr id
        , clock_type::time_point timeout
        , sstring extent_group_id
        , sstring disk_id
        , std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard, response_id_type response_id);

    void register_delete_extent_group_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard, sstring disk_id, response_id_type response_id
        )>&& func);

    void unregister_delete_extent_group_done();
    future<> delete_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id);

    // Wrapper for READ_JOURNAL
    // Note: WTH is future<foreign_ptr<lw_shared_ptr<query::result>>
    //void register_read_journal(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>> (const rpc::client_info&, hive_read_command cmd)>&& func);
    //void unregister_read_journal();
    //future<query::result> send_read_journal(msg_addr id, clock_type::time_point timeout, const hive_read_command& cmd);

    //// Wrapper for READ_JOURNAL_DIGEST
    //void register_read_journal_digest(std::function<future<query::result_digest> (const rpc::client_info&, hive_read_command cmd)>&& func);
    //void unregister_read_journal_digest();
    //future<query::result_digest> send_read_journal_digest(msg_addr id, clock_type::time_point timeout, const hive_read_command& cmd);

    // Wrapper for START_SLAVE_JOURNAL
    void register_start_secondary_journal(std::function<future<> (const rpc::client_info&, sstring volume_id)>&& func);
    void unregister_start_secondary_journal();
    future<> send_start_secondary_journal(msg_addr id, clock_type::time_point timeout, const sstring volume_id);

    // Wrapper for START_SLAVE_JOURNAL
    void register_stop_secondary_journal(std::function<future<> (const rpc::client_info&, sstring volume_id)>&& func);
    void unregister_stop_secondary_journal();
    future<> send_stop_secondary_journal(msg_addr id, clock_type::time_point timeout, const sstring volume_id);

    //Hive Wrapper for WRITE_REVISIONS
    void register_write_revisions(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::extent_revision_set revision_set
        , std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_write_revisions();
    future<> send_revisions(msg_addr id
        , clock_type::time_point timeout
        , hive::extent_revision_set& revision_set
        , std::vector<sstring> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);

    void register_write_revisions_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , sstring disk_id
        , response_id_type response_id
        )>&& func);

    void unregister_write_revisions_done();
    future<> send_revisions_done(
        msg_addr id
        , unsigned shard
        , sstring disk_id
        , response_id_type response_id);

    //Hive Wrpper for SYNC_COMMITLOG
    void register_sync_commitlog(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::sync_commitlog_data_type sync_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_sync_commitlog();
    future<> send_sync_commitlog(msg_addr id
        , clock_type::time_point timeout
        , hive::sync_commitlog_data_type& sync_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);

    void register_sync_commitlog_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_sync_commitlog_done();
    future<> send_sync_commitlog_done(
        msg_addr id
        , unsigned shard
        , response_id_type response_id);

    //Hive Wrpper for DISCARD_COMMITLOG_DONE
    void register_discard_commitlog(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , sstring commitlog_id
        , replay_position rp
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_discard_commitlog();
    future<> send_discard_commitlog(msg_addr id
        , clock_type::time_point timeout
        , sstring commitlog_id
        , replay_position rp
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);

    void register_discard_commitlog_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_discard_commitlog_done();
    future<> send_discard_commitlog_done(
        msg_addr id
        , unsigned shard
        , response_id_type response_id);


    //Hive Wrpper for hive_sreaming
    //---prepare_message
    void register_hive_prepare_message(std::function<future<hive::prepare_message> (
        const rpc::client_info& cinfo
        , hive::prepare_message msg
        , UUID plan_id
        , sstring description)>&& func);

    future<hive::prepare_message> send_hive_prepare_message(
        msg_addr peer_id
        , hive::prepare_message msg
        , UUID plan_id
        , sstring description);

    //---prepare_message_done
    void register_hive_prepare_message_done(std::function<future<> (
        const rpc::client_info& cinfo
        , UUID plan_id
        , unsigned dst_cpu_id)>&& func);

    future<> send_hive_prepare_message_done(msg_addr peer_id, UUID plan_id, unsigned dst_cpu_id);

    //---stream_block
    void register_stream_chunk(std::function<future<> (
        const rpc::client_info& cinfo
        , UUID plan_id
        , hive::migrate_chunk chunk
        , unsigned dst_cpu_id)>&& func);

    future<> send_stream_chunk(msg_addr id
                                     , UUID plan_id
                                     , hive::migrate_chunk chunk
                                     , unsigned dst_cpu_id);

    //---stream_block_done
    void register_stream_chunk_done(std::function<future<> (
        const rpc::client_info& cinfo
        , UUID plan_id
        , sstring extent_group_id
        , unsigned dst_cpu_id)>&& func);

    future<> send_stream_chunk_done(msg_addr peer_id
                                 , UUID plan_id
                                 , sstring task_id
                                 , unsigned dst_cpu_id);

    //---complete_message
    void register_hive_complete_message(std::function<future<> (
        const rpc::client_info& cinfo
        , UUID plan_id
        , unsigned dst_cpu_id)>&& func);

    future<> send_hive_complete_message(msg_addr peer_id, UUID plan_id, unsigned dst_cpu_id);

    //---send_test_message
    void register_test_message(std::function<future<> (
        const rpc::client_info& cinfo
        , hive::test_message msg
        )>&& func);

    void unregister_test_message();
    future<> send_test_message(msg_addr id
        , clock_type::time_point timeout
        , hive::test_message msg);

    //migrate_extent_group ex
    void register_migrate_extent_group(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::migrate_params_entry migrate_params
        , inet_address reply_to
        , sstring disk_id
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_migrate_extent_group();
    future<> migrate_extent_group(msg_addr id
        , clock_type::time_point timeout
        , hive::migrate_params_entry migrate_params
        , inet_address reply_to
        , sstring disk_id
        , unsigned shard
        , response_id_type response_id);

    void register_migrate_extent_group_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , sstring disk_id
        , response_id_type response_id
        )>&& func);

    void unregister_migrate_extent_group_done();
    future<> migrate_extent_group_done(msg_addr id, unsigned shard, sstring disk_id, response_id_type response_id);

    //migrate forward
    void register_migrate_forward(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::migrate_params_entry migrate_params
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_migrate_forward();
    future<> migrate_forward(msg_addr id
        , clock_type::time_point timeout
        , hive::migrate_params_entry migrate_params
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);

    void register_migrate_forward_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_migrate_forward_done();
    future<> migrate_forward_done(msg_addr id, unsigned shard, response_id_type response_id);

    void register_migrate_forward_exception(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , sstring exception
        , unsigned shard
        , response_id_type response_id
        )>&& func);

    void unregister_migrate_forward_exception();
    future<> migrate_forward_exception(msg_addr id, sstring exception, unsigned shard, response_id_type response_id);

    //////////////////////////////////
    // for journal new version start
    //////////////////////////////////
    //--- init segment
    void register_init_segment(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_init_segment init_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_init_segment();
    future<> send_init_segment(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_init_segment& init_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);
    
    void register_init_segment_done(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_init_segment_done();
    future<> send_init_segment_done(
        msg_addr id
        , unsigned shard
        , response_id_type response_id);

    //--- sync segment
    void register_sync_segment_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_sync_segment sync_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_sync_segment_ex();
    future<> send_sync_segment_ex(msg_addr id
        , clock_type::time_point timeout
        , hive::smd_sync_segment& sync_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);
    
    void register_sync_segment_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_sync_segment_done_ex();
    future<> send_sync_segment_done_ex(
        msg_addr id
        , unsigned shard
        , response_id_type response_id);

    //--- discard segment
    void register_discard_segment_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_discard_segment discard_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_discard_segment_ex();
    future<> send_discard_segment_ex(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_discard_segment& discard_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , response_id_type response_id);

    void register_discard_segment_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , response_id_type response_id
        )>&& func);
    void unregister_discard_segment_done_ex();
    future<> send_discard_segment_done_ex(
        msg_addr id
        , unsigned shard
        , response_id_type response_id);

    //--- get_journal_data
    void register_get_journal_data(
        std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_get_journal_data>>> (
            const rpc::client_info&
            , hive::smd_get_journal_data smd_data
            )>&& func);

    void unregister_get_journal_data();
    future<hive::rmd_get_journal_data> send_get_journal_data(
        msg_addr id
        , const hive::smd_get_journal_data& smd_data);

    /////////////////////////////////
    // for journal new version end
    /////////////////////////////////

    //////////////////////////////////////
    // for extent_store new version start
    //////////////////////////////////////
    //--- create_extent_group
    void register_create_extent_group_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_create_extent_group smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_create_extent_group_ex();
    future<> send_create_extent_group_ex(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_create_extent_group& smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id);

    void register_create_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_create_extent_group_done_ex();
    future<> send_create_extent_group_done_ex(
        msg_addr id
        , unsigned shard
        , uint64_t response_id);

    //--- delete_extent_group
    void register_delete_extent_group_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_delete_extent_group smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_delete_extent_group_ex();
    future<> send_delete_extent_group_ex(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_delete_extent_group& smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id);

    void register_delete_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_delete_extent_group_done_ex();
    future<> send_delete_extent_group_done_ex(
        msg_addr id
        , unsigned shard
        , uint64_t response_id);

    //--- rwrite_extent_group
    void register_rwrite_extent_group_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_rwrite_extent_group smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_rwrite_extent_group_ex();
    future<> send_rwrite_extent_group_ex(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_rwrite_extent_group& smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id);

    void register_rwrite_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_rwrite_extent_group_done_ex();
    future<> send_rwrite_extent_group_done_ex(
        msg_addr id
        , unsigned shard
        , uint64_t response_id);

    //--- read_extent_group
    void register_read_extent_group_ex(
        std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_read_extent_group>>> (
            const rpc::client_info&
            , hive::smd_read_extent_group smd_data
            )>&& func);

    void unregister_read_extent_group_ex();
    future<hive::rmd_read_extent_group> send_read_extent_group_ex(
        msg_addr id
        , const hive::smd_read_extent_group& smd_data);


    //--- migrate_extent_group
    void register_migrate_extent_group_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info&
        , hive::smd_migrate_extent_group smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_migrate_extent_group_ex();
    future<> send_migrate_extent_group_ex(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_migrate_extent_group& smd_data
        , std::vector<gms::inet_address> forward
        , inet_address reply_to
        , unsigned shard
        , uint64_t response_id);

    void register_migrate_extent_group_done_ex(std::function<future<rpc::no_wait_type> (
        const rpc::client_info& cinfo
        , unsigned shard
        , uint64_t response_id
        )>&& func);
    void unregister_migrate_extent_group_done_ex();
    future<> send_migrate_extent_group_done_ex(
        msg_addr id
        , unsigned shard
        , uint64_t response_id);

    //--- get_extent_group 
    void register_get_extent_group(
        std::function<future<foreign_ptr<lw_shared_ptr<hive::rmd_get_extent_group>>> (
            const rpc::client_info&
            , hive::smd_get_extent_group smd_data
            )>&& func);

    void unregister_get_extent_group();
    future<hive::rmd_get_extent_group> send_get_extent_group(
        msg_addr id
        , const hive::smd_get_extent_group& smd_data);
    //////////////////////////////////////
    // for extent_store new version end
    //////////////////////////////////////

    //////////////////////////////////////
    // for drain tasks start //
    //////////////////////////////////////
    //--- drain_task 
    void register_drain_task_group(std::function<future<rpc::no_wait_type> (
          const rpc::client_info& cinfo
          , hive::smd_drain_task_group smd_data
          , inet_address reply_to
          , unsigned shard
          )>&& func);
    void unregister_drain_task_group();
    future<> send_drain_task_group(
        msg_addr id
        , clock_type::time_point timeout
        , hive::smd_drain_task_group& smd_data
        , inet_address reply_to
        , unsigned shard
        );

    //--- drain_task_done
    void register_drain_task_group_done(std::function<future<rpc::no_wait_type> (
          const rpc::client_info& cinfo
          , hive::smd_drain_task_group group 
          , unsigned shard
          )>&& func);
    void unregister_drain_task_group_done();
    future<> send_drain_task_group_done(
        msg_addr id
        , hive::smd_drain_task_group& group 
        , unsigned shard
        );


    //////////////////////////////////////
    // for drain tasks end //
    //////////////////////////////////////
public:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    shared_ptr<rpc_protocol_client_wrapper> get_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only);
    void remove_error_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client(msg_addr id);
    std::unique_ptr<rpc_protocol_wrapper>& rpc();
    static msg_addr get_source(const rpc::client_info& client);
};

extern distributed<messaging_service> _the_messaging_service;

inline distributed<messaging_service>& get_messaging_service() {
    return _the_messaging_service;
}

inline messaging_service& get_local_messaging_service() {
    return _the_messaging_service.local();
}

} // namespace net
