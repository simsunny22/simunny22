#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"
#include "core/distributed.hh"
#include "cql3/query_processor.hh"
#include "hive/messaging_service.hh"
#include "utils/UUID.hh"
#include "sstables/sstables.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include <map>
#include <vector>
#include <memory>
#include "hive/stream/stream_session_state.hh"
#include "hive/stream/stream_transfer_task.hh"
#include "hive/stream/stream_receive_task.hh"
#include "hive/stream/stream_request.hh"
#include "hive/stream/prepare_message.hh"
#include "hive/stream/stream_detail.hh"
#include "hive/stream/stream_manager.hh"
#include "hive/stream/session_info.hh"
#include "hive/stream/migrate_params_entry.hh"


namespace hive {

class stream_result_future;

/**
 * Handles the streaming a one or more section of one of more sstables to and from a specific
 * remote node.
 *
 * Both this node and the remote one will create a similar symmetrical StreamSession. A streaming
 * session has the following life-cycle:
 *
 * 1. Connections Initialization
 *
 *   (a) A node (the initiator in the following) create a new StreamSession, initialize it (init())
 *       and then start it (start()). Start will create a {@link ConnectionHandler} that will create
 *       two connections to the remote node (the follower in the following) with whom to stream and send
 *       a StreamInit message. The first connection will be the incoming connection for the
 *       initiator, and the second connection will be the outgoing.
 *   (b) Upon reception of that StreamInit message, the follower creates its own StreamSession,
 *       initialize it if it still does not exist, and attach connecting socket to its ConnectionHandler
 *       according to StreamInit message's isForOutgoing flag.
 *   (d) When the both incoming and outgoing connections are established, StreamSession calls
 *       StreamSession#onInitializationComplete method to start the streaming prepare phase
 *       (StreamResultFuture.startStreaming()).
 *
 * 2. Streaming preparation phase
 *
 *   (a) This phase is started when the initiator onInitializationComplete() method is called. This method sends a
 *       PrepareMessage that includes what files/sections this node will stream to the follower
 *       (stored in a StreamTransferTask, each column family has it's own transfer task) and what
 *       the follower needs to stream back (StreamReceiveTask, same as above). If the initiator has
 *       nothing to receive from the follower, it goes directly to its Streaming phase. Otherwise,
 *       it waits for the follower PrepareMessage.
 *   (b) Upon reception of the PrepareMessage, the follower records which files/sections it will receive
 *       and send back its own PrepareMessage with a summary of the files/sections that will be sent to
 *       the initiator (prepare()). After having sent that message, the follower goes to its Streamning
 *       phase.
 *   (c) When the initiator receives the follower PrepareMessage, it records which files/sections it will
 *       receive and then goes to his own Streaming phase.
 *
 * 3. Streaming phase
 *
 *   (a) The streaming phase is started by each node (the sender in the follower, but note that each side
 *       of the StreamSession may be sender for some of the files) involved by calling startStreamingFiles().
 *       This will sequentially send a FileMessage for each file of each SteamTransferTask. Each FileMessage
 *       consists of a FileMessageHeader that indicates which file is coming and then start streaming the
 *       content for that file (StreamWriter in FileMessage.serialize()). When a file is fully sent, the
 *       fileSent() method is called for that file. If all the files for a StreamTransferTask are sent
 *       (StreamTransferTask.complete()), the task is marked complete (taskCompleted()).
 *   (b) On the receiving side, a SSTable will be written for the incoming file (StreamReader in
 *       FileMessage.deserialize()) and once the FileMessage is fully received, the file will be marked as
 *       complete (received()). When all files for the StreamReceiveTask have been received, the sstables
 *       are added to the CFS (and 2ndary index are built, StreamReceiveTask.complete()) and the task
 *       is marked complete (taskCompleted())
 *   (b) If during the streaming of a particular file an I/O error occurs on the receiving end of a stream
 *       (FileMessage.deserialize), the node will retry the file (up to DatabaseDescriptor.getMaxStreamingRetries())
 *       by sending a RetryMessage to the sender. On receiving a RetryMessage, the sender simply issue a new
 *       FileMessage for that file.
 *   (c) When all transfer and receive tasks for a session are complete, the move to the Completion phase
 *       (maybeCompleted()).
 *
 * 4. Completion phase
 *
 *   (a) When a node has finished all transfer and receive task, it enter the completion phase (maybeCompleted()).
 *       If it had already received a CompleteMessage from the other side (it is in the WAIT_COMPLETE state), that
 *       session is done is is closed (closeSession()). Otherwise, the node switch to the WAIT_COMPLETE state and
 *       send a CompleteMessage to the other side.
 */
class stream_session : public enable_shared_from_this<stream_session> {
private:
    using messaging_verb = hive::messaging_verb;
    using messaging_service = hive::messaging_service;
    using msg_addr = hive::messaging_service::msg_addr;
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    static void init_messaging_service_handler();
public:
    static hive::messaging_service& ms() {
        return hive::get_local_messaging_service();
    }
    static future<> init_streaming_service(uint32_t stream_chunk_send_limit);
public:
    /**
     * Streaming endpoint.
     *
     * Each {@code StreamSession} is identified by this InetAddress which is broadcast address of the node streaming.
     */
    inet_address peer;
    unsigned dst_cpu_id;
private:
    // should not be null when session is started
    shared_ptr<stream_result_future> _stream_result;

    // stream requests to send to the peer
    std::vector<stream_request> _requests;
    // streaming tasks are created and managed per extent_group_id 
    //std::map<sstring, stream_transfer_task> _transfers;
    std::map<sstring, std::shared_ptr<stream_task>> _transfers;
    // data receivers, filled after receiving prepare message
    std::map<sstring, stream_receive_task> _receivers;
    //private final StreamingMetrics metrics;
    /* can be null when session is created in remote */
    //private final StreamConnectionFactory factory;

    int64_t _bytes_sent = 0;
    int64_t _bytes_received = 0;

    int _retries;
    bool _is_aborted =  false;

    stream_session_state _state = stream_session_state::INITIALIZED;
    bool _complete_sent = false;

    // If the session is idle for 10 minutes, close the session
    std::chrono::seconds _keep_alive_timeout{20};
    // Check every 1 minutes
    std::chrono::seconds _keep_alive_interval{5};
    timer<lowres_clock> _keep_alive;
    stream_bytes _last_stream_bytes;
    lowres_clock::time_point _last_stream_progress;

    session_info _session_info;
public:
    void start_keep_alive_timer() {
        _keep_alive.rearm(lowres_clock::now() + _keep_alive_interval);
    }

    void add_bytes_sent(int64_t bytes) {
        _bytes_sent += bytes;
    }

    void add_bytes_received(int64_t bytes) {
        _bytes_received += bytes;
    }

    int64_t get_bytes_sent() const {
        return _bytes_sent;
    }

    int64_t get_bytes_received() const {
        return _bytes_received;
    }
public:
    stream_session();
    /**
     * Create new streaming session with the peer.
     *
     * @param peer Address of streaming peer
     * @param connecting Actual connecting address
     * @param factory is used for establishing connection
     */
    stream_session(inet_address peer_);
    ~stream_session();

    UUID plan_id();

    sstring description();

public:
    /**
     * Bind this session to report to specific {@link StreamResultFuture} and
     * perform pre-streaming initialization.
     *
     * @param streamResult result to report to
     */
    void init(shared_ptr<stream_result_future> stream_result_);

    void start();

    void add_stream_request(migrate_params_entry params) {
        _requests.emplace_back(params);
    }

    void add_stream_transfer(migrate_params_entry params);

    void close_session(stream_session_state final_state);

public:
    /**
     * Set current state to {@code newState}.
     *
     * @param newState new state to set
     */
    void set_state(stream_session_state new_state) {
        _state = new_state;
    }

    /**
     * @return current state
     */
    stream_session_state get_state() {
        return _state;
    }

    /**
     * Return if this session completed successfully.
     *
     * @return true if session completed successfully.
     */
    bool is_success() {
        return _state == stream_session_state::COMPLETE;
    }

    future<> initiate();

    /**
     * Call back when connection initialization is complete to start the prepare phase.
     */
    future<> on_initialization_complete();

    /**l
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    void on_error();

    /**
     * Prepare this session for sending/receiving files.
     */
    future<prepare_message> prepare(std::vector<stream_request> requests, std::vector<stream_summary> summaries);

    void follower_start_sent();

    /**
     * Check if session is completed on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    void complete();

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    void session_failed();

    /**
     * @return Current snapshot of this session info.
     */
    session_info make_session_info();

    session_info& get_session_info() {
        return _session_info;
    }

    const session_info& get_session_info() const {
        return _session_info;
    }

    future<> update_progress();

    void receive_task_completed(sstring task_id);
    void transfer_task_completed(sstring task_id);
private:
    void send_complete_message();
    bool maybe_completed();

    future<> prepare_for_summary(std::vector<stream_summary> summaries);
    void prepare_receiving(stream_summary summary);

    void start_streaming_files();
    future<> touch_extent_group_file(sstring extent_group_id, sstring dst_disk_id, size_t truncate_size);
    future<> touch_commitlog_file(migrate_scene scene, sstring commitlog_file_name, size_t truncate_size);

    future<> receive_chunk(migrate_chunk chunk);
    future<> receive_extent_group_chunk(migrate_chunk chunk);
    future<> receive_extent_journal_chunk(migrate_chunk chunk);
};

} // namespace hive 
