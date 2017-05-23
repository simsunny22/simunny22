#pragma once

#include "utils/UUID.hh"
#include "core/shared_ptr.hh"
#include "core/temporary_buffer.hh"
#include "hive/messaging_service.hh"
#include "sstables/sstables.hh"
#include <map>
#include <seastar/core/semaphore.hh>

#include "hive/stream/stream_task.hh"
#include "hive/stream/stream_detail.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/migrate_chunk.hh"

namespace hive {

class stream_session;

struct send_info {
    migrate_params_entry params;
    utils::UUID plan_id;
    hive::messaging_service::msg_addr peer_id;
    uint32_t dst_cpu_id;
    size_t chunk_nr{0};
    semaphore chunk_done{0};
    bool error_logged = false;

    send_info(migrate_params_entry params_ 
            , utils::UUID plan_id_
            , hive::messaging_service::msg_addr peer_id_
            , uint32_t dst_cpu_id_)
                : params(params_)
                , plan_id(plan_id_)
                , peer_id(peer_id_)
                , dst_cpu_id(dst_cpu_id_) {}
};

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
class stream_transfer_task : public stream_task {
private:
    migrate_params_entry _params;
    bool _aborted = false;
public:
    stream_transfer_task(stream_transfer_task&&) = default;
    stream_transfer_task(shared_ptr<stream_session> session_
                       , migrate_params_entry params_);

    void check_aborted();

    ~stream_transfer_task();
public:
    virtual void abort() override {
        _aborted = true;
    }

    virtual long get_total_size() override {
        return _params.length;
    }

    virtual stream_summary get_summary() override {
        return stream_summary(_params.extent_group_id, _params.dst_disk_id, get_total_size());
    }

    void start();

    future<stop_iteration> do_send_chunk(lw_shared_ptr<send_info> si, migrate_chunk chunk);
    future<> send_chunks(lw_shared_ptr<send_info> si);

    future<> test();
};

} // namespace hive 
