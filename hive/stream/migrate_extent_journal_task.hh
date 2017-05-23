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

struct send_info_ex {
    migrate_params_entry params;
    utils::UUID plan_id;
    hive::messaging_service::msg_addr peer_id;
    uint32_t dst_cpu_id;
    size_t chunk_nr{0};
    semaphore chunk_done{0};
    bool error_logged = false;

    send_info_ex(migrate_params_entry params_ 
            , utils::UUID plan_id_
            , hive::messaging_service::msg_addr peer_id_
            , uint32_t dst_cpu_id_)
                : params(params_)
                , plan_id(plan_id_)
                , peer_id(peer_id_)
                , dst_cpu_id(dst_cpu_id_) {}
};

class migrate_extent_journal_task : public stream_task {
private:
    migrate_params_entry _params;
    bool _aborted = false;
public:
    migrate_extent_journal_task(migrate_extent_journal_task&&) = default;
    migrate_extent_journal_task(shared_ptr<stream_session> session_
                              , migrate_params_entry params_);

    ~migrate_extent_journal_task();

    void check_aborted();
    
    void start();
    
    virtual void abort() override {
        _aborted = true;
    }

    virtual long get_total_size() override {
        return _params.length;
    }
    
    virtual stream_summary get_summary() override {
        auto scene = _params.scene;
        auto file_name = _params.commitlog_file_name;
        auto file_size = _params.length;
        return stream_summary(scene, file_name, file_size);
    }

    future<stop_iteration> do_send_chunk(lw_shared_ptr<send_info_ex> si, migrate_chunk chunk);
    future<> send_chunks(lw_shared_ptr<send_info_ex> si);
};

} // namespace streaming
