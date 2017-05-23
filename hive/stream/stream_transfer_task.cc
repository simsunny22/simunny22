#include "hive/stream/stream_transfer_task.hh"
#include "log.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "mutation.hh"
#include "hive/messaging_service.hh"
#include "range.hh"
#include "dht/i_partitioner.hh"
#include "service/priority_manager.hh"
#include <boost/range/irange.hpp>

#include "hive/stream/stream_detail.hh"
#include "hive/stream/stream_session.hh"
#include "hive/stream/stream_manager.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/stream_file_reader.hh"
#include "hive/extent_store.hh"
#include "hive/hive_config.hh"
#include "hive/hive_service.hh"


namespace hive {

static logging::logger logger("stream_transfer_task");

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session_, migrate_params_entry params_)
    : stream_task(session_)
    , _params(params_)
{
}

stream_transfer_task::~stream_transfer_task() = default;

void stream_transfer_task::check_aborted(){
    if(!_aborted){
        return; 
    }else {
        auto err = sprint("task has been aborted, params:{}", _params);
        logger.warn(err.c_str());
        throw std::runtime_error(err);
    }
}

future<stop_iteration> stream_transfer_task::do_send_chunk(lw_shared_ptr<send_info> si, migrate_chunk chunk) {
    check_aborted();
    logger.debug("{} start, volume_id:{}, extent_group_id:{}, offset:{}, length:{}"
        ,__func__, chunk.volume_id, chunk.extent_group_id, chunk.offset, chunk.length);

    return get_local_stream_manager().chunk_send_limit().wait().then(
            [this, si, chunk=std::move(chunk)]() mutable {
        logger.debug("do_send_chunk, plan_id={}, send to {}, src_file_path={}", si->plan_id, si->peer_id, si->params.src_file_path);
        auto chunk_size = chunk.length;
        this->check_aborted();
        hive::get_local_messaging_service().send_stream_chunk(si->peer_id, si->plan_id
                , std::move(chunk), si->dst_cpu_id).then([si, chunk_size] {
            logger.debug("do_send_chunk, plan_id={}, got replay from {}", si->plan_id, si->peer_id.addr);
            get_local_stream_manager().update_progress(si->plan_id, si->peer_id.addr, progress_info::direction::OUT, chunk_size);
            si->chunk_done.signal();
        }).handle_exception([si] (auto ep) {
            // There might be larger number of STREAM_MUTATION inflight.
            // Log one error per column_family per range
            if (!si->error_logged) {
                si->error_logged = true;
                logger.error("error, do_send_chunk, plan_id={},fail to send migrate chunk to {}, exception:{}"
                    , si->plan_id, si->peer_id, ep);
            }
            si->chunk_done.broken();
        }).finally([] {
            get_local_stream_manager().chunk_send_limit().signal();
        });
        return stop_iteration::no;
    });
}

future<> stream_transfer_task::send_chunks(lw_shared_ptr<send_info> si) {
//tododl:test
//return test();

    sstring send_file_path = si->params.src_file_path; 
    logger.debug("{} start, params:{}" ,__func__, si->params);
    
    check_aborted();
    return open_file_dma(send_file_path, open_flags::ro).then([this, si](file f) mutable {
        file_input_stream_options stream_options;
        stream_options.buffer_size = hive::get_local_hive_service().get_hive_config()->stream_chunk_size_in_kb()*1024; 
        
        stream_file_reader file_reader(std::move(f), si->params.offset, si->params.length, stream_options);
        uint64_t offset = 0;
        logger.debug("send_chunks, open file success, file_path:{}", si->params.src_file_path);
        return do_with(std::move(file_reader), std::move(offset), [this, si](auto& file_reader, auto& offset){
            logger.debug("send_chunks, in do_with, will start repeat");
            return repeat([this, si, &offset, &file_reader](){
                return file_reader().then([this, si, &offset](auto data){
                    if(!data.empty()){
                        logger.debug("send_chunks, read file data size:{}", data.size());
                        uint64_t length = data.size();
                        bytes content(length, 0);
                        std::memcpy(content.begin(), data.begin(), length);
                        logger.debug("send_chunks, build chunk params:{}", si->params);
                        migrate_chunk chunk(si->params.volume_id
                                          , si->params.extent_group_id
                                          , offset
                                          , length
                                          , std::move(content)
                                          , si->params.src_disk_id
                                          , si->params.dst_disk_id);
                        offset += length;
                        
                        return this->do_send_chunk(si, std::move(chunk));
                    }else{
                        logger.debug("send_chunks, read file over");
                        return make_ready_future<stop_iteration>(stop_iteration::yes); 
                    }
                }); 
            }); 
        });
    }).then_wrapped([send_file_path](auto f){
        try {
            f.get(); 
            return make_ready_future<>();
        } catch (...) {
            std::ostringstream out;
            out << "[send_chunks] error, send_file_path:" << send_file_path;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

#if 0
void stream_transfer_task::start() {
    check_aborted();
    auto extent_group_id = this->_params.extent_group_id;
    auto& local_extent_store = hive::get_local_extent_store();
    auto shard_id = local_extent_store.shard_of(extent_group_id);
    auto& extent_store = hive::get_extent_store();
    logger.debug("start, extent_group_id:{}, shard_id:{}", extent_group_id, shard_id); 
    extent_store.invoke_on(shard_id, [this, extent_group_id](auto& shard_extent_store){
//return shard_extent_store.with_monad(extent_group_id, [this, extent_group_id](){
        auto plan_id = this->session->plan_id();
        auto peer_id = hive::messaging_service::msg_addr{session->peer, this->session->dst_cpu_id};
        logger.debug("transfer_task start send HIVE_STREAM_CHUNK plan_id={}, extent_group_id={}", plan_id, extent_group_id);
        auto si = make_lw_shared<send_info>(_params, plan_id, peer_id, this->session->dst_cpu_id);
        return this->send_chunks(si).then([this, plan_id, extent_group_id, peer_id] {
            logger.debug("transfer_task, plan_id={}, start send HIVE_STREAM_CHUNK_DONE to {}, extent_group_id={}"
                , plan_id, peer_id, extent_group_id);
            this->check_aborted();
            sstring task_id = extent_group_id;
            return this->session->ms().send_stream_chunk_done(peer_id, plan_id, 
                    task_id, this->session->dst_cpu_id).handle_exception([plan_id, peer_id, task_id] (auto ep) {
                logger.error("transfer_task, plan_id={}, fail to send HIVE_STREAM_CHUNK_DONE to {}, exception:{}", plan_id, peer_id, ep);
                std::rethrow_exception(ep);
            });
        }).then([this, peer_id, plan_id] {
            logger.debug("transfer_task, plan_id={}, got send HIVE_STREAM_CHUNK_DONE reply from {}", plan_id, peer_id.addr);
            this->session->start_keep_alive_timer();
            sstring task_id = this->_params.extent_group_id;
            this->session->transfer_task_completed(task_id);
        }).handle_exception([this, plan_id, peer_id] (auto ep){
            logger.error("error, transfer_task, plan_id={}, fail to send to {}, exception:{}", plan_id, peer_id, ep);
            this->session->on_error();
        });
         
//});//with_monad
    });//invoke_on
}
#endif

void stream_transfer_task::start() {
    check_aborted();
    auto extent_group_id = this->_params.extent_group_id;
    logger.debug("start, extent_group_id:{}, shard_id:{}", extent_group_id, engine().cpu_id()); 
    
    auto plan_id = this->session->plan_id();
    auto peer_id = hive::messaging_service::msg_addr{session->peer, this->session->dst_cpu_id};
    logger.debug("transfer_task start send HIVE_STREAM_CHUNK plan_id={}, extent_group_id={}", plan_id, extent_group_id);
    auto si = make_lw_shared<send_info>(_params, plan_id, peer_id, this->session->dst_cpu_id);
    
    send_chunks(si).then([this, plan_id, extent_group_id, peer_id] {
        logger.debug("transfer_task, plan_id={}, start send HIVE_STREAM_CHUNK_DONE to {}, extent_group_id={}"
            , plan_id, peer_id, extent_group_id);
        this->check_aborted();
        sstring task_id = extent_group_id;
        return this->session->ms().send_stream_chunk_done(peer_id, plan_id, 
                task_id, this->session->dst_cpu_id).handle_exception([plan_id, peer_id, task_id] (auto ep) {
            logger.error("transfer_task, plan_id={}, fail to send HIVE_STREAM_CHUNK_DONE to {}, exception:{}", plan_id, peer_id, ep);
            std::rethrow_exception(ep);
        });
    }).then([this, peer_id, plan_id] {
        logger.debug("transfer_task, plan_id={}, got send HIVE_STREAM_CHUNK_DONE reply from {}", plan_id, peer_id.addr);
        this->session->start_keep_alive_timer();
        sstring task_id = this->_params.extent_group_id;
        this->session->transfer_task_completed(task_id);
    }).handle_exception([this, plan_id, peer_id] (auto ep){
        logger.error("error, transfer_task, plan_id={}, fail to send to {}, exception:{}", plan_id, peer_id, ep);
        this->session->on_error();
    });
}

future<> stream_transfer_task::test(){
    sstring src_file_path = "/opt/vega/data/hive/disk1/src_file";
    sstring dst_file_path = "/opt/vega/data/hive/disk1/dst_file";
    logger.debug("[{}] start, src_file:{}, dst_file:{}", __func__, src_file_path, dst_file_path);

    return open_file_dma(src_file_path, open_flags::ro).then([this, dst_file_path](file f) mutable {
        file_input_stream_options stream_options;
        stream_options.buffer_size = hive::get_local_hive_service().get_hive_config()->stream_chunk_size_in_kb()*1024; 
        stream_file_reader file_reader(std::move(f), 0, 33554432, stream_options);

        uint64_t offset = 0;
        return do_with(std::move(file_reader), std::move(offset), [this, dst_file_path](auto& file_reader, auto& offset){
            return repeat([this, &offset, &file_reader, dst_file_path](){
                return file_reader().then([this, &offset, dst_file_path](auto data){
                    if(!data.empty()){
                        uint64_t length = data.size();
                        bytes content(length, 0);
                        std::memcpy(content.begin(), data.begin(), length);

                        return do_with(std::move(content), [dst_file_path, &offset, length](auto& content){
                            return hive::write_file(dst_file_path, offset, length, content).then(
                                    [&offset, length](){
                                logger.debug("[test] offset:{}, length:{}", offset, length);
                                offset += length;
                                return stop_iteration::no;
                            });
                        }); 
                    }else{
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    }
                });
            });
        });
    });
}


} // namespace streaming
