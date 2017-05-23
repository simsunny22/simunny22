#include "db/consistency_level.hh"
#include "journal_proxy.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "query_result_merger.hh"
#include "core/do_with.hh"
#include "hive/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "core/future-util.hh"
#include "db/read_repair_decision.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/algorithm/count_if.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/remove_if.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/numeric.hpp>
#include <boost/range/algorithm/sort.hpp>
#include "utils/latency.hh"
#include "schema.hh"
#include "schema_registry.hh"
#include "utils/joinpoint.hh"

#include "hive/hive_service.hh"
#include "hive/message_data_type.hh"
#include "hive/stream_service.hh"
#include "hive/exceptions/exceptions.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "hive/journal/journal_service.hh"
#include "hive/context/context_service.hh"
#include "partition_slice_builder.hh"
#include <seastar/core/sleep.hh>
#include "hive/journal/volume_revision_set.hh"

using namespace std::chrono_literals;

namespace hive {

extern hive::hive_status hive_service_status;
static logging::logger logger("journal_proxy");

distributed<hive::journal_proxy> _the_journal_proxy;

using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}
static inline sstring get_local_ip() {
    return utils::fb_utilities::get_broadcast_address().to_sstring();
}

//define for inner struct
//journal_proxy::rh_entry::rh_entry(std::unique_ptr<abstract_journal_response_handler>&& h
journal_proxy::rh_entry::rh_entry(std::unique_ptr<common_response_handler>&& h
                                    , std::function<void()>&& cb) 
                                    : handler(std::move(h)), expire_timer(std::move(cb)) {}

journal_proxy::unique_response_handler::unique_response_handler(
    journal_proxy& p_
    , response_id_type id_) 
    : id(id_), p(p_) {}

journal_proxy::unique_response_handler::unique_response_handler(
    unique_response_handler&& x) 
    : id(x.id), p(x.p) { x.id = 0; };

journal_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
journal_proxy::response_id_type journal_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

//define for journal_proxy
journal_proxy::journal_proxy(){
    init_messaging_service();
}

journal_proxy::~journal_proxy() {
    uninit_messaging_service();
}

void journal_proxy::init_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    //for init_segment
    ms.register_init_segment([](const rpc::client_info& cinfo
                                , smd_init_segment init_data
                                , std::vector<gms::inet_address> forward
                                , gms::inet_address reply_to
                                , unsigned shard
                                , response_id_type response_id){
        logger.debug("receive INIT_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(init_data), get_local_shared_journal_proxy(),
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
               (smd_init_segment& init_data, shared_ptr<journal_proxy>& proxy) mutable{
            ++proxy->_stats.received_creates;
            proxy->_stats.forwarded_creates += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &init_data, &reply_to] {
                    return proxy->init_segment_locally(reply_to, init_data); 
                }).then([reply_to, shard, response_id](){
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send INIT_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_init_segment_done(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                               .then_wrapped([](future<> f){
                          f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id](std::exception_ptr eptr){
                    logger.debug("failed to init segment from {}#{}, response_id:{}, exception: {}"
                        , reply_to, shard, response_id, eptr);
                }),

               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &init_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a init segment to {}, response_id:{}", forward, response_id);
                   return ms.send_init_segment(hive::messaging_service::msg_addr{forward, 0}
                                                  , timeout
                                                  , init_data 
                                                  , {}
                                                  , reply_to
                                                  , shard
                                                  , response_id).then_wrapped([&proxy] (future<> f) {
                       if(f.failed()) {
                           ++proxy->_stats.forwarding_create_errors; 
                       }
                       f.ignore_ready_future();
                   });
               })
            ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
                return hive::messaging_service::no_wait();
            });
        });
    });
    
    ms.register_init_segment_done([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive INIT_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy().invoke_on(shard, [from, response_id] (journal_proxy& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //for sync_segment
    ms.register_sync_segment_ex([](const rpc::client_info& cinfo
                                   , smd_sync_segment sync_data
                                   , std::vector<gms::inet_address> forward
                                   , gms::inet_address reply_to
                                   , unsigned shard
                                   , response_id_type response_id) {
        logger.debug("receive SYNC_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(sync_data), get_local_shared_journal_proxy(), 
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id] 
               (smd_sync_segment& sync_data, shared_ptr<journal_proxy>& proxy) mutable{
           ++proxy->_stats.received_syncs;
           proxy->_stats.forwarded_syncs += forward.size();
           return when_all(
               futurize<void>::apply([&proxy, &sync_data, reply_to] {
                   return proxy->sync_segment_locally(reply_to, sync_data);
               }).then([reply_to, shard, response_id] {
                   auto& ms = hive::get_local_messaging_service();
                   logger.debug("send SYNC_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                   return ms.send_sync_segment_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                           .then_wrapped([] (future<> f) {
                       f.ignore_ready_future();
                   });
               }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                   logger.debug("failed to sync segment from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
               }),
               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &sync_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a sync segment to {}, response_id:{}", forward, response_id);
                   return ms.send_sync_segment_ex(hive::messaging_service::msg_addr{forward, 0}
                                                  , timeout
                                                  , sync_data 
                                                  , {}
                                                  , reply_to
                                                  , shard
                                                  , response_id).then_wrapped([&proxy] (future<> f) {
                       if(f.failed()) {
                           ++proxy->_stats.forwarding_sync_errors; 
                       }
                       f.ignore_ready_future();
                   });
               })
           ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
               //ignore ressult, since we'll be returning them via SYNC_COMMITLOG_DONE verbs
               return hive::messaging_service::no_wait();
           });
        });
    });

    ms.register_sync_segment_done_ex([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive SYNC_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy().invoke_on(shard, [from, response_id] (journal_proxy& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //for discard_segment
    ms.register_discard_segment_ex([](const rpc::client_info& cinfo
                                      , smd_discard_segment discard_data
                                      , std::vector<gms::inet_address> forward
                                      , gms::inet_address reply_to
                                      , unsigned shard
                                      , response_id_type response_id) {
        logger.debug("receive DISCARD_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(discard_data), get_local_shared_journal_proxy(), 
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id] 
               (smd_discard_segment& discard_data, shared_ptr<journal_proxy>& proxy) mutable{
           ++proxy->_stats.received_discards;
           proxy->_stats.forwarded_discards += forward.size();
           return when_all(
               futurize<void>::apply([&proxy, &discard_data, reply_to] {
                   return proxy->discard_secondary_segment_locally(reply_to, discard_data);
               }).then([reply_to, shard, response_id] {
                   auto& ms = hive::get_local_messaging_service();
                   logger.debug("send DISCARD_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                   return ms.send_discard_segment_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                           .then_wrapped([] (future<> f) {
                       f.ignore_ready_future();
                   });
               }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                   logger.debug("failed to discard segment from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
               }),
               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &discard_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a discard segment to {}, response_id:{}", forward, response_id);
                   return ms.send_discard_segment_ex(hive::messaging_service::msg_addr{forward, 0}
                                                  , timeout
                                                  , discard_data 
                                                  , {}
                                                  , reply_to
                                                  , shard
                                                  , response_id).then_wrapped([&proxy] (future<> f) {
                       if(f.failed()) {
                           ++proxy->_stats.forwarding_discard_errors; 
                       }
                       f.ignore_ready_future();
                   });
               })
           ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
               //ignore ressult, since we'll be returning them via DISCARD_COMMITLOG_DONE verbs
               return hive::messaging_service::no_wait();
           });
        });
    });

    ms.register_discard_segment_done_ex([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive DISCARD_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy().invoke_on(shard, [from, response_id] (journal_proxy& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //get segment data
    ms.register_get_journal_data([](const rpc::client_info& cinfo, smd_get_journal_data smd_data) {
        return do_with(get_local_shared_journal_proxy(), [&cinfo, smd_data=std::move(smd_data)]
                (shared_ptr<journal_proxy>& proxy)mutable {
            return proxy->get_journal_data_locally(smd_data);
        });
    });

}

void journal_proxy::uninit_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    ms.unregister_init_segment();
    ms.unregister_init_segment_done();

    ms.unregister_sync_segment_ex();
    ms.unregister_sync_segment_done_ex();

    ms.unregister_discard_segment_ex();
    ms.unregister_discard_segment_done_ex();
    ms.unregister_get_journal_data();
}

//response_id_type journal_proxy::register_response_handler(std::unique_ptr<abstract_journal_response_handler>&& h) {
response_id_type journal_proxy::register_response_handler(std::unique_ptr<common_response_handler>&& h) {
    auto id = h->response_id();
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), [this, id] {
    //for init_segment
    
        auto& e = _response_handlers.find(id)->second;
        if (!e.handler->achieved()) {
            std::ostringstream out;
            out << "response handler timeout, handler_id:" << e.handler->response_id();
            out << ", not response targets: " <<  hive_tools::format(e.handler->get_targets());
            sstring error_info = out.str();
            logger.error(error_info.c_str());
            //e.handler->trigger_exception(std::runtime_error(error_info));
            e.handler->trigger_stop();
        } 
        remove_response_handler(id);
    }));
    assert(e.second);
    return id;
}

common_response_handler& journal_proxy::get_response_handler(response_id_type id) {
    return *_response_handlers.find(id)->second.handler;
}

void journal_proxy::remove_response_handler(response_id_type id) {
    _response_handlers.erase(id);
}

void journal_proxy::got_response(response_id_type id, gms::inet_address from) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        if (it->second.handler->response(from)) {
            //last one, remove entry. Will cancel expiration timer too.
            remove_response_handler(id);         
        }
    }
}

future<> journal_proxy::response_wait(response_id_type id, clock_type::time_point timeout) {
    auto& e = _response_handlers.find(id)->second;
    e.expire_timer.arm(timeout);
    return e.handler->wait().discard_result();
}

future<> journal_proxy::response_end(future<> result, utils::latency_counter lc) {
    assert(result.available());
    try {
        result.get();
        logger.debug("[{}] success latency:{} ns", __func__, lc.stop().latency_in_nano());
        return make_ready_future<>();
        //tododl:yellow add some detailed exception
    } catch(...) {
        return make_exception_future<>(std::current_exception());
    }
}

//for init segment
future<> journal_proxy::init_segment(smd_init_segment data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy::init_segment targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    logger.debug("[{}] start, data:{}, targets.size:{}", __func__, data, targets.size());
  
    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, create_init_segment_response_handler(std::move(data), targets));
    
    auto timeout_in_ms = get_local_hive_service().get_hive_config()->init_commitlog_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    auto response_id = response_h.id;
    auto f = response_wait(response_id, timeout);
    init_segment_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy::create_init_segment_response_handler(
        smd_init_segment init_data,
        std::unordered_set<gms::inet_address> targets) {
    union_md data;
    data.init_segment =  std::make_shared<smd_init_segment>(std::move(init_data));
    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets)
    );

    return register_response_handler(std::move(handler));
}

void journal_proxy::init_segment_to_live_endpoints(
    response_id_type response_id
    , clock_type::time_point timeout) {

    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();

    auto& init_data = handler.get_data();

    auto lmutate = [this, &init_data, response_id, my_address] {
        logger.debug("[init_segment_to_live_endpoints] in lmutate, data:{}", *(init_data.init_segment));
        return init_segment_locally(my_address, *(init_data.init_segment)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &init_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[init_segment_to_live_endpoints] in rmutate, data:{}, forward.size():{}"
            , *(init_data.init_segment), forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_init_segment(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            *(init_data.init_segment), 
            std::move(forward), 
            my_address, 
            engine().cpu_id(), 
            response_id);
    };

    std::vector<std::vector<gms::inet_address>> all;
    std::vector<gms::inet_address> local;
    std::vector<gms::inet_address> remote;

    for(auto dest : get_response_handler(response_id).get_targets()) {
        is_me(dest) ? local.push_back(dest) : remote.push_back(dest);
    }
    if(local.size() > 0){
        all.push_back(std::move(local)); 
    }
    if(remote.size() > 0){
        all.push_back(std::move(remote));
    }

    for(auto forward : all) {
        auto coordinator = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (coordinator == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, coordinator, std::move(forward));
        }

        f.handle_exception([coordinator] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during init segment to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy::init_segment_locally(gms::inet_address from, smd_init_segment& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    
    if(is_me(from)){
        logger.debug("[{}], is_me do nothing ...", __func__);
        return make_ready_future<>();
    }else{
        unsigned cpu_id = data.cpu_id;
        sstring volume_id = data.volume_id;
        sstring segment_id = data.segment_id;
        uint64_t max_size = data.size;
        //auto shard = get_local_journal_service().shard_of(segment_id);
        logger.debug("journal_proxy::init_segment_locally ..... cpu_id:{}, volume_id:{}, segment_id:{}, size:{}"
            , cpu_id, volume_id, segment_id, max_size);
        return hive::get_journal_service().invoke_on(cpu_id, [volume_id, segment_id, max_size](journal_service& journal_service){
            auto secondary_journal_ptr = journal_service.get_or_create_secondary_journal(volume_id);
            return secondary_journal_ptr->create_segment(volume_id, segment_id, max_size);

        });
    }

}

//for sync segment
future<> journal_proxy::sync_segment(smd_sync_segment data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy::sync_segment targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    logger.debug("[{}] start, data:{}, targets.size:{}", __func__, data, targets.size());
  
    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, create_sync_segment_response_handler(std::move(data), targets));
    
    auto timeout_in_ms = get_local_hive_service().get_hive_config()->sync_commitlog_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    auto response_id = response_h.id;
    auto f = response_wait(response_id, timeout);
    sync_segment_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy::create_sync_segment_response_handler(
        smd_sync_segment sync_data,
        std::unordered_set<gms::inet_address> targets) {
    union_md data;
    data.sync_segment = std::make_shared<smd_sync_segment>(std::move(sync_data));
    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void journal_proxy::sync_segment_to_live_endpoints(
    response_id_type response_id, 
    clock_type::time_point timeout)
{
    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto& sync_data = handler.get_data();

    auto my_address = utils::fb_utilities::get_broadcast_address();

    auto lmutate = [this, &sync_data, response_id, my_address] {
        logger.debug("[sync_segment_to_live_endpoints] in lmutate, volume_id:{}", sync_data.sync_segment->volume_id);
        return sync_segment_locally(my_address, *(sync_data.sync_segment)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &sync_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[sync_segment_to_live_endpoints] in rmutate, volume_id:{}, forward.size():{}"
            , sync_data.sync_segment->volume_id, forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_sync_segment_ex(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            *(sync_data.sync_segment), 
            std::move(forward), 
            my_address, 
            engine().cpu_id(), 
            response_id);
    };

    std::vector<std::vector<gms::inet_address>> all;
    std::vector<gms::inet_address> local;
    std::vector<gms::inet_address> remote;

    for(auto dest : get_response_handler(response_id).get_targets()) {
        is_me(dest) ? local.push_back(dest) : remote.push_back(dest);
    }
    if(local.size() > 0){
        all.push_back(std::move(local)); 
    }
    if(remote.size() > 0){
        all.push_back(std::move(remote));
    }

    for(auto forward : all) {
        auto coordinator = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (coordinator == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, coordinator, std::move(forward));
        }

        f.handle_exception([coordinator] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during sync segment to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy::sync_segment_locally(gms::inet_address from, smd_sync_segment& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    if(is_me(from)){
        logger.debug("[{}] start, is_me do nothing ...", __func__);
        return make_ready_future<>();
    }else{
        unsigned cpu_id = data.cpu_id;
        sstring segment_id = data.segment_id;
        uint64_t offset_in_segment = data.offset_in_segment;
        sstring volume_id = data.volume_id;
        uint64_t offset_in_volume = data.offset_in_volume;
        sstring extent_id = data.extent_id;
        uint64_t offset_in_extent = data.offset_in_extent;
        uint64_t data_length = data.data_length;
        uint64_t serialize_length = data.serialize_length;
        uint64_t vclock = data.vclock;
        logger.debug("cpu_id:{}, volume_id:{}, segment_id:{}, extent_id:{} , offset:{}, data_length:{}, data_size:{}"
            , cpu_id, volume_id, segment_id, extent_id, offset_in_segment, data_length, data.content.size());
        bytes&& content  = std::move(data.content);
        //auto shard = get_local_journal_service().shard_of(segment_id);
        return hive::get_journal_service().invoke_on(cpu_id, [segment_id, offset_in_segment, volume_id, offset_in_volume, extent_id
                , offset_in_extent, data_length, serialize_length, vclock, content = std::move(content)](journal_service& journal_service) mutable{
            //chenjl for temp test
            try{
               auto secondary_journal_ptr = journal_service.get_secondary_journal(volume_id);
               return secondary_journal_ptr->sync_segment(
                   segment_id, 
                   offset_in_segment,
                   offset_in_volume, 
                   extent_id, 
                   offset_in_extent,
                   data_length,
                   serialize_length,
                   vclock, 
                   std::move(content));
            }catch (...){
               logger.error("chenjl for temp test, exception during sync segment to volume_id{}: {}", volume_id, std::current_exception());
               return make_ready_future<>();
            }
        });
    }
}

//for discard segment
future<> journal_proxy::discard_segment(smd_discard_segment data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy::discard_segment targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }
    logger.debug("[{}] start, data:{}", __func__, data);

    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, this->create_discard_segment_response_handler(std::move(data), targets));

    auto timeout_in_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    
    auto response_id = response_h.id;
    auto f = this->response_wait(response_id, timeout);
    this->discard_segment_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=this->shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy::create_discard_segment_response_handler(
    smd_discard_segment discard_data
    , std::unordered_set<gms::inet_address> targets) {
    union_md data;
    data.discard_segment = std::make_shared<smd_discard_segment>(std::move(discard_data));
    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void journal_proxy::discard_segment_to_live_endpoints(
    response_id_type response_id
    , clock_type::time_point timeout) {

    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto& discard_data = handler.get_data();
    
    auto my_address = utils::fb_utilities::get_broadcast_address();

    auto lmutate = [this, &discard_data, response_id, my_address] {
        logger.debug("[discard_segment_to_live_endpoints] in lmutate, response_id:{}", response_id);
        return discard_secondary_segment_locally(my_address, *(discard_data.discard_segment)).then(
                [this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &discard_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[discard_segment_to_live_endpoints] in rmutate, response_id:{}, forward.size():{}"
            , response_id, forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_discard_segment_ex(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            *(discard_data.discard_segment), 
            std::move(forward), 
            my_address, 
            engine().cpu_id(), 
            response_id);
    };

    std::vector<std::vector<gms::inet_address>> all;
    std::vector<gms::inet_address> local;
    std::vector<gms::inet_address> remote;
    for(auto dest: get_response_handler(response_id).get_targets()) {
        is_me(dest) ? local.push_back(dest) : remote.push_back(dest);
    }
    if(local.size() > 0){
        all.push_back(std::move(local)); 
    }
    if(remote.size() > 0){
        all.push_back(std::move(remote)); 
    }

    for (auto forwards : all) {
        auto coordinator = forwards.back();
        forwards.pop_back();
        
        future<> f = make_ready_future<>();
        if (coordinator == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, coordinator, std::move(forwards));
        }

        f.handle_exception([coordinator] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during discard segment to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy::discard_secondary_segment_locally(gms::inet_address from, smd_discard_segment& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    
    if(is_me(from)){
        logger.debug("[{}] start, data:{}", __func__, data);
        return make_ready_future<>();
    }else{
        logger.debug("discard_secondary_segment_locally volume_id:{}, segment_id:{} ", data.volume_id, data.segment_id);
        unsigned cpu_id = data.cpu_id;
        sstring volume_id = data.volume_id;
        sstring segment_id = data.segment_id;
        //auto shard = get_local_journal_service().shard_of(segment_id);
        return hive::get_journal_service().invoke_on(cpu_id, [volume_id, segment_id](journal_service& journal_service) mutable{
            //chenjl add for test
            try{
                auto secondary_journal_ptr = journal_service.get_secondary_journal(volume_id);
                return secondary_journal_ptr->discard_segment(segment_id);
            }catch (...){
                logger.error("chenjl for temp test, exception during discard segment to volume_id{}: {}", volume_id, std::current_exception());
                return make_ready_future<>();
            }
        });
    }
}


future<> journal_proxy::stop() {
    return make_ready_future<>();
}

//for get segment data
future<rmd_get_journal_data>
journal_proxy::get_journal_data(smd_get_journal_data request_data, gms::inet_address target) {
    if(is_me(target)) {
        return get_journal_data_locally(std::move(request_data)).then([](auto data_ptr){
            return make_ready_future<rmd_get_journal_data>(std::move(*data_ptr)); 
        }); 
    } else {
        return do_with(std::move(request_data), [target](auto& request_data)mutable{
            auto& ms = hive::get_local_messaging_service();
            return ms.send_get_journal_data(
                hive::messaging_service::msg_addr{target, 0}
                , request_data
            );
        });
    }
}

future<std::map<sstring, std::vector<revision_data>>>
journal_proxy::do_get_journal_data(
    sstring volume_id
    , sstring segment_id
    , uint64_t shard
    , std::map<sstring, std::vector<extent_group_revision>> revisions){

    return hive::get_journal_service().invoke_on(shard, [volume_id, segment_id, revisions=std::move(revisions)]
            (auto& shard_journal_service)mutable{
        return shard_journal_service.get_journal_data(volume_id, segment_id, std::move(revisions));
    });
}

future<foreign_ptr<lw_shared_ptr<rmd_get_journal_data>>>
journal_proxy::get_journal_data_locally(smd_get_journal_data smd_data) {
    logger.debug("[{}] start, smd_data:{}", __func__, smd_data);
    return seastar::async([this, smd_data = std::move(smd_data)](){
        auto volume_id  = smd_data.volume_id;
        auto segment_id = smd_data.segment_id;
        auto shard      = smd_data.shard;
        auto revisions  = smd_data.revisions;

        auto&& revision_datas = do_get_journal_data(volume_id, segment_id, shard, std::move(revisions)).get0();
        return make_foreign(make_lw_shared<rmd_get_journal_data>(
            volume_id,
            segment_id,
            std::move(revision_datas))
        );
    });
}

} //namespace hive
