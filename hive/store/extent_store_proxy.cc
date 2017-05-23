#include "extent_store_proxy.hh"
#include "db/consistency_level.hh"
#include "unimplemented.hh"
#include "frozen_mutation.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/exceptions/exceptions.hh"
#include "core/do_with.hh"
#include "hive/messaging_service.hh"
#include "gms/inet_address.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "core/future-util.hh"
#include "db/config.hh"
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
#include "types.hh"

#include "hive/extent_store.hh"
#include "hive/file_store.hh"
#include "hive/hive_config.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "hive/context/context_service.hh"
#include "hive/extent_revision_set.hh"
#include "hive/migration_manager.hh"

#include "hive/message_data_type.hh"
#include "hive/hive_service.hh"

namespace hive{

static logging::logger logger("extent_store_proxy");

distributed<extent_store_proxy> _the_extent_store_proxy;
using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

static inline sstring get_local_ip() {
    return utils::fb_utilities::get_broadcast_address().to_sstring();
}

static inline sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(local_addr);
}

//for inner struct define
extent_store_proxy::rh_entry::rh_entry(
    std::unique_ptr<common_response_handler>&& h,
    std::function<void()>&& cb)
    : handler(std::move(h))
    , expire_timer(std::move(cb))
{}

extent_store_proxy::unique_response_handler::unique_response_handler(
    extent_store_proxy& proxy,
    uint64_t response_id)
    : id(response_id)
    , p(proxy)
{}

extent_store_proxy::unique_response_handler::unique_response_handler(
    unique_response_handler&& x)
    : id(x.id)
    , p(x.p)
{
    x.id = 0;
};

extent_store_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}

uint64_t extent_store_proxy::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

//for extent_store_proxy define
extent_store_proxy::extent_store_proxy(
    const db::config config,
    distributed<extent_store>& extent_store)
    : _config(std::make_unique<db::config>(config))
    , _extent_store(extent_store) {

    logger.debug("constructor start");
    init_messaging_service();

    auto periodic = hive_config::wash_monad_queue_periodic;
    _timer.set_callback(std::bind(&extent_store_proxy::on_timer, this));
    _timer.arm(lowres_clock::now()+std::chrono::seconds(3)
        , std::experimental::optional<lowres_clock::duration> {std::chrono::seconds(periodic)}
    );
}

extent_store_proxy::~extent_store_proxy() {
    uninit_messaging_service();
}

void extent_store_proxy::init_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    //create extent group
    ms.register_create_extent_group_ex([] (const rpc::client_info& cinfo
                                      , smd_create_extent_group smd_data
                                      , std::vector<gms::inet_address> forward
                                      , gms::inet_address reply_to
                                      , unsigned shard
                                      , uint64_t response_id) {
        logger.debug("receive CREATE_EXTENT_GROUP message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(smd_data), get_local_shared_extent_store_proxy(),
                [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
                (smd_create_extent_group& smd_data, shared_ptr<extent_store_proxy>& proxy) mutable {
            ++proxy->_stats.received_creates;
            proxy->_stats.forwarded_creates += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &smd_data] {
                    return proxy->create_extent_group_locally(smd_data);
                }).then([reply_to, shard, response_id] {
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send CREATE_EXTENT_GROUP_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_create_extent_group_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                            .then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                    //print warn log with error level
                    logger.error("warn, failed to create extent group from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
                }),
                parallel_for_each(forward.begin(), forward.end(),
                    [reply_to, shard, response_id, &smd_data, &proxy]
                    (gms::inet_address forward)mutable {
                    auto& ms = hive::get_local_messaging_service();
                    auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                    auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                    logger.debug("forward a create extent group to {}, response_id:{}", forward, response_id);
                    return ms.send_create_extent_group_ex(hive::messaging_service::msg_addr{forward, 0}
                                                        , timeout
                                                        , smd_data
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
                // ignore ressult, since we'll be returning them via MUTATION_DONE verbs
                return hive::messaging_service::no_wait();
            });
        });
    });

    ms.register_create_extent_group_done_ex([] (const rpc::client_info& cinfo
                                              , unsigned shard
                                              , uint64_t response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive CREATE_EXTENT_GROUP_DONE from {}, response_id:{}", from, response_id);
        return get_extent_store_proxy().invoke_on(shard, [from, response_id] (auto& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });

    });

    //delete extent group
    ms.register_delete_extent_group_ex([] (const rpc::client_info& cinfo
                                         , smd_delete_extent_group smd_data
                                         , std::vector<gms::inet_address> forward
                                         , gms::inet_address reply_to
                                         , unsigned shard
                                         , uint64_t response_id) {
        logger.debug("receive DELETE_EXTENT_GROUP message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(smd_data), get_local_shared_extent_store_proxy(),
                [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
                (smd_delete_extent_group& smd_data, shared_ptr<extent_store_proxy>& proxy) mutable {
            ++proxy->_stats.received_deletes;
            proxy->_stats.forwarded_deletes += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &smd_data] {
                    return proxy->delete_extent_group_locally(smd_data);
                }).then([reply_to, shard, response_id] {
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send DELETE_EXTENT_GROUP_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_delete_extent_group_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                            .then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                    //print warn log with error level
                    logger.error("warn, failed to delete extent group from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
                }),
                parallel_for_each(forward.begin(), forward.end(),
                    [reply_to, shard, response_id, &smd_data, &proxy]
                    (gms::inet_address forward)mutable {
                    auto& ms = hive::get_local_messaging_service();
                    auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                    auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                    logger.debug("forward a delete extent group to {}, response_id:{}", forward, response_id);
                    return ms.send_delete_extent_group_ex(hive::messaging_service::msg_addr{forward, 0}
                                                        , timeout
                                                        , smd_data
                                                        , {}
                                                        , reply_to
                                                        , shard
                                                        , response_id).then_wrapped([&proxy] (future<> f) {
                        if(f.failed()) {
                            ++proxy->_stats.forwarding_delete_errors;
                        }
                        f.ignore_ready_future();
                   });
                })
            ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore ressult, since we'll be returning them via MUTATION_DONE verbs
                return hive::messaging_service::no_wait();
            });
        });
    });

    ms.register_delete_extent_group_done_ex([] (const rpc::client_info& cinfo
                                              , unsigned shard
                                              , uint64_t response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive DELETE_EXTENT_GROUP_DONE from {}, response_id:{}", from, response_id);
        return get_extent_store_proxy().invoke_on(shard, [from, response_id] (auto& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //rwrite extent group
    ms.register_rwrite_extent_group_ex([] (const rpc::client_info& cinfo
                                         , smd_rwrite_extent_group smd_data
                                         , std::vector<gms::inet_address> forward
                                         , gms::inet_address reply_to
                                         , unsigned shard
                                         , uint64_t response_id) {
        logger.debug("receive RWRITE_EXTENT_GROUP message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(smd_data), get_local_shared_extent_store_proxy(),
                [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
                (smd_rwrite_extent_group& smd_data, shared_ptr<extent_store_proxy>& proxy) mutable {
            ++proxy->_stats.received_rwrites;
            proxy->_stats.forwarded_rwrites += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &smd_data] {
                    return proxy->rwrite_extent_group_locally(smd_data);
                }).then([reply_to, shard, response_id] {
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send RWRITE_EXTENT_GROUP_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_rwrite_extent_group_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                            .then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                    //print warn log with error level
                    logger.error("warn, failed to rwrite extent group from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
                }),
                parallel_for_each(forward.begin(), forward.end(),
                    [reply_to, shard, response_id, &smd_data, &proxy]
                    (gms::inet_address forward)mutable {
                    auto& ms = hive::get_local_messaging_service();
                    auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                    auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                    logger.debug("forward a rwrite extent group to {}, response_id:{}", forward, response_id);
                    return ms.send_rwrite_extent_group_ex(hive::messaging_service::msg_addr{forward, 0}
                                                        , timeout
                                                        , smd_data
                                                        , {}
                                                        , reply_to
                                                        , shard
                                                        , response_id).then_wrapped([&proxy] (future<> f) {
                        if(f.failed()) {
                            ++proxy->_stats.forwarding_rwrite_errors;
                        }
                        f.ignore_ready_future();
                   });
                })
            ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore ressult, since we'll be returning them via MUTATION_DONE verbs
                return hive::messaging_service::no_wait();
            });
        });
    });

    ms.register_rwrite_extent_group_done_ex([] (const rpc::client_info& cinfo
                                              , unsigned shard
                                              , uint64_t response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive RWRITE_EXTENT_GROUP_DONE from {}, response_id:{}", from, response_id);
        return get_extent_store_proxy().invoke_on(shard, [from, response_id] (auto& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //migrate extent group
    ms.register_migrate_extent_group_ex([] (const rpc::client_info& cinfo
                                         , smd_migrate_extent_group smd_data
                                         , std::vector<gms::inet_address> forward
                                         , gms::inet_address reply_to
                                         , unsigned shard
                                         , uint64_t response_id) {
        logger.debug("receive MIGRATE_EXTENT_GROUP message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(smd_data), get_local_shared_extent_store_proxy(),
                [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
                (smd_migrate_extent_group& smd_data, shared_ptr<extent_store_proxy>& proxy) mutable {
            ++proxy->_stats.received_migrates;
            proxy->_stats.forwarded_migrates += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &smd_data] {
                    return proxy->migrate_extent_group_locally(smd_data);
                }).then([reply_to, shard, response_id] {
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send MIGRATE_EXTENT_GROUP_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_migrate_extent_group_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                            .then_wrapped([] (future<> f) {
                        f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                    //print warn log with error level
                    logger.error("warn, failed to migrate extent group from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
                }),
                parallel_for_each(forward.begin(), forward.end(),
                    [reply_to, shard, response_id, &smd_data, &proxy]
                    (gms::inet_address forward)mutable {
                    auto& ms = hive::get_local_messaging_service();
                    auto config_timeout_ms = get_local_hive_service().get_hive_config()->migrate_extent_group_timeout_in_ms();
                    auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                    logger.debug("forward a migrate extent group to {}, response_id:{}", forward, response_id);
                    return ms.send_migrate_extent_group_ex(hive::messaging_service::msg_addr{forward, 0}
                                                        , timeout
                                                        , smd_data
                                                        , {}
                                                        , reply_to
                                                        , shard
                                                        , response_id).then_wrapped([&proxy] (future<> f) {
                        if(f.failed()) {
                            ++proxy->_stats.forwarding_migrate_errors;
                        }
                        f.ignore_ready_future();
                   });
                })
            ).then_wrapped([] (future<std::tuple<future<>, future<>>>&& f) {
                // ignore ressult, since we'll be returning them via MUTATION_DONE verbs
                return hive::messaging_service::no_wait();
            });
        });
    });

    ms.register_migrate_extent_group_done_ex([] (const rpc::client_info& cinfo
                                              , unsigned shard
                                              , uint64_t response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive MIGRATE_EXTENT_GROUP_DONE from {}, response_id:{}", from, response_id);
        return get_extent_store_proxy().invoke_on(shard, [from, response_id] (auto& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //read extent group
    ms.register_read_extent_group_ex([](const rpc::client_info& cinfo, smd_read_extent_group smd_data) {
        //auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        return do_with(get_local_shared_extent_store_proxy(), [&cinfo, smd_data=std::move(smd_data)]
                (shared_ptr<extent_store_proxy>& proxy)mutable {
            return proxy->read_extent_group_locally(smd_data);
        });
        //.finally([from](){
        //    logger.debug("read extent group handling is done, sending a reesponse to {}", from);
        //});
    });

    //TODO:yellow need merge with read extent group
    //get extent group
    ms.register_get_extent_group([](const rpc::client_info& cinfo, smd_get_extent_group smd_data) {
        return do_with(get_local_shared_extent_store_proxy(), [&cinfo, smd_data=std::move(smd_data)]
                (shared_ptr<extent_store_proxy>& proxy)mutable {
            return proxy->get_extent_group_locally(smd_data);
        });
    });

}

void extent_store_proxy::uninit_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    ms.unregister_create_extent_group_ex();
    ms.unregister_create_extent_group_done_ex();

    ms.unregister_delete_extent_group_ex();
    ms.unregister_delete_extent_group_done_ex();

    ms.unregister_read_extent_group_ex();
    ms.unregister_get_extent_group();

    ms.unregister_rwrite_extent_group_ex();
    ms.unregister_rwrite_extent_group_done_ex();

    ms.unregister_migrate_extent_group();
    ms.unregister_migrate_extent_group_done();

}

future<> extent_store_proxy::stop() {
    return make_ready_future<>();
}

unsigned extent_store_proxy::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

future<disk_context_entry> extent_store_proxy::filter_local_disk_context(std::vector<sstring> disk_ids){
    return seastar::async([this, disk_ids]{
        auto& context_service = hive::get_local_context_service();
        auto local_ip = get_local_ip();

        for(auto disk_id : disk_ids) {
            auto disk_context = context_service.get_or_pull_disk_context(disk_id).get0();
            auto disk_ip = disk_context.get_ip();
            if(disk_ip == local_ip){
                return std::move(disk_context);
            }
        }

        auto error_info = sprint("failed to filter local disk context, local_ip:%s, disk_ids:%s"
            , local_ip, hive_tools::format(disk_ids));
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    });
}

future<disk_context_entry> extent_store_proxy::check_disk_belong_to_me(sstring disk_id) {
    logger.debug("[{}] start, disk_id:{}", __func__, disk_id);
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then([disk_id](auto disk_context){
        auto local_ip = get_local_ip();
        auto disk_ip = disk_context.get_ip();
        if(disk_ip == local_ip){
            return make_ready_future<disk_context_entry>(std::move(disk_context));
        }

        auto error_info = sprint("failed to check disk belong to me, disk_id:%s, disk_ip:%s, local_ip:%s"
            , disk_id, disk_ip, local_ip);
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    });
}

uint64_t extent_store_proxy::register_response_handler(std::unique_ptr<common_response_handler>&& h) {
    auto id = h->response_id();
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), [this, id] {
        auto& e = _response_handlers.find(id)->second;

        if (!e.handler->achieved()) {
            std::ostringstream out;
            out << "response handler timeout, handler_id:" << e.handler->response_id();
            out << ", not response targets: " << hive_tools::format(e.handler->get_targets());
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

common_response_handler& extent_store_proxy::get_response_handler(uint64_t response_id) {
    return *_response_handlers.find(response_id)->second.handler;
}

void extent_store_proxy::remove_response_handler(uint64_t response_id) {
    _response_handlers.erase(response_id);
}

void extent_store_proxy::got_response(uint64_t response_id, gms::inet_address from) {
    auto itor = _response_handlers.find(response_id);
    if (itor != _response_handlers.end()) {
        if (itor->second.handler->response(from)) {
            //last one, remove entry. Will cancel expiration timer too.
            remove_response_handler(response_id);
        }
    }
}

future<response_entry>
extent_store_proxy::response_wait(uint64_t response_id, clock_type::time_point timeout) {
    auto& e = _response_handlers.find(response_id)->second;
    e.expire_timer.arm(timeout);
    return e.handler->wait();
}

future<response_entry> extent_store_proxy::response_end(future<response_entry> result, utils::latency_counter lc) {
    assert(result.available());
    try {
        auto response = result.get();
        logger.debug("[{}] success latency:{} ns", __func__, lc.stop().latency_in_nano());
        return make_ready_future<response_entry>(response);
        //tododl:yellow add some detailed exception
    } catch(...) {
        return make_exception_future<response_entry>(std::current_exception());
    }
}

void extent_store_proxy::cancel_response_handler_timeout(uint64_t response_id){
    auto itor = _response_handlers.find(response_id);
    if(itor != _response_handlers.end()){
      auto& handler_entry = itor->second;
      handler_entry.expire_timer.cancel();
    }
}

hive_shared_mutex& extent_store_proxy::get_or_create_mutex(sstring extent_group_id){
    std::map<sstring, hive_shared_mutex>::iterator itor = _monad_queue.find(extent_group_id);
    if(itor != _monad_queue.end()){
        return itor->second;
    }else{
        auto ret = _monad_queue.insert(std::make_pair(extent_group_id, hive_shared_mutex()));
        if(true == ret.second){
            return ret.first->second;
        }else{
            throw std::runtime_error("error, insert map error[extent_store::get_or_create_mutex]");
        }
    }
}

size_t extent_store_proxy::get_monad_waiters_count(sstring extent_group_id){
    return get_or_create_mutex(extent_group_id).waiters_count();
}

void extent_store_proxy::on_timer(){
    wash_monad_queue();
}

void extent_store_proxy::wash_monad_queue(){
    logger.info("{} start, _monad_queue.size()={}", __func__, _monad_queue.size());
    std::map<sstring, hive_shared_mutex>::iterator itor;
    for(itor = _monad_queue.begin(); itor != _monad_queue.end(); itor++){
        if(itor->second.is_idle()){
            _monad_queue.erase(itor);
        }
    }
    logger.info("{} done, _monad_queue.size()={}", __func__, _monad_queue.size());
}

//start create extent group
future<response_entry> extent_store_proxy::create_extent_group(
    smd_create_extent_group create_data
    , std::unordered_set<gms::inet_address> targets) {
    logger.debug("[{}] start, create_data:{}, targets:{}", __func__, create_data, hive_tools::format(targets));

    auto extent_group_id = create_data.extent_group_id;
    return with_monad(extent_group_id, [this, create_data=std::move(create_data), targets]()mutable{
        utils::latency_counter lc;
        lc.start();
        unique_response_handler response_h(*this, build_create_extent_group_response_handler(std::move(create_data), targets));

        auto timeout_in_ms = get_local_hive_service().get_hive_config()->create_extent_group_timeout_in_ms();
        auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        auto response_id = response_h.id;
        auto f = response_wait(response_id, timeout);
        create_extent_group_on_all_nodes(response_h.release(), timeout);

        return std::move(f).then_wrapped([p=shared_from_this(), lc] (auto f) {
            return p->response_end(std::move(f), lc);
        });
    });
}

uint64_t extent_store_proxy::build_create_extent_group_response_handler(
    smd_create_extent_group create_data
    , std::unordered_set<gms::inet_address> targets) {

    //std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_create_extent_group>(std::move(create_data));
    union_md data;
    //data.create_extent_group = std::move(create_data); wztest
    data.create_extent_group = std::make_shared<smd_create_extent_group>(std::move(create_data));
    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets)
    );

    return register_response_handler(std::move(handler));
}

void extent_store_proxy::create_extent_group_on_all_nodes(
    uint64_t response_id
    , clock_type::time_point timeout){
    logger.debug("[{}] start, response_id:{}", __func__, response_id);

    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();
    //auto data_ptr = std::dynamic_pointer_cast<smd_create_extent_group>(handler.get_data());
    auto& data = handler.get_data();

    auto lmutate = [this, &data, response_id, my_address] {
        logger.debug("[create_extent_group_on_all_nodes] in lmutate, create_data:{}", *(data.create_extent_group));
        return create_extent_group_locally(*(data.create_extent_group)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &data, response_id, my_address, timeout] 
            (gms::inet_address target, std::vector<gms::inet_address>&& forward)mutable{
        logger.debug("[create_extent_group_on_all_nodes] in rmutate, data:{}, target:{}, forward:{}"
                    , *(data.create_extent_group), target, hive_tools::format(forward));
        auto& ms = hive::get_local_messaging_service();
        return ms.send_create_extent_group_ex(
            hive::messaging_service::msg_addr{target, 0},
            timeout,
            *(data.create_extent_group),
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
        auto target = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (target == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, target, std::move(forward));
        }

        f.handle_exception([target] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during create extent group to {}: {}"
                    , target, std::current_exception());
            }
        });
    }
}

future<> extent_store_proxy::create_extent_group_locally(smd_create_extent_group& create_data) {
    logger.debug("[{}] start, create_data:{}", __func__, create_data);
    auto extent_group_id = create_data.extent_group_id;
    auto disk_ids = create_data.disk_ids;
    return filter_local_disk_context(disk_ids).then([this, extent_group_id](auto disk_context){
        auto disk_path = disk_context.get_mount_path();
        auto shard = _extent_store.local().shard_of(extent_group_id);
        return _extent_store.invoke_on(shard, [extent_group_id, disk_path](auto& extent_store){
             return extent_store.create_extent_group_ex(extent_group_id, disk_path);
        });
    });
}
//end create extent group

//start delete extent group
future<response_entry> extent_store_proxy::delete_extent_group(
    smd_delete_extent_group delete_data
    , std::unordered_set<gms::inet_address> targets) {
    logger.debug("[{}] start, delete_data:{}, targets:{}", __func__, delete_data, hive_tools::format(targets));

    auto extent_group_id = delete_data.extent_group_id;
    return with_monad(extent_group_id, [this, delete_data=std::move(delete_data), targets]()mutable{
        utils::latency_counter lc;
        lc.start();

        unique_response_handler response_h(*this, build_delete_extent_group_response_handler(std::move(delete_data), targets));

        auto timeout_in_ms = get_local_hive_service().get_hive_config()->delete_extent_group_timeout_in_ms();
        auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        auto response_id = response_h.id;
        auto f = response_wait(response_id, timeout);
        delete_extent_group_on_all_nodes(response_h.release(), timeout);

        return std::move(f).then_wrapped([p=shared_from_this(), lc] (auto f) {
            return p->response_end(std::move(f), lc);
        });
    });
}

uint64_t extent_store_proxy::build_delete_extent_group_response_handler(
    smd_delete_extent_group delete_data
    , std::unordered_set<gms::inet_address> targets) {

    //std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_delete_extent_group>(std::move(delete_data));
    union_md data;
    //data.delete_extent_group = std::move(delete_data); wztest
    data.delete_extent_group = std::make_shared<smd_delete_extent_group>(std::move(delete_data));

    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void extent_store_proxy::delete_extent_group_on_all_nodes(
    uint64_t response_id
    , clock_type::time_point timeout){
    logger.debug("[{}] start, response_id:{}", __func__, response_id);

    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();
    //auto data_ptr = std::dynamic_pointer_cast<smd_delete_extent_group>(handler.get_data());
    auto& data = handler.get_data();

    auto lmutate = [this, &data, response_id, my_address] {
        logger.debug("[delete_extent_group_on_all_nodes] in lmutate, delete_data:{}", *(data.delete_extent_group));
        return delete_extent_group_locally(*(data.delete_extent_group)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &data, response_id, my_address, timeout]
            (gms::inet_address target, std::vector<gms::inet_address>&& forward)mutable{
        logger.debug("[delete_extent_group_on_all_nodes] in rmutate, data:{}, target:{}, forward:{}"
                    , *(data.delete_extent_group), target, hive_tools::format(forward));
        auto& ms = hive::get_local_messaging_service();
        return ms.send_delete_extent_group_ex(
            hive::messaging_service::msg_addr{target, 0},
            timeout,
            *(data.delete_extent_group),
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
        auto target = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (target == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, target, std::move(forward));
        }

        f.handle_exception([target] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during delete extent group to {}: {}"
                    , target, std::current_exception());
            }
        });
    }
}

future<> extent_store_proxy::delete_extent_group_locally(smd_delete_extent_group& delete_data) {
    logger.debug("[{}] start, delete_data:{}", __func__, delete_data);
    auto extent_group_id = delete_data.extent_group_id;
    auto disk_ids = delete_data.disk_ids;
    return filter_local_disk_context(disk_ids).then([this, extent_group_id](auto disk_context){
        auto disk_path = disk_context.get_mount_path();
        auto shard = _extent_store.local().shard_of(extent_group_id);
        return _extent_store.invoke_on(shard, [extent_group_id, disk_path](auto& extent_store){
             return extent_store.delete_extent_group(extent_group_id, disk_path);
        });
    });
}
//end delete extent group

//start rwrite extent group
future<response_entry> extent_store_proxy::rwrite_extent_group(
    smd_rwrite_extent_group rwrite_data
    , std::unordered_set<gms::inet_address> targets) {
    logger.debug("[{}] start, rwrite_data:{}, targets:{}", __func__, rwrite_data, hive_tools::format(targets));

    auto extent_group_id = rwrite_data.extent_group_id;
    return with_monad(extent_group_id, [this, rwrite_data=std::move(rwrite_data), targets]()mutable{
        utils::latency_counter lc;
        lc.start();

        unique_response_handler response_h(*this, build_rwrite_extent_group_response_handler(std::move(rwrite_data), targets));

        auto timeout_in_ms = get_local_hive_service().get_hive_config()->rwrite_extent_group_timeout_in_ms();
        auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        auto response_id = response_h.id;
        auto f = response_wait(response_id, timeout);
        rwrite_extent_group_on_all_nodes(response_h.release(), timeout);

        return std::move(f).then_wrapped([p=shared_from_this(), lc] (auto f) {
            return p->response_end(std::move(f), lc);
        });
    });
}

uint64_t extent_store_proxy::build_rwrite_extent_group_response_handler(
    smd_rwrite_extent_group rwrite_data
    , std::unordered_set<gms::inet_address> targets) {

    //std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_rwrite_extent_group>(std::move(rwrite_data));
    union_md data;
    //data.rwrite_extent_group = std::move(rwrite_data); //wztest
    data.rwrite_extent_group = std::make_shared<smd_rwrite_extent_group>(std::move(rwrite_data));

    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void extent_store_proxy::rwrite_extent_group_on_all_nodes(
    uint64_t response_id
    , clock_type::time_point timeout){
    logger.debug("[{}] start, response_id:{}", __func__, response_id);

    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();
    //auto data_ptr = std::dynamic_pointer_cast<smd_rwrite_extent_group>(handler.get_data());
    auto& data = handler.get_data();

    auto lmutate = [this, &data, response_id, my_address] {
        logger.debug("[rwrite_extent_group_on_all_nodes] in lmutate, rwrite_data:{}", *(data.rwrite_extent_group));
        return rwrite_extent_group_locally(*(data.rwrite_extent_group)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &data, response_id, my_address, timeout]
            (gms::inet_address target, std::vector<gms::inet_address>&& forward)mutable{
        logger.debug("[rwrite_extent_group_on_all_nodes] in rmutate, data:{}, target:{}, forward:{}"
                    , *(data.rwrite_extent_group), target, hive_tools::format(forward));

//assert((*data_ptr).extent_group_id != "");

        auto& ms = hive::get_local_messaging_service();
        return ms.send_rwrite_extent_group_ex(
            hive::messaging_service::msg_addr{target, 0},
            timeout,
            *(data.rwrite_extent_group),
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
        auto target = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (target == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, target, std::move(forward));
        }

        f.handle_exception([target] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during rwrite extent group to {}: {}"
                    , target, std::current_exception());
            }
        });
    }
}

future<> extent_store_proxy::rwrite_extent_group_locally(smd_rwrite_extent_group& rwrite_data) {
    logger.debug("[{}] start, rwrite_data:{}", __func__, rwrite_data);
    auto disk_ids = rwrite_data.disk_ids;
    return filter_local_disk_context(disk_ids).then([this, &rwrite_data]
            (auto disk_context)mutable{
        auto extent_group_id = rwrite_data.extent_group_id;
        auto disk_path = disk_context.get_mount_path();

        auto shard = _extent_store.local().shard_of(extent_group_id);
        return _extent_store.invoke_on(shard, [extent_group_id, disk_path, &rwrite_data]
                (auto& extent_store)mutable{
            return extent_store.rwrite_extent_group(extent_group_id, disk_path, rwrite_data.revisions);
        });
    });
}
//end rwrite extent group


//start read extent group
future<gms::inet_address> extent_store_proxy::get_addr_by_disk_id(sstring disk_id){
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then([](auto disk_context){
        auto addr = gms::inet_address(disk_context.get_ip());
        return make_ready_future<gms::inet_address>(addr);
    });
}

future<foreign_ptr<lw_shared_ptr<rmd_read_extent_group>>>
extent_store_proxy::read_extent_group(hive_read_subcommand read_subcmd, bool read_object){
    logger.debug("[{}] start, read_subcmd:{}", __func__, read_subcmd);

//tododl:red 'read_object' not use now
//tododl:red not use monad

    return seastar::async([this,read_subcmd = std::move(read_subcmd)]() mutable {
        //1. sort disk id with the local be first
        sstring local_disk_id;
        auto disk_ids = hive_tools::split_to_vector(read_subcmd.disk_ids, ":");

        std::vector<sstring> sort_disk_ids;
        for(auto disk_id: disk_ids){
            auto addr = get_addr_by_disk_id(disk_id).get0();
            if(is_me(addr)){
                local_disk_id = disk_id;
            }else{
                sort_disk_ids.push_back(disk_id);
            }
        }
        if(local_disk_id != ""){
            sort_disk_ids.insert(sort_disk_ids.begin(), local_disk_id);
        }

        //2. query by order
        auto& ms = hive::get_local_messaging_service();
        std::__exception_ptr::exception_ptr last_exception;
        for(auto disk_id : sort_disk_ids){
            smd_read_extent_group smd_data(
                read_subcmd.owner_id,
                read_subcmd.extent_group_id,
                read_subcmd.extent_id,
                read_subcmd.extent_offset_in_group,
                read_subcmd.data_offset_in_extent,
                read_subcmd.length,
                disk_id
            );
            auto disk_ip = get_addr_by_disk_id(disk_id).get0();
            try {
                auto target = hive::messaging_service::msg_addr{disk_ip, 0}; //tododl:yellow why always 0??
                auto rmd_data = ms.send_read_extent_group_ex(target, smd_data).get0();
                hit_rate(disk_id);
                return make_foreign(make_lw_shared<rmd_read_extent_group>(std::move(rmd_data)));
            } catch (...) {
                last_exception = std::current_exception();
                continue;
            }
        }

        //3. query failed from all disk_ids
        std::ostringstream out;
        out << "[read_extent_group] failed, read_subcmd:" << read_subcmd;
        out << ", exception:" << last_exception;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    });
}

future<foreign_ptr<lw_shared_ptr<rmd_read_extent_group>>>
extent_store_proxy::read_extent_group_locally(smd_read_extent_group read_params) {
    logger.debug("[{}] start, read_params:{}", __func__, read_params);
    auto disk_id = read_params.disk_id;
    return check_disk_belong_to_me(disk_id)
            .then([this, read_params=std::move(read_params)](auto disk_context)mutable{
        auto disk_path = disk_context.get_mount_path();

        unsigned shard = _extent_store.local().shard_of(read_params.extent_group_id);
        return _extent_store.invoke_on(shard, [read_params, disk_path] (auto& extent_store) {
            auto offset = read_params.extent_offset_in_group + read_params.data_offset_in_extent;
            return extent_store.read_extent_group(read_params.extent_group_id
                                                , offset
                                                , read_params.length
                                                , disk_path).then([read_params](auto&& data) {

                if(read_params.only_need_digest){
                    sstring md5 = hive_tools::calculate_md5(data);
                    rmd_read_extent_group rmd_data(
                        read_params.owner_id
                        , read_params.extent_group_id
                        , read_params.extent_id
                        , read_params.extent_offset_in_group
                        , read_params.data_offset_in_extent
                        , read_params.length
                        , read_params.disk_id
                        , {}
                        , md5);
                    return make_foreign(make_lw_shared<rmd_read_extent_group>(std::move(rmd_data)));
                }else{
                    rmd_read_extent_group rmd_data(
                        read_params.owner_id
                        , read_params.extent_group_id
                        , read_params.extent_id
                        , read_params.extent_offset_in_group
                        , read_params.data_offset_in_extent
                        , read_params.length
                        , read_params.disk_id
                        , std::move(data));
                    return make_foreign(make_lw_shared<rmd_read_extent_group>(std::move(rmd_data)));
                }
            });
        });
    });
}
//end read extent group

//start migrate extent group
future<response_entry> extent_store_proxy::migrate_extent_group(
    smd_migrate_extent_group migrate_data
    , std::unordered_set<gms::inet_address> targets) {
    logger.debug("[{}] start, migrate_data:{}, targets:{}", __func__, migrate_data, hive_tools::format(targets));

    auto extent_group_id = migrate_data.extent_group_id;
    return with_monad(extent_group_id, [this, migrate_data, targets](){
        utils::latency_counter lc;
        lc.start();

        unique_response_handler response_h(*this, build_migrate_extent_group_response_handler(std::move(migrate_data), targets));
        auto timeout_in_ms = get_local_hive_service().get_hive_config()->migrate_extent_group_timeout_in_ms();
        auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
        auto response_id = response_h.id;
        auto f = response_wait(response_id, timeout);
        migrate_extent_group_on_all_nodes(response_h.release(), timeout);

        return std::move(f).then_wrapped([p=shared_from_this(), lc] (auto f) {
            return p->response_end(std::move(f), lc);
        });
    });
}

uint64_t extent_store_proxy::build_migrate_extent_group_response_handler(
    smd_migrate_extent_group migrate_data
    , std::unordered_set<gms::inet_address> targets) {

    //std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_migrate_extent_group>(std::move(migrate_data));
    union_md data;
    //data.migrate_extent_group = std::move(migrate_data); //wztest
    data.migrate_extent_group = std::make_shared<smd_migrate_extent_group>(std::move(migrate_data));
    auto handler = std::make_unique<common_response_handler>(
        _next_response_id++,
        std::move(data),
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void extent_store_proxy::migrate_extent_group_on_all_nodes(
    uint64_t response_id
    , clock_type::time_point timeout){
    logger.debug("[{}] start, response_id:{}", __func__, response_id);

    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();
    //auto data_ptr = std::dynamic_pointer_cast<smd_migrate_extent_group>(handler.get_data());
    auto& data = handler.get_data();

    auto lmutate = [this, &data, response_id, my_address] {
        logger.debug("[migrate_extent_group_on_all_nodes] in lmutate, migrate_data:{}", *(data.migrate_extent_group));
        return migrate_extent_group_locally(*(data.migrate_extent_group)).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &data, response_id, my_address, timeout]
            (gms::inet_address target, std::vector<gms::inet_address>&& forward)mutable{
        logger.debug("[migrate_extent_group_on_all_nodes] in rmutate, data:{}, target:{}, forward:{}"
                    , *(data.migrate_extent_group), target, hive_tools::format(forward));
        auto& ms = hive::get_local_messaging_service();
        return ms.send_migrate_extent_group_ex(
            hive::messaging_service::msg_addr{target, 0},
            timeout,
            *(data.migrate_extent_group),
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
        auto target = forward.back();
        forward.pop_back();

        future<> f = make_ready_future<>();
        if (target == my_address) {
            f = futurize<void>::apply(lmutate);
        } else {
            f = futurize<void>::apply(rmutate, target, std::move(forward));
        }

        f.handle_exception([target] (std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch(rpc::closed_error&) {
                // ignore, disconnect will be logged by gossiper
            } catch(seastar::gate_closed_exception&) {
                // may happen during shutdown, ignore it
            } catch(...) {
                logger.error("exception during migrate extent group to {}: {}"
                    , target, std::current_exception());
            }
        });
    }
}

future<> extent_store_proxy::migrate_extent_group_locally(smd_migrate_extent_group& migrate_data) {
    logger.debug("[{}] start, migrate_data:{}", __func__, migrate_data);
    check_disk_belong_to_me(migrate_data.src_disk_id);

    migrate_params_entry migrate_params(
        migrate_data.driver_node_ip
        , migrate_data.container_name
        , migrate_data.intent_id
        , migrate_data.owner_id
        , migrate_data.extent_group_id
        , migrate_data.src_disk_id
        , migrate_data.dst_disk_id
        , migrate_data.offset
        , migrate_data.length
    );

    auto& local_migration_manager = hive::get_local_migration_manager();
    return local_migration_manager.stream_extent_group(migrate_params);
}
//end migrate extent group

void extent_store_proxy::hit_rate(sstring hit_disk_id){
    auto& context_service = hive::get_local_context_service();
    context_service.get_or_pull_disk_context(hit_disk_id).then([this, hit_disk_id](auto disk_context){
        auto disk_type = disk_context.get_type();
        auto disk_ip = disk_context.get_ip();
        if (disk_type  == "SSD") {
            if (is_me(gms::inet_address(disk_ip))) {
                this -> _stats.local_ssd_hit_count++;
            } else {
                this -> _stats.remote_ssd_hit_count++;
            }
        } else if(disk_type == "HDD") {
            if (is_me(gms::inet_address(disk_ip))) {
                this -> _stats.local_hdd_hit_count++;
            } else {
                this -> _stats.remote_hdd_hit_count++;
            }
        } else {
            std::ostringstream out;
            out << "[hit_rate] error, hit_disk_id:" << hit_disk_id;
            out << ", exception: unsurport disk type:" << disk_type;
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
        this->_stats.read_requests++;
    });
}

//start get extent group
future<rmd_get_extent_group>
extent_store_proxy::get_extent_group(smd_get_extent_group request_data, gms::inet_address target){
    if(is_me(target)) {
        return get_extent_group_locally(std::move(request_data)).then([](auto data_ptr){
            return make_ready_future<rmd_get_extent_group>(std::move(*data_ptr));
        }); 
    }else {
        return do_with(std::move(request_data), [target](auto& request_data)mutable{
            auto& ms = hive::get_local_messaging_service();
            return ms.send_get_extent_group(
                hive::messaging_service::msg_addr{target, 0}
                , request_data
            );
        });
    }
}

future<sstring> 
extent_store_proxy::get_local_disk_mount_path(sstring volume_id, sstring extent_group_id){
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_extent_group_context(volume_id, extent_group_id).then(
            [&context_service, volume_id, extent_group_id](auto extent_group_context){
        auto disk_ids_str = extent_group_context.get_disk_ids();
        auto disk_ids_set = hive_tools::split_to_set(disk_ids_str, ":");

        return context_service.get_or_pull_disk_context(disk_ids_set).then(
                [volume_id, extent_group_id](auto disk_contexts){
            auto local_ip = get_local_ip();
            for(auto& disk_context : disk_contexts){
                auto disk_id = disk_context.get_id();
                auto disk_ip = disk_context.get_ip();
                if(disk_ip == local_ip){
                    auto mount_path = disk_context.get_mount_path();   
                    return make_ready_future<sstring>(mount_path);
                }
            }
            auto error_info = sprint("the disk_ids of extent_group(%s) is not include local(%s)"
                , extent_group_id, local_ip);
            throw hive::invalid_request_exception(error_info);
        });
    });


}

future<foreign_ptr<lw_shared_ptr<rmd_get_extent_group>>>
extent_store_proxy::get_extent_group_locally(smd_get_extent_group smd_data) {
    logger.debug("[{}] start, smd_data:{}", __func__, smd_data);
    sstring volume_id = smd_data.volume_id;
    sstring extent_group_id = smd_data.extent_group_id;
    return get_local_disk_mount_path(volume_id, extent_group_id).then([this, volume_id, extent_group_id]
            (auto disk_mount_path){
        unsigned shard = _extent_store.local().shard_of(extent_group_id);

        return _extent_store.invoke_on(shard, [extent_group_id, disk_mount_path](auto& extent_store) {
            auto offset = 0; 
            auto length = hive_config::extent_group_size; 
            return extent_store.read_extent_group(extent_group_id
                                                , offset
                                                , length
                                                , disk_mount_path);
        }).then([volume_id, extent_group_id](auto&& data){
            rmd_get_extent_group rmd_data(volume_id, extent_group_id, std::move(data));         
            return make_foreign(make_lw_shared<rmd_get_extent_group>(std::move(rmd_data)));
        });
   });
}
//end get extent group

} //namespace hive
