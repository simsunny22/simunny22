#include "hive/migrate_forward_proxy.hh"
#include "db/consistency_level.hh"
#include "unimplemented.hh"
#include "core/do_with.hh"
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
#include "utils/joinpoint.hh"
#include "types.hh"

#include "hive/context/context_service.hh"
#include "hive/messaging_service.hh"
#include "hive/migration_manager.hh"


namespace hive{
using namespace exceptions;

static logging::logger logger("migrate_forward_proxy");
distributed<migrate_forward_proxy> _the_migrate_forward_proxy;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

static inline
sstring get_local_dc() {
    auto local_addr = utils::fb_utilities::get_broadcast_address();
    auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
    return snitch_ptr->get_datacenter(local_addr);
}

class migrate_forward_response_handler {
protected:
    migrate_forward_proxy::response_id_type       _id;
    shared_ptr<migrate_forward_proxy>             _proxy;
    std::unordered_set<gms::inet_address> _dst_peers_addr; 
    migrate_params_entry                  _migrate_params;
    size_t  _block = 0;
    sstring _discription = "";

    size_t _acks = 0;
    bool   _achieved = false;
    promise<> _ready; // available when achieved
    std::unordered_set<gms::inet_address> _reply_peers_addr; 
protected:
    virtual size_t total_block_for() {
        return _block;
    }
   
public:
    migrate_forward_response_handler(shared_ptr<migrate_forward_proxy> proxy
                            , std::unordered_set<gms::inet_address> dst_peers_addr
                            , migrate_params_entry migrate_params 
                            , size_t block
                            , sstring discription)
                                : _id(proxy->_next_response_id++)
                                , _proxy(std::move(proxy))
                                , _dst_peers_addr(dst_peers_addr)
                                , _migrate_params(migrate_params)
                                , _block(block)
                                , _discription(discription){
    }
    virtual ~migrate_forward_response_handler() {
        if (!_achieved) {
            _ready.set_value();
        }
    };

    void trigger_exception(sstring error_info){
        if(!_achieved){
            _achieved = true;
            _ready.set_exception(std::runtime_error(error_info));
        }
    }

    void signal(size_t nr = 1) {
        _acks += nr;
        logger.debug("signal enter !, _achieved {}, _acks {}, total_block_for() {}"
            , _achieved, _acks, total_block_for());
        if (!_achieved && _acks >= total_block_for()) {
             _achieved = true;
             _ready.set_value();
        }
    }

    // return true when last ack
    bool response(gms::inet_address reply_from) {
        auto it = _dst_peers_addr.find(reply_from);
        if(it != _dst_peers_addr.end()) {
            signal();
            _dst_peers_addr.erase(it);
            _reply_peers_addr.insert(reply_from);
        }
        return _dst_peers_addr.size() == 0;
    }

    future<> wait(){
        return _ready.get_future();
    }
 
    migrate_params_entry get_migrate_params() {
        return _migrate_params;
    }
    
    migrate_forward_proxy::response_id_type id() const {
        return _id;
    }
    
    std::unordered_set<gms::inet_address> get_dst_peers_addr(){
        return _dst_peers_addr; 
    }

    friend migrate_forward_proxy;

}; //migrate_forward_response_handler


////////////////////////////////////////////////////////////////
// migrate_forward_proxy 
////////////////////////////////////////////////////////////////
migrate_forward_proxy::migrate_forward_proxy(db::config config)
    : _config(std::make_unique<db::config>(config)){
    logger.info("[{}] constructor start", __func__);
   
    init_messaging_service();

    //_collectd_registrations = std::make_unique<scollectd::registrations>(scollectd::registrations({
    //   scollectd::add_polled_metric(scollectd::type_instance_id("migrate_forward_proxy"
    //            , scollectd::per_cpu_plugin_instance
    //            , "queue_length", "reads")
    //            , scollectd::make_typed(scollectd::data_type::GAUGE, _stats.reads)
    //    ),
    //}));
}

migrate_forward_proxy::~migrate_forward_proxy() {
    logger.info("[{}] deconstructor start", __func__);
}

void migrate_forward_proxy::init_messaging_service() {
    auto& ms = hive::get_local_messaging_service();
    ms.register_migrate_forward([](const rpc::client_info& cinfo, migrate_params_entry migrate_params 
            ,  gms::inet_address reply_to, unsigned shard, migrate_forward_proxy::response_id_type response_id){
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive msg MIGRATE_FORWARD from {}", from);
        auto local_proxy = get_local_shared_migrate_forward_proxy();
        return local_proxy->migrate_forward_locally(migrate_params).then(
                [reply_to, shard, response_id](){
            auto& ms = hive::get_local_messaging_service();
            logger.debug("will send MIGRATE_FORWARD_DONE to:{}, shard:{}", reply_to, shard);
            return ms.migrate_forward_done(hive::messaging_service::msg_addr{reply_to, shard}
                    , shard, response_id).then_wrapped([reply_to, shard] (future<> f) {
                logger.debug("send MIGRATE_FORWARD_DONE ok to:{}, shard:{}", reply_to, shard);
                f.ignore_ready_future();
            });
        }).then_wrapped([reply_to, shard, response_id](auto f){
            try{
                f.get();
            }catch(...){
                std::ostringstream out;
                out << "error, handle MIGRATE_FORWARD message";
                out << ", exception:" << std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());
                auto& ms = hive::get_local_messaging_service();
                ms.migrate_forward_exception(hive::messaging_service::msg_addr{reply_to, shard}
                    , error_info, shard, response_id).then_wrapped([reply_to, shard] (future<> f) {
                    logger.debug("send MIGRATE_FORWARD_EXCEPTION ok to:{}, shard:{}", reply_to, shard);
                    f.ignore_ready_future();
                });
            }
            return hive::messaging_service::no_wait();
        });
    });

    ms.register_migrate_forward_done([](const rpc::client_info& cinfo, unsigned shard
            , migrate_forward_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive msg MIGRATE_FORWARD_DONE from {}", from);
        return get_migrate_forward_proxy().invoke_on(shard, [response_id, from] (auto& shard_proxy) {
            shard_proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    ms.register_migrate_forward_exception([](const rpc::client_info& cinfo, sstring exception, unsigned shard
            , migrate_forward_proxy::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive msg MIGRATE_FORWARD_EXCEPTION from {}, exception:{}", from, exception);
        return get_migrate_forward_proxy().invoke_on(shard, [response_id, from, exception] (auto& shard_proxy) {
            shard_proxy.trigger_exception(response_id, from, exception);
            return hive::messaging_service::no_wait();
        });
    });
}

void migrate_forward_proxy::uninit_messaging_service() {
    logger.info("[{}] start", __func__);
    auto& ms = hive::get_local_messaging_service();
    ms.unregister_migrate_forward();
    ms.unregister_migrate_forward_done();
    ms.unregister_migrate_forward_exception();
}


future<> migrate_forward_proxy::stop() {
    logger.info("[{}] start", __func__);
    uninit_messaging_service();
    return make_ready_future<>();
}

unsigned migrate_forward_proxy::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

migrate_forward_proxy::response_id_type 
migrate_forward_proxy::register_response_handler(std::unique_ptr<migrate_forward_response_handler>&& response_handler) {
    auto response_handler_id = response_handler->id();
    auto ret = _response_handlers.emplace(response_handler_id
           , rh_entry(std::move(response_handler), [this, response_handler_id] {
               //response timeout callback
               auto& response_handler = _response_handlers.find(response_handler_id)->second;
               if (!response_handler.handler->_achieved) {
                    sstring error_info = "response handler timeout";
                    response_handler.handler->trigger_exception(error_info);
                } else {
                    logger.trace("already response, response_handler_id:{}", response_handler_id);
                }
                remove_response_handler(response_handler_id);
           })
    );

    assert(ret.second);
    return response_handler_id;
}

void migrate_forward_proxy::remove_response_handler(migrate_forward_proxy::response_id_type response_handler_id) {
    logger.debug("[{}] start, response_handler_id:{}", __func__, response_handler_id);
    _response_handlers.erase(response_handler_id);
}

void migrate_forward_proxy::got_response(migrate_forward_proxy::response_id_type response_handler_id
                               , gms::inet_address from) {
    auto it = _response_handlers.find(response_handler_id);
    if (it != _response_handlers.end()) {
        if (it->second.handler->response(from)) {
            remove_response_handler(response_handler_id); // last one, remove entry. Will cancel expiration timer too.
        }
    }else{
        logger.warn("[{}] can not find response_handler, response_handler_id:{}"
            , __func__, response_handler_id); 
    }
}

void migrate_forward_proxy::trigger_exception(migrate_forward_proxy::response_id_type response_handler_id
        , gms::inet_address from
        , sstring exception) {
    auto it = _response_handlers.find(response_handler_id);
    if (it != _response_handlers.end()) {
        it->second.handler->trigger_exception(exception);
    }else{
        logger.warn("[{}] can not find response_handler, response_handler_id:{}"
            , __func__, response_handler_id); 
    }
}

future<> migrate_forward_proxy::response_wait(migrate_forward_proxy::response_id_type response_handler_id
                           , clock_type::time_point timeout) {
    auto& handler_entry = _response_handlers.find(response_handler_id)->second;
    handler_entry.expire_timer.arm(timeout);
    return handler_entry.handler->wait();
}

migrate_forward_response_handler& 
migrate_forward_proxy::get_response_handler(migrate_forward_proxy::response_id_type response_handler_id) {
    return *_response_handlers.find(response_handler_id)->second.handler;
}

migrate_forward_proxy::rh_entry::rh_entry(std::unique_ptr<migrate_forward_response_handler>&& h
                                , std::function<void()>&& cb) 
                                    : handler(std::move(h))
                                    , expire_timer(std::move(cb)){
                                    
}

migrate_forward_proxy::unique_response_handler::unique_response_handler(migrate_forward_proxy& p_, response_id_type id_) : id(id_), p(p_) {}
migrate_forward_proxy::unique_response_handler::unique_response_handler(unique_response_handler&& x) : id(x.id), p(x.p) { x.id = 0; };
migrate_forward_proxy::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}

migrate_forward_proxy::response_id_type migrate_forward_proxy::unique_response_handler::release() {
    auto old_id = id;
    id = 0;
    return old_id;
}

future<> migrate_forward_proxy::do_action_prepare(migrate_params_entry migrate_params
        , uint32_t timeout_in_ms
        , std::function<void (migrate_forward_proxy::response_id_type, clock_type::time_point)> action_fun) {
    //create_response_handler 
    gms::inet_address peer_addr;
    if(migrate_type::MIGRATE_EXTENT_GROUP == migrate_params.type){
        peer_addr = gms::inet_address(migrate_params.driver_node_ip);
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == migrate_params.type){
        peer_addr = gms::inet_address(migrate_params.src_node_ip);
    }else{
        auto error_info = "[do_action_prepare] error, unknown migrate type";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    std::unordered_set<gms::inet_address> dst_peers_addr;
    dst_peers_addr.insert(peer_addr);
    assert(dst_peers_addr.size()>0);

    size_t block = dst_peers_addr.size();
    sstring discription= "migrate api forward";
    std::unique_ptr<migrate_forward_response_handler> h = std::make_unique<migrate_forward_response_handler>
        (shared_from_this(), dst_peers_addr, migrate_params, block, discription);

    auto response_id = register_response_handler(std::move(h));

    //send request
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    auto wait_fut = response_wait(response_id, timeout);
    action_fun(response_id, timeout); // response is now running and it will either complete or timeout
    return std::move(wait_fut);    
}

future<> migrate_forward_proxy::do_action_done(future<> result_fut, utils::latency_counter lc) {
    assert(result_fut.available());
    logger.debug("[{}] latency: {} nanosecond", __func__, lc.stop().latency_in_nano());
    try {
        logger.debug("response wait start!");
        result_fut.get();
        logger.debug("response wait done!");
        return make_ready_future<>();
    } catch (exceptions::unavailable_exception& ex) {
        logger.error("error, Unavailable, exception:{}", std::current_exception());
        return make_exception_future<>(std::current_exception());
    } catch(overloaded_exception& ex) {
        logger.error("error, Overloaded, exception:{}", std::current_exception());
        return make_exception_future<>(std::current_exception());
    } catch(...) {
        logger.error("error, unkown exception:{}", std::current_exception());
        return make_exception_future<>(std::current_exception());
    }
}

future<bool> migrate_forward_proxy::is_driver_node(sstring volume_id){
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.get_or_pull_volume_context(volume_id).then(
        [](auto volume_context){
            auto driver_node_ip = volume_context.get_driver_node().get_ip();
            return make_ready_future<bool>(is_me(driver_node_ip));
    }); 
}


future<> migrate_forward_proxy::migrate_forward(migrate_params_entry migrate_params) {
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params); 
    utils::latency_counter lc;
    lc.start();

    uint32_t timeout;
    if(migrate_type::MIGRATE_EXTENT_GROUP == migrate_params.type){
        timeout = _config->migrate_forward_extent_group_timeout_in_ms();
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == migrate_params.type){
        timeout = _config->migrate_forward_extent_journal_timeout_in_ms();
    }else{
        auto error_info = "[migrate_forward] error, unknown migrate type";    
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    auto need_forward  = _config->migrate_need_forward();
    if(need_forward){
        return do_action_prepare(migrate_params, timeout, 
                [this, migrate_params](auto response_id, auto timeout){
            this->do_migrate_forward(response_id, timeout, migrate_params);
        }).then_wrapped([p = shared_from_this(), lc] (auto f) {
            return p->do_action_done(std::move(f), lc);
        });
    }else{
        return is_driver_node(migrate_params.volume_id).then([this, migrate_params](bool is_me){
            if(!is_me){
                auto error_info = "[migrate_forward] error, is not a driver node";
                logger.error(error_info);
                return make_exception_future<>(std::runtime_error(error_info));
            }
            return this->migrate_forward_locally(migrate_params);
        });
    }
    
}

void migrate_forward_proxy::do_migrate_forward(migrate_forward_proxy::response_id_type response_id
                                          , clock_type::time_point timeout
                                          , migrate_params_entry migrate_params){
    logger.debug("[{}] start, response_id:{}, migrate_params:{}", __func__, response_id, migrate_params);
    auto lmutate = [this, response_id, migrate_params](gms::inet_address dst_peer_addr) {
        return migrate_forward_locally(migrate_params).then_wrapped(
            [this, response_id, p=shared_from_this(), dst_peer_addr](auto f){
            try{
                f.get(); 
                this->got_response(response_id, dst_peer_addr);
            }catch(...){
                std::ostringstream out;
                out << "error, do_migrate_forward, in lmutate, dst_peer_addr:"<< dst_peer_addr;
                out << ", exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                this->trigger_exception(response_id, dst_peer_addr, error_info);
            }
        });
    };

    auto my_address = utils::fb_utilities::get_broadcast_address();
    auto rmutate = [this, response_id, timeout, my_address, migrate_params](gms::inet_address dst_peer_addr) {
        logger.debug("do_migrate_forward,send message MIGRATE_FORWARD to {}, response_id:{}, migrate_params:{}"
            , dst_peer_addr, response_id, migrate_params);
        auto& ms = hive::get_local_messaging_service();
        return ms.migrate_forward(hive::messaging_service::msg_addr{dst_peer_addr, 0}, timeout, migrate_params
               , my_address, engine().cpu_id(), response_id).then_wrapped([this, response_id, dst_peer_addr](auto f){
            try{
                f.get(); 
            }catch(...){
                std::ostringstream out;
                out << "error, do_migrate_forward, in rmutate, exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                this->trigger_exception(response_id, dst_peer_addr, error_info);
            }
        }).finally([this, p = shared_from_this()] {
            logger.debug("{} dltest================= finally in", __func__);
        });
    };

    std::unordered_set<gms::inet_address> dst_peers_addr = get_response_handler(response_id).get_dst_peers_addr();

    assert(1 == dst_peers_addr.size()); //for migrate forward proxy 
    auto dst_peer_addr = *dst_peers_addr.begin();


    future<> f = make_ready_future<>();
    if (dst_peer_addr == my_address) {
        logger.debug("[{do_migrate_forward}], callback lmutate start");
        auto f = futurize<void>::apply(lmutate, dst_peer_addr);
    } else {
        logger.debug("[{do_migrate_forward}], callback rmutate start");
        auto f = futurize<void>::apply(rmutate, dst_peer_addr);
    }

    f.handle_exception([dst_peer_addr] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch(rpc::closed_error&) {
            // ignore, disconnect will be logged by gossiper
            logger.warn("warn, during migrate forward to {}, exception:{}", dst_peer_addr, std::current_exception());
        } catch(seastar::gate_closed_exception&) {
            // may happen during shutdown, ignore it
            logger.warn("warn, during migrate forward to {}, exception:{}", dst_peer_addr, std::current_exception());
        } catch(...) {
            logger.error("exception during migrate forward to {}, exception:{}", dst_peer_addr, std::current_exception());
        }
    });
}

future<> migrate_forward_proxy::migrate_forward_extent_group_locally(migrate_params_entry migrate_params){
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);
    assert(migrate_type::MIGRATE_EXTENT_GROUP == migrate_params.type);
    auto shard_id = hive::get_local_migration_manager().shard_of(migrate_params.extent_group_id);
    return hive::get_migration_manager().invoke_on(shard_id, [migrate_params]
            (auto& shard_migration_manager) mutable {
        return shard_migration_manager.migrate_extent_group(migrate_params).then_wrapped(
                [migrate_params](auto f)mutable{
            try{
                f.get();
                logger.debug("[migrate_forward_locally] done");
                return make_ready_future<>();
            }catch(...){
                std::ostringstream out;
                out << "[migrate_forward_locally] failed, migrate_params:" << migrate_params;
                out << ", exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);
            }
        });
    });
}

future<> migrate_forward_proxy::migrate_forward_extent_journal_locally(migrate_params_entry migrate_params){
    assert(migrate_type::MIGRATE_EXTENT_JOURNAL == migrate_params.type);
    //tododl:yellow not shard here
    //return hive::get_local_migration_manager().migrate_extent_journal_concurrent(migrate_params);
    return hive::get_local_migration_manager().migrate_extent_journal_ordered(migrate_params);
}

future<> migrate_forward_proxy::migrate_forward_locally(migrate_params_entry migrate_params){
    if(migrate_type::MIGRATE_EXTENT_GROUP == migrate_params.type){
        return migrate_forward_extent_group_locally(migrate_params); 
    }else if(migrate_type::MIGRATE_EXTENT_JOURNAL == migrate_params.type){
        return migrate_forward_extent_journal_locally(migrate_params); 
    }else {
        auto error_info("[migrate_forward_locally] error, unknown migrate type"); 
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info));
    }
}

} //namespace hive
