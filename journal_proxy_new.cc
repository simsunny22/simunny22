#include "db/consistency_level.hh"
#include "db/commitlog/commitlog.hh"
#include "journal_proxy_new.hh"
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

using namespace std::chrono_literals;

namespace hive {

extern hive::hive_status hive_service_status;
static logging::logger logger("journal_proxy_new");

distributed<hive::journal_proxy_new> _the_journal_proxy_new;

using namespace exceptions;

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

class abstract_journal_response_handler {
protected:
    journal_proxy_new::response_id_type    _id;
    shared_ptr<journal_proxy_new>          _proxy;
    std::shared_ptr<abstract_md>   _data;
    std::unordered_set<gms::inet_address>  _targets; 
    size_t _cl_acks = 0;
    bool   _cl_achieved = false;
    promise<> _ready; // available when cl is achieved
protected:
    size_t total_block_for() {
        return _targets.size();
    }
    virtual void signal(gms::inet_address from) {
        signal();
    }
public:
    abstract_journal_response_handler(shared_ptr<journal_proxy_new> p, 
            std::shared_ptr<abstract_md> data, 
            std::unordered_set<gms::inet_address> targets)
            : _id(p->_next_response_id++)
            , _proxy(std::move(p))
            , _data(std::move(data))
            , _targets(std::move(targets)) 
    {}
    virtual ~abstract_journal_response_handler() {
        if (!_cl_achieved) {
            _ready.set_exception(std::runtime_error("response handler destruct but _cl_achieved is false"));
        }
    };

    void trigger_exception(std::exception ex){
        if(!_cl_achieved){
            _cl_achieved = true;
            _ready.set_exception(ex);
        }
    }

    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= total_block_for()) {
             _cl_achieved = true;
             _ready.set_value();
         }
    }
    // return true on last ack
    bool response(gms::inet_address from) {
        signal(from);
        auto it = _targets.find(from);
        assert(it != _targets.end());
        _targets.erase(it);
        return _targets.size() == 0;
    }
    future<> wait() {
        return _ready.get_future();
    }

    const std::unordered_set<gms::inet_address>& get_targets() const {
        return _targets;
    }

    sstring get_format_targets() {
        sstring format_str;
        for(auto& target : _targets) {
            format_str += target.to_sstring() + ":";
        }
        return format_str.substr(0, format_str.length()-1);
    }

    std::shared_ptr<abstract_md> get_data() {
        return _data;
    }

    journal_proxy_new::response_id_type id() const {
      return _id;
    }

    friend journal_proxy_new;
}; //class abstract_journal_response_handler 

class init_commitlog_response_handler : public abstract_journal_response_handler {
public:
    init_commitlog_response_handler(shared_ptr<journal_proxy_new> proxy
                                  , std::shared_ptr<abstract_md> data 
                                  , std::unordered_set<gms::inet_address> targets) 
                                  : abstract_journal_response_handler(std::move(proxy)
                                      , std::move(data)
                                      , std::move(targets)) 
    {}
};

class sync_commitlog_response_handler_ex : public abstract_journal_response_handler {
public:
    sync_commitlog_response_handler_ex(shared_ptr<journal_proxy_new> proxy
                                  , std::shared_ptr<abstract_md> data 
                                  , std::unordered_set<gms::inet_address> targets) 
                                  : abstract_journal_response_handler(std::move(proxy)
                                      , std::move(data)
                                      , std::move(targets)) 
    {}
};

class discard_commitlog_response_handler_ex : public abstract_journal_response_handler {
public:
    discard_commitlog_response_handler_ex(shared_ptr<journal_proxy_new> proxy
                                     , std::shared_ptr<abstract_md> data 
                                     , std::unordered_set<gms::inet_address> targets) 
                                     : abstract_journal_response_handler(std::move(proxy)
                                         , std::move(data)
                                         , std::move(targets)) 
    {}
};

//define for inner struct
journal_proxy_new::rh_entry::rh_entry(std::unique_ptr<abstract_journal_response_handler>&& h
                                    , std::function<void()>&& cb) 
                                    : handler(std::move(h)), expire_timer(std::move(cb)) {}

journal_proxy_new::unique_response_handler::unique_response_handler(
    journal_proxy_new& p_
    , response_id_type id_) 
    : id(id_), p(p_) {}

journal_proxy_new::unique_response_handler::unique_response_handler(
    unique_response_handler&& x) 
    : id(x.id), p(x.p) { x.id = 0; };

journal_proxy_new::unique_response_handler::~unique_response_handler() {
    if (id) {
        p.remove_response_handler(id);
    }
}
journal_proxy_new::response_id_type journal_proxy_new::unique_response_handler::release() {
    auto r = id;
    id = 0;
    return r;
}

//define for journal_proxy_new
journal_proxy_new::journal_proxy_new(){
    init_messaging_service();
}

journal_proxy_new::~journal_proxy_new() {
    uninit_messaging_service();
}

void journal_proxy_new::init_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    //for init_commitlog
    ms.register_init_commitlog([](const rpc::client_info& cinfo
                                , smd_init_commitlog init_data
                                , std::vector<gms::inet_address> forward
                                , gms::inet_address reply_to
                                , unsigned shard
                                , response_id_type response_id){
        logger.debug("receive INIT_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(init_data), get_local_shared_journal_proxy_new(),
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id]
               (smd_init_commitlog& init_data, shared_ptr<journal_proxy_new>& proxy) mutable{
            ++proxy->_stats.received_creates;
            proxy->_stats.forwarded_creates += forward.size();
            return when_all(
                futurize<void>::apply([&proxy, &init_data, &reply_to] {
                    return proxy->init_commitlog_locally(reply_to, init_data); 
                }).then([reply_to, shard, response_id](){
                    auto& ms = hive::get_local_messaging_service();
                    logger.debug("send INIT_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                    return ms.send_init_commitlog_done(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                               .then_wrapped([](future<> f){
                          f.ignore_ready_future();
                    });
                }).handle_exception([reply_to, shard, response_id](std::exception_ptr eptr){
                    logger.debug("failed to init commitlog from {}#{}, response_id:{}, exception: {}"
                        , reply_to, shard, response_id, eptr);
                }),

               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &init_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a init commitlog to {}, response_id:{}", forward, response_id);
                   return ms.send_init_commitlog(hive::messaging_service::msg_addr{forward, 0}
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
    
    ms.register_init_commitlog_done([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy_new::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive INIT_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy_new().invoke_on(shard, [from, response_id] (journal_proxy_new& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //for sync_commitlog
    ms.register_sync_commitlog_ex([](const rpc::client_info& cinfo
                                   , smd_sync_commitlog sync_data
                                   , std::vector<gms::inet_address> forward
                                   , gms::inet_address reply_to
                                   , unsigned shard
                                   , response_id_type response_id) {
        logger.debug("receive SYNC_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(sync_data), get_local_shared_journal_proxy_new(), 
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id] 
               (smd_sync_commitlog& sync_data, shared_ptr<journal_proxy_new>& proxy) mutable{
           ++proxy->_stats.received_syncs;
           proxy->_stats.forwarded_syncs += forward.size();
           return when_all(
               futurize<void>::apply([&proxy, &sync_data, reply_to] {
                   return proxy->sync_commitlog_locally(reply_to, sync_data);
               }).then([reply_to, shard, response_id] {
                   auto& ms = hive::get_local_messaging_service();
                   logger.debug("send SYNC_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                   return ms.send_sync_commitlog_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                           .then_wrapped([] (future<> f) {
                       f.ignore_ready_future();
                   });
               }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                   logger.debug("failed to sync commitlog from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
               }),
               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &sync_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a sync commitlog to {}, response_id:{}", forward, response_id);
                   return ms.send_sync_commitlog_ex(hive::messaging_service::msg_addr{forward, 0}
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

    ms.register_sync_commitlog_done_ex([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy_new::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive SYNC_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy_new().invoke_on(shard, [from, response_id] (journal_proxy_new& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

    //for discard_commitlog
    ms.register_discard_commitlog_ex([](const rpc::client_info& cinfo
                                      , smd_discard_commitlog discard_data
                                      , std::vector<gms::inet_address> forward
                                      , gms::inet_address reply_to
                                      , unsigned shard
                                      , response_id_type response_id) {
        logger.debug("receive DISCARD_COMMITLOG message, from {}, forward.size():{}", reply_to, forward.size());
        return do_with(std::move(discard_data), get_local_shared_journal_proxy_new(), 
               [&cinfo, forward=std::move(forward), reply_to, shard, response_id] 
               (smd_discard_commitlog& discard_data, shared_ptr<journal_proxy_new>& proxy) mutable{
           ++proxy->_stats.received_discards;
           proxy->_stats.forwarded_discards += forward.size();
           return when_all(
               futurize<void>::apply([&proxy, &discard_data, reply_to] {
                   return proxy->discard_secondary_commitlog_locally(reply_to, discard_data);
               }).then([reply_to, shard, response_id] {
                   auto& ms = hive::get_local_messaging_service();
                   logger.debug("send DISCARD_COMMITLOG_DONE to {}, response_id:{}", reply_to, response_id);
                   return ms.send_discard_commitlog_done_ex(hive::messaging_service::msg_addr{reply_to, shard}, shard, response_id)
                           .then_wrapped([] (future<> f) {
                       f.ignore_ready_future();
                   });
               }).handle_exception([reply_to, shard, response_id] (std::exception_ptr eptr) {
                   logger.debug("failed to discard commitlog from {}#{}, response_id:{}, exception: {}"
                       , reply_to, shard, response_id, eptr);
               }),
               parallel_for_each(forward.begin(), forward.end(), 
                   [reply_to, shard, response_id, &discard_data, &proxy] 
                   (gms::inet_address forward)mutable {
                   auto& ms = hive::get_local_messaging_service();
                   auto config_timeout_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
                   auto timeout = clock_type::now() + std::chrono::milliseconds(config_timeout_ms);
                   logger.debug("forward a discard commitlog to {}, response_id:{}", forward, response_id);
                   return ms.send_discard_commitlog_ex(hive::messaging_service::msg_addr{forward, 0}
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

    ms.register_discard_commitlog_done_ex([] (const rpc::client_info& cinfo, unsigned shard, journal_proxy_new::response_id_type response_id) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive DISCARD_COMMITLOG_DONE from {}, response_id:{}", from, response_id);
        return get_journal_proxy_new().invoke_on(shard, [from, response_id] (journal_proxy_new& proxy) {
            proxy.got_response(response_id, from);
            return hive::messaging_service::no_wait();
        });
    });

}

void journal_proxy_new::uninit_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    ms.unregister_init_commitlog();
    ms.unregister_init_commitlog_done();

    ms.unregister_sync_commitlog_ex();
    ms.unregister_sync_commitlog_done_ex();

    ms.unregister_discard_commitlog_ex();
    ms.unregister_discard_commitlog_done_ex();
}

response_id_type journal_proxy_new::register_response_handler(std::unique_ptr<abstract_journal_response_handler>&& h) {
    auto id = h->id();
    auto e = _response_handlers.emplace(id, rh_entry(std::move(h), [this, id] {
    //for init_commitlog
    
        auto& e = _response_handlers.find(id)->second;
        if (!e.handler->_cl_achieved) {
            std::ostringstream out;
            out << "response handler timeout, handler_id:" << e.handler->id();
            out << ", not response targets: " << e.handler->get_format_targets();
            sstring error_info = out.str();
            logger.error(error_info.c_str());
            e.handler->trigger_exception(std::runtime_error(error_info));
        } 
        remove_response_handler(id);
    }));
    assert(e.second);
    return id;
}

abstract_journal_response_handler& journal_proxy_new::get_response_handler(response_id_type id) {
    return *_response_handlers.find(id)->second.handler;
}

void journal_proxy_new::remove_response_handler(response_id_type id) {
    _response_handlers.erase(id);
}

void journal_proxy_new::got_response(response_id_type id, gms::inet_address from) {
    auto it = _response_handlers.find(id);
    if (it != _response_handlers.end()) {
        if (it->second.handler->response(from)) {
            //last one, remove entry. Will cancel expiration timer too.
            remove_response_handler(id);         
        }
    }
}

future<> journal_proxy_new::response_wait(response_id_type id, clock_type::time_point timeout) {
    auto& e = _response_handlers.find(id)->second;
    e.expire_timer.arm(timeout);
    return e.handler->wait();
}

future<> journal_proxy_new::response_end(future<> result, utils::latency_counter lc) {
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

//for init commitlog
future<> journal_proxy_new::init_commitlog(smd_init_commitlog data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy_new::init_commitlog targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    logger.debug("[{}] start, data:{}, targets.size:{}", __func__, data, targets.size());
  
    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, create_init_commitlog_response_handler(std::move(data), targets));
    
    auto timeout_in_ms = get_local_hive_service().get_hive_config()->init_commitlog_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    auto response_id = response_h.id;
    auto f = response_wait(response_id, timeout);
    init_commitlog_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy_new::create_init_commitlog_response_handler(
        smd_init_commitlog data,
        std::unordered_set<gms::inet_address> targets) {
    
    std::unique_ptr<abstract_journal_response_handler> handler;
    std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_init_commitlog>(std::move(data));
    handler = std::make_unique<init_commitlog_response_handler>(
        shared_from_this(), 
        std::move(data_ptr), 
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void journal_proxy_new::init_commitlog_to_live_endpoints(
    response_id_type response_id
    , clock_type::time_point timeout) {

    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto my_address = utils::fb_utilities::get_broadcast_address();




    auto data_ptr = std::dynamic_pointer_cast<smd_init_commitlog>(handler.get_data());
    auto& init_data = *data_ptr;


    auto lmutate = [this, &init_data, response_id, my_address] {
        logger.debug("[init_commitlog_to_live_endpoints] in lmutate, data:{}", init_data);
        return init_commitlog_locally(my_address, init_data).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &init_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[init_commitlog_to_live_endpoints] in rmutate, data:{}, forward.size():{}"
            , init_data, forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_init_commitlog(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            init_data, 
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
                logger.error("exception during init commitlog to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy_new::init_commitlog_locally(gms::inet_address from, smd_init_commitlog& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    
    if(is_me(from)){
        logger.debug("[{}], is_me do nothing ...", __func__);
        return make_ready_future<>();
    }else{
        unsigned cpu_id = data.cpu_id;
        sstring volume_id = data.volume_id;
        sstring commitlog_id = data.commitlog_id;
        uint64_t max_size = data.size;
        //auto shard = get_local_journal_service().shard_of(commitlog_id);
        logger.debug("journal_proxy_new::init_commitlog_locally ..... cpu_id:{}, volume_id:{}, commitlog_id:{}, size:{}"
            , cpu_id, volume_id, commitlog_id, max_size);
        return hive::get_journal_service().invoke_on(cpu_id, [volume_id, commitlog_id, max_size](journal_service& journal_service){
            auto secondary_journal_ptr = journal_service.get_or_create_secondary_journal(volume_id);
            return secondary_journal_ptr->create_segment(volume_id, commitlog_id, max_size);

        });
    }

}




//for sync commitlog
future<> journal_proxy_new::sync_commitlog(smd_sync_commitlog data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy_new::sync_commitlog targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    logger.debug("[{}] start, data:{}, targets.size:{}", __func__, data, targets.size());
  
    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, create_sync_commitlog_response_handler(std::move(data), targets));
    
    auto timeout_in_ms = get_local_hive_service().get_hive_config()->sync_commitlog_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    auto response_id = response_h.id;
    auto f = response_wait(response_id, timeout);
    sync_commitlog_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy_new::create_sync_commitlog_response_handler(
        smd_sync_commitlog data,
        std::unordered_set<gms::inet_address> targets) {
    
    std::unique_ptr<abstract_journal_response_handler> handler;
    std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_sync_commitlog>(std::move(data));
    handler = std::make_unique<sync_commitlog_response_handler_ex>(
        shared_from_this(), 
        std::move(data_ptr), 
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void journal_proxy_new::sync_commitlog_to_live_endpoints(
    response_id_type response_id, 
    clock_type::time_point timeout)
{
    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto data_ptr = std::dynamic_pointer_cast<smd_sync_commitlog>(handler.get_data());
    auto& sync_data = *data_ptr;

    auto my_address = utils::fb_utilities::get_broadcast_address();

    auto lmutate = [this, &sync_data, response_id, my_address] {
        logger.debug("[sync_commitlog_to_live_endpoints] in lmutate, volume_id:{}", sync_data.volume_id);
        return sync_commitlog_locally(my_address, sync_data).then([this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &sync_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[sync_commitlog_to_live_endpoints] in rmutate, volume_id:{}, forward.size():{}"
            , sync_data.volume_id, forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_sync_commitlog_ex(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            sync_data, 
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
                logger.error("exception during sync commitlog to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy_new::sync_commitlog_locally(gms::inet_address from, smd_sync_commitlog& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    if(is_me(from)){
        logger.debug("[{}] start, is_me do nothing ...", __func__);
        return make_ready_future<>();
    }else{
        unsigned cpu_id = data.cpu_id;
        sstring volume_id = data.volume_id;
        sstring commitlog_id = data.commitlog_id;
        sstring extent_group_id = data.extent_group_id;
        sstring extent_id = data.extent_id;
        uint64_t offset = data.offset;
        uint64_t size = data.size;
        uint64_t vclock = data.vclock;
        logger.debug("cpu_id:{}, volume_id:{}, commitlog_id:{}, extent_group_id:{}, extent_id:{} , offset:{}, size:{}, data_size:{}"
            , cpu_id, volume_id, commitlog_id, extent_group_id, extent_id,  data.offset,  data.size, data.content.size());
        bytes&& content  = std::move(data.content);
        //auto shard = get_local_journal_service().shard_of(commitlog_id);
        return hive::get_journal_service().invoke_on(cpu_id, [volume_id, commitlog_id, extent_group_id, extent_id
                , offset, size, vclock, content = std::move(content)](journal_service& journal_service) mutable{
            //chenjl for temp test
            try{
               auto secondary_journal_ptr = journal_service.get_secondary_journal(volume_id);
               return secondary_journal_ptr->sync_segment(commitlog_id, extent_group_id, extent_id, offset, size, vclock, std::move(content));
            }catch (...){
               logger.error("chenjl for temp test, exception during sync commitlog to volume_id{}: {}", volume_id, std::current_exception());
               return make_ready_future<>();
            }
        });
    }
}

//for discard commitlog
future<> journal_proxy_new::discard_commitlog(smd_discard_commitlog data, std::unordered_set<gms::inet_address> targets) {
    logger.debug("journal_proxy_new::discard_commitlog targets size :{}", targets.size());
    if(targets.size() <= 0) {
        auto error_info = sprint("[{}] error, targets.size {}<=0, data:{}", __func__, targets.size(), data);
        return make_exception_future<>(std::runtime_error(error_info));
    }
    logger.debug("[{}] start, data:{}", __func__, data);

    utils::latency_counter lc;
    lc.start();
    unique_response_handler response_h(*this, this->create_discard_commitlog_response_handler(std::move(data), targets));

    auto timeout_in_ms = get_local_hive_service().get_hive_config()->write_request_timeout_in_ms();
    auto timeout = clock_type::now() + std::chrono::milliseconds(timeout_in_ms);
    
    auto response_id = response_h.id;
    auto f = this->response_wait(response_id, timeout);
    this->discard_commitlog_to_live_endpoints(response_h.release(), timeout); 

    return std::move(f).then_wrapped([p=this->shared_from_this(), lc] (future<> f) {
        return p->response_end(std::move(f), lc);
    });
}

response_id_type journal_proxy_new::create_discard_commitlog_response_handler(
    smd_discard_commitlog data
    , std::unordered_set<gms::inet_address> targets) {

    std::unique_ptr<abstract_journal_response_handler> handler;
    std::shared_ptr<abstract_md> data_ptr = std::make_shared<smd_discard_commitlog>(std::move(data));
    handler = std::make_unique<discard_commitlog_response_handler_ex>(
        shared_from_this(), 
        std::move(data_ptr), 
        std::move(targets));

    return register_response_handler(std::move(handler));
}

void journal_proxy_new::discard_commitlog_to_live_endpoints(
    response_id_type response_id
    , clock_type::time_point timeout) {

    logger.debug("[{}] start, response_id:{}", __func__, response_id);
    auto&& handler = get_response_handler(response_id);
    auto data_ptr = std::dynamic_pointer_cast<smd_discard_commitlog>(handler.get_data());
    auto& discard_data = *data_ptr;
    
    auto my_address = utils::fb_utilities::get_broadcast_address();

    auto lmutate = [this, &discard_data, response_id, my_address] {
        logger.debug("[discard_commitlog_to_live_endpoints] in lmutate, response_id:{}", response_id);
        return discard_secondary_commitlog_locally(my_address, discard_data).then(
                [this, response_id, my_address] {
            got_response(response_id, my_address);
        });
    };

    auto rmutate = [this, &discard_data, response_id, my_address, timeout] 
            (gms::inet_address coordinator, std::vector<gms::inet_address>&& forward)mutable {
        logger.debug("[discard_commitlog_to_live_endpoints] in rmutate, response_id:{}, forward.size():{}"
            , response_id, forward.size());
        auto& ms = hive::get_local_messaging_service();
        return ms.send_discard_commitlog_ex(
            hive::messaging_service::msg_addr{coordinator, 0}, 
            timeout, 
            discard_data, 
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
                logger.error("exception during discard commitlog to {}: {}", coordinator, std::current_exception());
            }
        });
    }
}

future<> journal_proxy_new::discard_secondary_commitlog_locally(gms::inet_address from, smd_discard_commitlog& data){
    logger.debug("[{}] start, data:{}", __func__, data);
    
    if(is_me(from)){
        logger.debug("[{}] start, data:{}", __func__, data);
        return make_ready_future<>();
    }else{
        logger.debug("discard_secondary_commitlog_locally volume_id:{}, commitlog_id:{} ", data.volume_id, data.commitlog_id);
        unsigned cpu_id = data.cpu_id;
        sstring volume_id = data.volume_id;
        sstring commitlog_id = data.commitlog_id;
        //auto shard = get_local_journal_service().shard_of(commitlog_id);
        return hive::get_journal_service().invoke_on(cpu_id, [volume_id, commitlog_id](journal_service& journal_service) mutable{
            //chenjl add for test
            try{
                auto secondary_journal_ptr = journal_service.get_secondary_journal(volume_id);
                return secondary_journal_ptr->discard_segment(commitlog_id);
            }catch (...){
                logger.error("chenjl for temp test, exception during discard commitlog to volume_id{}: {}", volume_id, std::current_exception());
                return make_ready_future<>();
            }
        });
    }
}


future<> journal_proxy_new::stop() {
    return make_ready_future<>();
}

} //namespace hive
