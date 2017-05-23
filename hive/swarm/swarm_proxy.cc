#include "swarm_proxy.hh"
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
#include "hive/extent_datum.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"
#include "hive/context/context_service.hh"
#include "hive/extent_revision_set.hh"
#include "hive/migration_manager.hh"
#include "hive/swarm/swarm_service.hh"
#include "hive/swarm/job_coordinator_service.hh"

#include "hive/message_data_type.hh"
#include "hive/hive_service.hh"


namespace hive{

static logging::logger logger("swarm_proxy");

distributed<swarm_proxy> _the_swarm_proxy;
using namespace exceptions;

//for swarm_proxy define
swarm_proxy::swarm_proxy() {
    logger.debug("constructor start");
    init_messaging_service();
}

swarm_proxy::~swarm_proxy() {
    uninit_messaging_service();
}

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

void swarm_proxy::init_messaging_service() {
    auto& ms = hive::get_local_messaging_service();

    //handle drain task operation
    ms.register_drain_task_group([this] (const rpc::client_info& cinfo
          , smd_drain_task_group task_group
          , gms::inet_address reply_to
          , unsigned shard
          ) {
        logger.debug("receive DRAIN_TASK_GROUP message, from {}", reply_to);
        return do_with(std::move(task_group), get_local_shared_swarm_proxy(),
            [this, &cinfo, reply_to, shard] (smd_drain_task_group task_group, shared_ptr<swarm_proxy>& proxy) mutable {
            ++proxy->_stats.received_drain_tasks;
            return this->perform_drain_locally(std::move(task_group)).then([reply_to, shard]
                    (smd_drain_task_group result) {
                // send response back to caller
                auto& ms = hive::get_local_messaging_service();
                logger.debug("send DRAIN_TASK_GROUP to {}", reply_to);
                return do_with(std::move(result), [reply_to, shard, &ms](smd_drain_task_group result) {
                    return ms.send_drain_task_group_done(
                        hive::messaging_service::msg_addr{reply_to, shard}
                        , result
                        , shard
                        );
                });
            }).then_wrapped([] (future<> f) {
                f.ignore_ready_future();
            }).handle_exception([reply_to, shard] (std::exception_ptr eptr) {
                logger.error("warn, failed to drain from {}#{}, exception: {}"
                    , reply_to, shard, eptr);
            });
        }).then_wrapped([] (auto&& f) {
            return hive::messaging_service::no_wait();
        });
    });

    // handle drain task done, dispatch to caller
    ms.register_drain_task_group_done([this] (const rpc::client_info& cinfo
          , smd_drain_task_group result
          , unsigned shard
          ) {
        auto& from = cinfo.retrieve_auxiliary<gms::inet_address>("baddr");
        logger.debug("receive DRAIN_TASK_GROUP_DONE from {}", from);
        auto& proxy = hive::get_swarm_proxy();
        return proxy.invoke_on(shard, [result=std::move(result)] (auto& shard_proxy) mutable {
            shard_proxy.drain_task_group_done(std::move(result));
            return hive::messaging_service::no_wait();
        });
    });
}

future<smd_drain_task_group>
swarm_proxy::perform_drain_locally(smd_drain_task_group task_group) {
    logger.debug("drain task group locall dipatch");
    return hive::get_local_swarm_service().perform_drain(std::move(task_group));
}

void swarm_proxy::uninit_messaging_service() {
    auto& ms = hive::get_local_messaging_service();
    ms.unregister_drain_task_group();
    ms.unregister_drain_task_group_done();
}

future<> swarm_proxy::stop() {
    return make_ready_future<>();
}

future<> swarm_proxy::drain_task_group_done(smd_drain_task_group result) {
    return hive::get_local_job_coordinator_service().drain_task_group_done(std::move(result));
}

future<> swarm_proxy::perform_drain(
    smd_drain_task_group task_group 
    , gms::inet_address target
    , clock_type::time_point timeout
    ) {
    
    logger.debug("[{}] drain task group on remote node: {}", __func__, target);

    // do not dispatch this message if target is me
    if (is_me(target)) {
       logger.debug("[{}] drain task on local node: {}", __func__, target);
       perform_drain_locally(std::move(task_group)).then([this] (auto result) {
          return this->drain_task_group_done(std::move(result));
       });//not need wait
       return make_ready_future<>();
    }

    auto my_address = utils::fb_utilities::get_broadcast_address();
    auto& ms        = hive::get_local_messaging_service();
    return ms.send_drain_task_group(
        hive::messaging_service::msg_addr{target, 0}
        , timeout
        , task_group
        , my_address
        , engine().cpu_id()
       );
}

} //namespace hive
