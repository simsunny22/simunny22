#include "job_coordinator_service.hh"
#include "swarm_proxy.hh"

#include "unimplemented.hh"
#include "core/future-util.hh"
#include "exceptions/exceptions.hh"
#include "locator/snitch_base.hh"
#include "log.hh"
#include "to_string.hh"
#include <seastar/core/thread.hh>
#include <sstream>
#include <algorithm>
#include "unimplemented.hh"
#include <sys/time.h>
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/enum.hh>
#include <seastar/net/tls.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "atomic_cell.hh"
#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include "bytes.hh"
#include <chrono>
#include <functional>
#include <cstdint>
#include "hive/drain/drain_task_group.hh"
#include "hive/stream_service.hh"

using namespace std::chrono_literals;
namespace hive {
static logging::logger logger("job_coordinator_service");

distributed<job_coordinator_service> _the_job_coordinator_service;
using namespace exceptions;

job_coordinator_service::job_coordinator_service(){} 
job_coordinator_service::~job_coordinator_service(){}

future<>
job_coordinator_service::stop() {
    return make_ready_future<>();
}

future<>
job_coordinator_service::perform_drain(drain_task_group task_group) {
    // default send message timeout 30s
    auto _timeout = clock_type::now() + std::chrono::milliseconds(30000);
    gms::inet_address _target(task_group.target);
    smd_drain_task_group _task_group(
        task_group.volume_id
        , task_group.job_id
        , task_group.group_id
        , task_group.job_generation
        , task_group.target
        , task_group.segment_id
        , task_group.shard
        , task_group.journal_nodes
        , task_group.tasks
        , task_group.group_type
        );
    return hive::get_local_shared_swarm_proxy()->perform_drain(std::move(_task_group), _target, _timeout);
}

future<>
job_coordinator_service::drain_task_group_done(smd_drain_task_group result) {
    drain_task_group res(result.volume_id, result.job_id, result.group_id, result.job_generation, result.target, result.segment_id, result.shard, result.journal_nodes, result.tasks, result.group_type);
    return hive::get_local_stream_service().drain_task_group_done(std::move(res));
}

} //namespace hive
