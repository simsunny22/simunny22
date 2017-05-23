#include "swarm_service.hh"

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

#include "hive/drain/drain_extent_group_task.hh"
#include "hive/drain/drain_task_group.hh"

using namespace std::chrono_literals;
namespace hive {
static logging::logger logger("swarm_service");

distributed<swarm_service> _the_swarm_service;
using namespace exceptions;

swarm_service::swarm_service() {
    _drain_minion = make_lw_shared<drain_minion>();
}

swarm_service::~swarm_service(){}

future<>
swarm_service::stop() {
    return make_ready_future<>();
}

future<smd_drain_task_group>
swarm_service::perform_drain(smd_drain_task_group task_group) {
    hive::drain_task_group task_group_(task_group.volume_id, task_group.job_id, task_group.group_id, task_group.job_generation, task_group.target, task_group.segment_id, task_group.shard, task_group.journal_nodes, task_group.tasks, task_group.group_type);
    return _drain_minion->perform_drain(std::move(task_group_)).then([](auto result){
        smd_drain_task_group smd_result(result.volume_id, result.job_id, result.group_id, result.job_generation, result.target, result.segment_id, result.shard, result.journal_nodes, result.tasks, result.group_type);
        return make_ready_future<smd_drain_task_group>(std::move(smd_result));
    });
}

} //namespace hive
