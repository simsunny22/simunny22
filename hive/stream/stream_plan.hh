#pragma once

#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "core/sstring.hh"
#include "gms/inet_address.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include <vector>
#include "hive/stream/stream_coordinator.hh"
#include "hive/stream/stream_event_handler.hh"
#include "hive/stream/stream_detail.hh"
#include "hive/stream/migrate_params_entry.hh"


namespace hive {

class stream_state;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
class stream_plan {
private:
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    UUID    _plan_id;
    sstring _description;
    std::vector<stream_event_handler*> _handlers;
    shared_ptr<stream_coordinator> _coordinator;
    bool _range_added = false;
public:

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    stream_plan(sstring description)
        : _plan_id(utils::UUID_gen::get_time_UUID())
        , _description(description)
        , _coordinator(make_shared<stream_coordinator>()) {
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    stream_plan& request_files(inet_address from, migrate_params_entry params);

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    stream_plan& transfer_files(inet_address to, migrate_params_entry params);

    stream_plan& listeners(std::vector<stream_event_handler*> handlers);
public:
    /**
     * @return true if this plan has no plan to execute
     */
    bool is_empty() {
        return !_coordinator->has_active_sessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    future<stream_state> execute();
};

} // namespace hive
