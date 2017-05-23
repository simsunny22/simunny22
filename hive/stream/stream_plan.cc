#include "hive/stream/stream_plan.hh"
#include "hive/stream/stream_result_future.hh"
#include "hive/stream/stream_state.hh"

namespace hive {

static logging::logger logger("stream_plan");

stream_plan& stream_plan::request_files(inet_address from, migrate_params_entry params) {
    logger.debug("{}, start", __func__);
    _range_added = true;
    auto session = _coordinator->get_or_create_session(from);
    session->add_stream_request(std::move(params));
    return *this;
}

stream_plan& stream_plan::transfer_files(inet_address to, migrate_params_entry params) {
    logger.debug("{}, start", __func__);
    _range_added = true;
    auto session = _coordinator->get_or_create_session(to);
    session->add_stream_transfer(std::move(params));
    return *this;
}

future<stream_state> stream_plan::execute() {
    logger.debug("{} start, plan_id:{}, description:{}, range_added:{}", __func__,  _plan_id, _description, _range_added);
    if (!_range_added) {
        stream_state state(_plan_id, _description, std::vector<session_info>());
        return make_ready_future<stream_state>(std::move(state));
    }
    return stream_result_future::init_sending_side(_plan_id, _description, _handlers, _coordinator);
}

stream_plan& stream_plan::listeners(std::vector<stream_event_handler*> handlers) {
    std::copy(handlers.begin(), handlers.end(), std::back_inserter(_handlers));
    return *this;
}

} //namespace hive
