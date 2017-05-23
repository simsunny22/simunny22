#pragma once

#include "utils/UUID.hh"
#include "gms/inet_address.hh"
#include "hive/stream/stream_session.hh"
#include "hive/stream/session_info.hh"
#include "hive/stream/progress_info.hh"

namespace hive {

class stream_event {
public:
    using UUID = utils::UUID;
    enum class type {
        STREAM_PREPARED,
        STREAM_COMPLETE,
        FILE_PROGRESS,
    };

    type event_type;
    UUID plan_id;

    stream_event(type event_type_, UUID plan_id_)
        : event_type(event_type_)
        , plan_id(plan_id_) {
    }
};

struct session_complete_event : public stream_event {
    using inet_address = gms::inet_address;
    inet_address peer;
    bool success;

    session_complete_event(shared_ptr<stream_session> session)
        : stream_event(stream_event::type::STREAM_COMPLETE, session->plan_id())
        , peer(session->peer)
        , success(session->is_success()) {
    }
};

struct progress_event : public stream_event {
    using UUID = utils::UUID;
    progress_info progress;
    progress_event(UUID plan_id_, progress_info progress_)
        : stream_event(stream_event::type::FILE_PROGRESS, plan_id_)
        , progress(std::move(progress_)) {
    }
};

struct session_prepared_event : public stream_event {
    using UUID = utils::UUID;
    session_info session;
    session_prepared_event(UUID plan_id_, session_info session_)
        : stream_event(stream_event::type::STREAM_PREPARED, plan_id_)
        , session(std::move(session_)) {
    }
};

} // namespace hive 
