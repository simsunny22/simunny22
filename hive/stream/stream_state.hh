#pragma once

#include "utils/UUID.hh"
#include "hive/stream/session_info.hh"
#include <vector>

namespace hive {

/**
 * Current snapshot of streaming progress.
 */
class stream_state {
public:
    using UUID = utils::UUID;
    UUID plan_id;
    sstring description;
    std::vector<session_info> sessions;

    stream_state(UUID plan_id_, sstring description_, std::vector<session_info> sessions_)
        : plan_id(std::move(plan_id_))
        , description(std::move(description_))
        , sessions(std::move(sessions_)) {
    }

    bool has_failed_session() {
        for (auto& x : sessions) {
            if (x.is_failed()) {
                return true;
            }
        }
        return false;
    }
};

} // namespace hive 
