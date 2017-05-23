#pragma once

#include <ostream>

namespace hive {

enum class stream_session_state {
    INITIALIZED,
    PREPARING,
    STREAMING,
    WAIT_COMPLETE,
    COMPLETE,
    FAILED,
};

std::ostream& operator<<(std::ostream& os, const stream_session_state& s);

} // namespace hive
