#include "hive/stream/stream_session_state.hh"
#include <ostream>
#include <map>
#include <seastar/core/sstring.hh>

namespace hive {

static const std::map<stream_session_state, sstring> stream_session_state_names = {
    {stream_session_state::INITIALIZED,     "INITIALIZED"},
    {stream_session_state::PREPARING,       "PREPARING"},
    {stream_session_state::STREAMING,       "STREAMING"},
    {stream_session_state::WAIT_COMPLETE,   "WAIT_COMPLETE"},
    {stream_session_state::COMPLETE,        "COMPLETE"},
    {stream_session_state::FAILED,          "FAILED"},
};

std::ostream& operator<<(std::ostream& os, const stream_session_state& s) {
    os << stream_session_state_names.at(s);
    return os;
}

}//namespace hive
