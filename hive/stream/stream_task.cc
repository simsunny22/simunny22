#include "hive/stream/stream_task.hh"
#include "hive/stream/stream_session.hh"

namespace hive {

stream_task::stream_task(shared_ptr<stream_session> session_):session(session_){
}

stream_task::~stream_task() = default;

} //namespace hive
