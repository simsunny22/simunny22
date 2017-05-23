#include "hive/stream/stream_session.hh"
#include "hive/stream/stream_receive_task.hh"

namespace hive {

stream_receive_task::stream_receive_task(shared_ptr<stream_session> session_
                                       , uint64_t total_size_)
                                           : stream_task(session_)
                                           , _total_size(total_size_) {
}

stream_receive_task::~stream_receive_task() {
}

} // namespace hive 
