#pragma once

#include "hive/stream/stream_event.hh"

namespace hive {

class stream_event_handler /* extends FutureCallback<StreamState> */ {
public:
    /**
     * Callback for various streaming events.
     *
     * @see StreamEvent.Type
     * @param event Stream event.
     */
    virtual void handle_stream_event(session_complete_event event) {}
    virtual void handle_stream_event(progress_event event) {}
    virtual void handle_stream_event(session_prepared_event event) {}
    virtual ~stream_event_handler() {};
};

} // namespace hive 
