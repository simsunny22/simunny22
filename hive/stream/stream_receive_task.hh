#pragma once

#include "utils/UUID.hh"
#include "hive/stream/stream_task.hh"
#include <memory>

namespace hive {

class stream_session;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
class stream_receive_task: public stream_task {
private:
    uint64_t _total_size;
    bool _done = false;

public:
    stream_receive_task(shared_ptr<stream_session> session_
                      , uint64_t total_size_);
    ~stream_receive_task();

    virtual void abort() override {
    }

    virtual void start() override {
    }
 
    virtual long get_total_size() override {
        return _total_size;
    }
   
    virtual stream_summary get_summary() {
        return stream_summary("", "", get_total_size());
    }
};

} // namespace hive 
