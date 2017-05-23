#pragma once

#include "utils/UUID.hh"
#include "hive/stream/stream_summary.hh"
#include <memory>
#include "core/shared_ptr.hh"

namespace hive {

class stream_session;

/**
 * StreamTask is an abstraction of the streaming task performed over specific ColumnFamily.
 */
class stream_task {
public:
    /** StreamSession that this task belongs */
    shared_ptr<stream_session> session;
    stream_task(shared_ptr<stream_session> session_);

    virtual ~stream_task();

public:
    virtual void start() = 0;
    virtual void abort() = 0;

    virtual long get_total_size() = 0;

    virtual stream_summary get_summary() = 0;
};

} // namespace hive 
