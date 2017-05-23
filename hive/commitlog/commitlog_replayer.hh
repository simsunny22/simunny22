#pragma once

#include <memory>
#include <core/future.hh>
#include <core/sharded.hh>

class database;

namespace hive{

class hive_commitlog;
class stream_service;

class commitlog_replayer {
private:
    commitlog_replayer();
    class impl;
    std::unique_ptr<impl> _impl;

public:
    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    static future<commitlog_replayer> create_replayer(sstring rp_log_dir);

    future<> replay(std::vector<sstring> files);
    future<> replay(sstring file);

};

} //namespace hive
