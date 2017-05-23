#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <db/config.hh>
#include <cstdio>
#include <database.hh>
#include "hive/hive_httpd.hh" 
#include "utils/file_lock.hh"
#include "disk-error-handler.hh"
#include "hive/cache/stream_cache.hh"
#include "hive/hive_directories.hh"

namespace bpo = boost::program_options;

class hive_thrift_server;

namespace hive {

enum hive_status{
    IDLE = 0,
    NORMAL,
    BOOTING, 
    STOPPING
};

class hive_service;
inline distributed<hive_service>& get_hive_service();

class hive_service: public seastar::async_sharded_service<hive_service> {
private:
    hive::hive_httpd          _hive_httpd; 
    hive::directories         _dirs;
    timer<>                   _context_task_timer;
    lw_shared_ptr<db::config> _hive_config;

    sstring _operation_in_progress;

    // thrift server
    shared_ptr<distributed<hive_thrift_server>> _thrift_server;
    bool _thrift_server_start = false;

private:
    template <typename Func>
    inline auto run_with_api_lock(sstring operation, Func&& func) {
        return get_hive_service().invoke_on(0, [operation = std::move(operation),
                func = std::forward<Func>(func)] (hive_service& hs) mutable {
            if (!hs._operation_in_progress.empty()) {
                throw std::runtime_error(sprint("Operation %s is in progress, try again", hs._operation_in_progress));
            }
            hs._operation_in_progress = std::move(operation);
            return func(hs).finally([&hs] {
                hs._operation_in_progress = sstring();
            });
        });
    }

    template <typename Func>
    inline auto run_with_no_api_lock(Func&& func) {
        return get_hive_service().invoke_on(0, [func = std::forward<Func>(func)] (hive_service& hs) mutable {
            return func(hs);
        });
    }

private:
    void print_config_info();
    future<> init_config();
    future<> touch_commitlog_directory();

    future<> start_message_service();
    future<> start_context_service();
    future<> auto_inject_context();
    future<> start_stream_cache();

    future<std::vector<sstring>> list_files(sstring dirname);
    //future<std::vector<sstring>> clear_commitlog_files(sstring directory, std::vector<sstring> old_files);
    std::vector<sstring> get_delete_files(std::vector<sstring> new_files, std::vector<sstring> old_files);
    //future<bool> do_replay_commitlog(sstring volume_id, std::vector<sstring> commitlog_files);

    future<> start_httpd_service();
    future<> start_udp_service();

    timer<lowres_clock> _timer;
    void set_timer();

    // thrift server
    future<> do_stop_rpc_server();

public:
    hive_service();
    ~hive_service();

    future<> init();
    future<> stop();

    future<> test();

    void on_timer();
    future<> print_memory();

    future<> start_services();
    future<> close_services();

    inline lw_shared_ptr<db::config> get_hive_config(){ return _hive_config; }
    inline db::config& get_conf_file(){ return *_hive_config; }

    future<> replay_commitlog(sstring volume_id="");

    // thrift server API
    bool is_thrift_server_start();
    future<> start_rpc_server();
    future<> stop_rpc_server();
    future<bool> is_rpc_server_running();

}; // class hive_service

extern distributed<hive_service> _the_hive_service;

inline distributed<hive_service>& get_hive_service() {
    return _the_hive_service;
}

inline hive_service& get_local_hive_service() {
    return _the_hive_service.local();
}

inline shared_ptr<hive_service> get_local_shared_hive_service() {
    return _the_hive_service.local_shared();
}

} // namespace hive

