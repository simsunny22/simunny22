#include "hive/hive_httpd.hh"
#include "http/transformers.hh"
#include "http/exception.hh"
#include "hive/api/ping_handler.hh"
#include "hive/api/read_volume_handler.hh"
#include "hive/api/rwrite_volume_handler.hh"
#include "hive/api/create_extent_group_handler.hh"
#include "hive/api/delete_extent_group_handler.hh"
#include "hive/api/force_drain_handler.hh"
#include "hive/api/commit_metadata_handler.hh"
#include "hive/api/replay_primary_journal_handler.hh"
#include "hive/api/migrate_extent_group_handler.hh"
#include "hive/api/migrate_extent_journal_handler.hh"
#include "hive/api/get_context_handler.hh"
#include "hive/api/remove_context_handler.hh"
#include "hive/api/inject_context_handler.hh"
#include "hive/api/inject_volume_context_handler.hh"
#include "hive/api/inject_disk_context_handler.hh"
#include "hive/api/inject_extent_group_context_handler.hh"
#include "hive/api/replicate_extent_group_handler.hh"
#include "hive/api/rebuild_volume_driver_handler.hh"
#include "hive/api/stop_volume_driver_handler.hh"

#include "hive/api/object/read_object_handler.hh"
#include "hive/api/object/write_object_handler.hh"
#include "hive/api/object/write_object_handler.hh"

//#include "hive/api/log/create_log_table_handler.hh"
//#include "hive/api/log/read_log_handler.hh"
//#include "hive/api/log/crawl_extent_group_log_handler.hh"
#include "hive/api/trail/write_access_trail_handler.hh"

#include "hive/api/test_handler/db_driver_test_handler.hh"
#include "hive/api/test_handler/message_service_test_handler.hh"
#include "hive/api/test_handler/read_and_rwrite_volume_handler_test.hh"
#include "hive/api/test_handler/read_extent_group_handler.hh"
#include "hive/api/test_handler/read_volume_handler_test.hh"
#include "hive/api/test_handler/rwrite_volume_handler_test.hh"
#include "hive/api/test_handler/read_object_handler_test.hh"
#include "hive/api/test_handler/write_object_handler_test.hh"
#include "hive/api/test_handler/rwrite_volume_handler_test_need_body.hh"
#include "hive/api/test_handler/write_object_handler_test_need_body.hh"
#include "hive/api/test_handler/write_extent_group_handler.hh"
#include "hive/api/test_handler/check_force_drain_handler.hh"
#include "hive/api/test_handler/catch_exception_handler.hh"
#include "hive/api/test_handler/read_cache_handler_test.hh"
#include "hive/api/test_handler/read_and_write_cache_handler_test.hh"
#include "hive/api/test_handler/get_extent_group_md5_handler.hh"
#include "hive/api/test_handler/http_client_test_handler.hh"
#include "hive/api/test_handler/test_function_handler.hh"
#include "hive/api/test_handler/memcache_handler.hh"

namespace hive {

static std::unique_ptr<reply> exception_reply(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (std::exception& ex) {
        throw bad_param_exception(ex.what());
    }
    // We never going to get here
    return std::make_unique<reply>();
}

static void set_routes(routes& r) {
    r.register_exeption_handler(exception_reply);
    r.add(operation_type::GET, url("/hive_service/ping"), new ping_handler());
    r.add(operation_type::GET, url("/hive_service/read_volume"), new read_volume_handler());
    r.add(operation_type::POST, url("/hive_service/ping"), new ping_handler());
    r.add(operation_type::POST, url("/hive_service/create_extent_group"), new create_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/delete_extent_group"), new delete_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/rwrite_volume"), new rwrite_volume_handler());
    r.add(operation_type::POST, url("/hive_service/read_volume"), new read_volume_handler());
    r.add(operation_type::POST, url("/hive_service/replay_primary_journal"), new replay_primary_journal_handler());
    r.add(operation_type::POST, url("/hive_service/migrate_extent_group"), new migrate_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/replicate_extent_group"), new replicate_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/migrate_extent_journal"), new migrate_extent_journal_handler());
    r.add(operation_type::POST, url("/hive_service/force_drain"), new force_drain_handler());
    r.add(operation_type::POST, url("/hive_service/commit_metadata"),new commit_metadata_handler());
    r.add(operation_type::POST, url("/hive_service/get_context"), new get_context_handler());
    r.add(operation_type::POST, url("/hive_service/remove_context"), new remove_context_handler());
    r.add(operation_type::POST, url("/hive_service/inject_context"), new inject_context_handler());
    r.add(operation_type::POST, url("/hive_service/inject_volume_context"), new inject_volume_context_handler());
    r.add(operation_type::POST, url("/hive_service/inject_disk_context"), new inject_disk_context_handler());
    r.add(operation_type::POST, url("/hive_service/inject_extent_group_context"), new inject_extent_group_context_handler());
    r.add(operation_type::POST, url("/hive_service/resume_volumes_driver"), new rebuild_volume_driver_handler());
    r.add(operation_type::POST, url("/hive_service/stop_volumes_driver"), new stop_volume_driver_handler());
    
    r.add(operation_type::GET, url("/hive_service/read_volume_test"), new read_volume_handler_test());
    r.add(operation_type::GET, url("/hive_service/rwrite_volume_test"), new rwrite_volume_handler_test());
    r.add(operation_type::POST, url("/hive_service/rwrite_volume_test"), new rwrite_volume_handler_test());
    //for test
    r.add(operation_type::GET, url("/hive_service/test_function"), new test_function_handler());
    //for object
    r.add(operation_type::POST, url("/hive_service/write_object"), new write_object_handler());
    r.add(operation_type::POST, url("/hive_service/read_object"), new read_object_handler());

    //for log 
    //r.add(operation_type::POST, url("/hive_service/create_log_table"), new create_log_table_handler());
    //r.add(operation_type::POST, url("/hive_service/read_log"), new read_log_handler());
    r.add(operation_type::POST, url("/hive_service/write_access_trail"), new write_access_trail_handler());

#if 0
    //for test handler 
    r.add(operation_type::GET, url("/hive_service/test/catch_exception_test"), new catch_exception_handler());
    r.add(operation_type::GET, url("/hive_service/test/read_and_rwrite_volume_test"), new read_and_rwrite_volume_handler_test());
    r.add(operation_type::GET, url("/hive_service/test/read_volume_test"), new read_volume_handler_test());
    r.add(operation_type::GET, url("/hive_service/test/read_cache_test"), new read_cache_handler_test());
    r.add(operation_type::GET, url("/hive_service/test/read_and_write_cache_test"), new read_and_write_cache_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/db_driver_test"), new db_driver_test_handler());
    r.add(operation_type::POST, url("/hive_service/test/test_message"), new message_service_test_handler());
    r.add(operation_type::POST, url("/hive_service/test/read_and_rwrite_volume_test"), new read_and_rwrite_volume_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/read_extent_group"), new read_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/test/rwrite_volume_test"), new rwrite_volume_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/write_object_test"), new write_object_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/rwrite_volume_test_need_body"), new rwrite_volume_handler_test_need_body());
    r.add(operation_type::POST, url("/hive_service/test/write_object_test_need_body"), new write_object_handler_test_need_body());
    r.add(operation_type::POST, url("/hive_service/test/write_extent_group"), new write_extent_group_handler());
    r.add(operation_type::POST, url("/hive_service/test/read_volume_test"), new read_volume_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/read_object_test"), new read_object_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/check_force_drain"), new check_force_drain_handler());
    r.add(operation_type::POST, url("/hive_service/test/read_cache_test"), new read_cache_handler_test());
    r.add(operation_type::POST, url("/hive_service/test/get_extent_group_md5"), new get_extent_group_md5_handler());
    r.add(operation_type::POST, url("/hive_service/test/http_client_test"), new http_client_test_handler());
#endif
    r.add(operation_type::POST, url("/hive_service/test/memcache"), new memcache_handler());
}

future<> hive_httpd::init() {
    return _http_server.set_routes(set_routes);
}

}

