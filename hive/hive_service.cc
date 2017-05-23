#include "hive/hive_service.hh"
#include "http/httpd.hh"
#include "dns.hh"
#include "db/config.hh"
#include "database.hh"
#include "hive/extent_store.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/journal/journal_service.hh"
#include "hive/token_service.hh"
#include "log.hh"
#include "core/app-template.hh"
#include "utils/runtime.hh"
#include "utils/file_lock.hh"
#include "debug.hh"
#include "release.hh"
#include <cstdio>
#include <core/file.hh>
#include <sys/time.h>
#include <sys/resource.h>
#include <boost/lexical_cast.hpp>
#include "disk-error-handler.hh"
#include "checked-file-impl.hh"
#include "gms/inet_address.hh"

#include "hive/file_store.hh"
#include "hive/commitlog/commitlog_replayer.hh"

#include "hive/cache/cache.hh"
#include "hive/cache/stream_cache.hh"
#include "hive/context/context_service.hh"
#include "hive/context/inject_context_helper.hh"
#include "hive/hive_tools.hh"
#include "hive/stream/stream_session.hh"
#include "hive/migration_manager.hh"
#include "hive/messaging_service.hh"
#include "hive/migrate_forward_proxy.hh"
#include "hive/object/object_service.hh"
#include "hive/udp_service/udp_server.hh"
#include "hive/trail/trail_service.hh"
#include "hive/system_service/system_service.hh"
#include "hive/journal/journal_proxy.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/swarm/swarm_service.hh"
#include "hive/swarm/swarm_proxy.hh"
#include "hive/swarm/job_coordinator_service.hh"
#include "hive/memcache/memcached_service.hh"

#include "hive/guru/guru_kit.hh"
#include "hive/metadata_service/metadata_service.hh"
#include "hive/metadata_service/calculate/io_planner.hh"

#include "hive/thrift/server.hh"


extern void supervisor_notify(sstring msg, bool ready = false);
namespace bpo = boost::program_options;

namespace hive {

static logging::logger  logger("hive_service");
using encrypt_what = hive::messaging_service::encrypt_what;
using namespace seastar::tls;

hive::hive_status hive_service_status = hive::hive_status::IDLE;

distributed<hive_service> _the_hive_service;
using boost::lexical_cast;

static boost::filesystem::path relative_conf_dir(boost::filesystem::path path) {
    static auto conf_dir = hive::config::get_conf_dir(); // this is not gonna change in our life time
    logger.debug("relative_conf_dir: {}", conf_dir);
    return conf_dir / path;
}

static void print_log(sstring log_msg){
    logger.debug(log_msg.c_str());
    std::cout << log_msg << std::endl;
}


hive_service::hive_service(){
    hive_service_status = BOOTING;
   _hive_config = make_lw_shared<db::config>();
}

hive_service::~hive_service(){
}


future<> hive_service::stop() {
    //tododl:yellow something need to do 
    return make_ready_future<>();
}

future<> hive_service::init(){
    return init_config().then([this](){
        set_timer(); 
        return make_ready_future<>();
    });
}

future<> hive_service::init_config(){
    using namespace boost::filesystem;
    sstring standard_config_file = "/etc/vega_confs/vega_hive.yaml";

    return file_exists(standard_config_file).then([this, standard_config_file](auto is_exists){
        sstring config_file;
        if(is_exists) {
            config_file = standard_config_file; 
        } else {
            config_file = relative_conf_dir("vega_hive.yaml").string();
        }

        auto print_info = sprint("==> read_config shard:%d, config_file:%s \n", engine().cpu_id(), config_file);
        logger.debug(print_info.c_str());
        std::cout << print_info;
        return check_direct_io_support(config_file).then([this, config_file] {
            return this->_hive_config->read_from_file(config_file);
        }).handle_exception([config_file](auto ep) {
            logger.error("Could not read configuration config_file {}: {}", config_file, ep);
            return make_exception_future<>(ep);
        });
    });
}

void hive_service::set_timer(){
    auto periodic = _hive_config->periodic_print_memory_stats_in_s();
    if(periodic > 0) {
        _timer.set_callback(std::bind(&hive_service::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(3)
            , std::experimental::optional<lowres_clock::duration>{std::chrono::seconds(periodic)});
    }
}

void hive_service::on_timer(){
    print_memory();
}

future<> hive_service::print_memory(){
    logalloc::shard_tracker().print_memory_info();
    return make_ready_future<>();
}

void  hive_service::print_config_info(){
    std::cout << "-----> debug_mode: " << (_hive_config->debug_mode()?"true":"false") << std::endl;
    std::cout << "-----> cache_enabled: " << (_hive_config->hive_cache_enabled()?"true":"false") << std::endl;
    std::cout << "-----> auto_replay_commitlog: " << (_hive_config->auto_replay_commitlog()?"true":"false") << std::endl;
    std::cout << "-----> stream_chunk_limit_num_per_cpu: " << _hive_config->stream_chunk_limit_num_per_cpu() << std::endl;
    std::cout << "-----> stream_plan_limit_num_per_cpu: " << _hive_config->stream_plan_limit_num_per_cpu() << std::endl;
    std::cout << "-----> migrate_forward_extent_group_timout_in_ms: " << _hive_config->migrate_forward_extent_group_timeout_in_ms() << std::endl;
    std::cout << "-----> migrate_forward_extent_journal_timout_in_ms: " << _hive_config->migrate_forward_extent_journal_timeout_in_ms() << std::endl;

    std::cout << "-----> auto_inject_context_when_start: " << (_hive_config->auto_inject_context_when_start() ? "true":"false") << std::endl;
    std::cout << "-----> vega_log_udp_port: " << _hive_config->vega_log_udp_port() << std::endl;
    std::cout << "-----> vega_log_enable: " << _hive_config->vega_log_enable() << std::endl;

}

static future<> hive_disk_sanity(sstring path, bool developer_mode) {
    return check_direct_io_support(path).then([path, developer_mode] {
        return file_system_at(path).then([path, developer_mode] (auto fs) {
            if (fs != fs_type::xfs) {
                if (!developer_mode) {
                    logger.error("{} is not on XFS. This is a non-supported setup, and performance is expected to be very bad.\n"
                            "For better performance, placing your data on XFS-formatted directories is required."
                            " To override this error, see the developer_mode configuration option.", path);
                    throw std::runtime_error(sprint("invalid configuration: path \"%s\" on unsupported filesystem", path));
                } else {
                    logger.warn("{} is not on XFS. This is a non-supported setup, and performance is expected to be very bad.\n"
                            "For better performance, placing your data on XFS-formatted directories is strongly recommended", path);
                }
            }
        });
    });
};

future<> hive_service::touch_commitlog_directory(){
    return _dirs.touch_and_lock(_hive_config->primary_commitlog_directory()).then([this](){
        return _dirs.touch_and_lock(_hive_config->secondary_commitlog_directory()).then([this](){
            return _dirs.touch_and_lock(_hive_config->replay_position_log_directory());//rp_log
        });
    }).then([this](){
        supervisor_notify("verifying hive commitlog directories");
        std::unordered_set<sstring> directories;
        directories.insert(_hive_config->primary_commitlog_directory());
        directories.insert(_hive_config->secondary_commitlog_directory());
        directories.insert(_hive_config->replay_position_log_directory());
        return parallel_for_each(directories, [this] (sstring pathname) {
            return hive_disk_sanity(pathname, _hive_config->developer_mode());
        });
    });
}

template<typename K, typename V, typename KK, typename VV = V>
static V get_or_default(const std::unordered_map<K, V>& src, const KK& key, const VV& def = V()) {
    auto i = src.find(key);
    if (i != src.end()) {
        return i->second;
    }
    return def;
    
}
future<> hive_service::start_message_service() {
    logger.debug("[{}] start", __func__);
    return seastar::async([this](){
        //tododl:yellow this function need review
        const gms::inet_address listen(_hive_config->hive_message_service_addr());
        future<> f = make_ready_future<>();
        ::shared_ptr<server_credentials> creds;
        const auto& ssl_opts = _hive_config->server_encryption_options();
        auto ms_encrypt_what = get_or_default(ssl_opts, "internode_encryption", "none");
        auto trust_store = get_or_default(ssl_opts, "truststore");
        auto cert = get_or_default(ssl_opts, "certificate", relative_conf_dir("scylla.crt").string());
        auto key = get_or_default(ssl_opts, "keyfile", relative_conf_dir("scylla.key").string());

        encrypt_what ew = encrypt_what::none;
        if (ms_encrypt_what == "all") {
            ew = encrypt_what::all;
        } else if (ms_encrypt_what == "dc") {
            ew = encrypt_what::dc;
        } else if (ms_encrypt_what == "rack") {
            ew = encrypt_what::rack;
        } 
        if (ew != encrypt_what::none) {
            // note: credentials are immutable after this, and ok to share across shards
            creds = ::make_shared<server_credentials>(::make_shared<dh_params>(dh_params::level::MEDIUM));
            creds->set_x509_key_file(cert, key, x509_crt_format::PEM).get();
            trust_store.empty() ? creds->set_system_trust().get() :
                                     creds->set_x509_trust_file(trust_store, x509_crt_format::PEM).get();
        }

        // Init messaging_service
        auto service_port = _hive_config->hive_message_service_port();
        auto service_ssl_port = _hive_config->hive_message_service_ssl_port();
        hive::get_messaging_service().start(listen, service_port, ew, service_ssl_port, creds).get();
    }).then_wrapped([](auto f){
        try {
            f.get();
        } catch (...) {
            logger.error("error, start message service failed, exception:{}", std::current_exception());
            throw std::runtime_error("start message service failed");
        }
    });
}

future<> hive_service::auto_inject_context(){
    auto auto_inject_context_when_start = _hive_config->auto_inject_context_when_start();
    if(auto_inject_context_when_start){
        inject_context_helper context_helper;
        return context_helper.inject_context_by_config(_hive_config);
    }else{
        return make_ready_future<>(); 
    }
}

future<> hive_service::start_context_service() {
    logger.debug("[{}] start", __func__);
    auto& context_service = hive::get_context_service();
    return context_service.start(std::ref(*_hive_config)).then([this] {
        return auto_inject_context();
    });
}

future<> hive_service::start_stream_cache(){
    logger.debug("[{}] start", __func__);
    return seastar::async([this](){
        bool cache_enabled = _hive_config->hive_cache_enabled();
        if(cache_enabled){
            auto& cache = hive::get_cache();
            cache.start(std::ref(*_hive_config)).get();
            cache.invoke_on_all([](auto& shard_cache){
                return shard_cache.init(); 
            }).get(); 

            auto& stream_cache = hive::get_stream_cache();
            stream_cache.start(std::ref(*_hive_config)).get();
            
        }
    });
}

std::vector<sstring> hive_service::get_delete_files(std::vector<sstring> new_files
                                                  , std::vector<sstring> old_files){
    for(auto old_it = old_files.begin(); old_it !=  old_files.end(); old_it ++){
        auto new_it = new_files.begin();
        auto new_end = new_files.end();
        while(new_it != new_end){
            if (*old_it == *new_it){
                new_files.erase(new_it);
                break;
            }   
            new_it++;
        }   
    }   
    return new_files;
}


future<> hive_service::replay_commitlog(sstring volume_id){
    logger.debug("[{}] start, volume_id:{}", __func__, volume_id);
    if(!_hive_config->auto_replay_commitlog()){
        return make_ready_future<>(); 
    }
    return make_ready_future<>();

}

future<> hive_service::start_httpd_service(){
    logger.debug("[{}] start", __func__);
    sstring httpd_address = _hive_config->hive_httpd_address() != "" ? _hive_config->hive_httpd_address() : _hive_config->rpc_address();
    return dns::gethostbyname(httpd_address).then([this](dns::hostent e){
        auto ip = e.addresses[0].in.s_addr;
        return _hive_httpd.get_http_server().start().then([this, ip] {
            return _hive_httpd.init();
        }).then([this, ip] {
            return _hive_httpd.get_http_server().listen(ipv4_addr{ip, _hive_config->hive_httpd_port()});
        });
    });
}

//wztest
future<> hive_service::start_udp_service(){
    if(_hive_config->vega_log_enable()){
        logger.debug("[{}] start", __func__);
        auto udp_port = _hive_config->vega_log_udp_port();
        auto& dist_udp_server = get_udp_server();
        return dist_udp_server.start().then([&dist_udp_server, udp_port] () mutable {
            //engine().at_exit([&dist_udp_server] {
            //    return dist_udp_server.stop();
            //}); 
            dist_udp_server.invoke_on(0, &udp_server::start, udp_port);
            //dist_udp_server.invoke_on_all(&udp_server::start, udp_port);
        }).then([udp_port] {
            return make_ready_future<>();
        });
    }
    return make_ready_future<>();
}

future<> hive_service::test() {

    sstring build_str = "{\"extents\":[";
    build_str += "{\"extent_id\":\"extent1\",";
    build_str += "\"offset\":1048576,";
    build_str += "\"ref_count\":1,";
    build_str += "\"check_sum\":\"aaa\"}";
    build_str += "]}";

    std::string err;
    auto extents_json = hive::Json::parse(build_str, err);
    //std::cout << "test extents_json: "<< extents_json<<std::endl;
    for(auto& extent: extents_json["extents"].array_items()) {
        auto offset    = extent["offset"].uint64_value();
        std::cout<< "test  offset ="<<offset<<std::endl;
        auto ref_count = extent["ref_count"].uint64_value();
        std::cout<< "test  ref_count ="<<ref_count<<std::endl;
    }
    return make_ready_future<>();
}


future<> hive_service::start_services(){
    std::cout << "==> hive_service::start_services, start" << std::endl;
    logger.info("==> hive_service::start_services, start");
    return seastar::async([this](){
        print_config_info();
        // system service
        auto& system_service = hive::get_system_service();
        system_service.start(_hive_config).get();
        // log service
        trail_service::init(*_hive_config).get();
    
        // touch commitlog_directory
        touch_commitlog_directory().get(); 

        // message_service
        start_message_service().get();
       
        // context_service
        start_context_service().get();

        // metadata_service 
        auto& metadata_service = hive::get_metadata_service();
        metadata_service.start().get();

        // volume_service 
        auto& volume_service = hive::get_volume_service();
        volume_service.start().get();

        // stream_service 
        auto& stream_service = hive::get_stream_service();
        stream_service.start(std::ref(*_hive_config)).get();

        // journal_service 
        auto& journal_service = hive::get_journal_service();
        auto total_segments_token = _hive_config->total_segments_token();
        journal_service.start(std::ref(*_hive_config), total_segments_token).get(); 
    
        // journal_proxy 
        //auto& journal_proxy = hive::get_journal_proxy();
        //journal_proxy.start().get();

        // journal_proxy 
        auto& journal_proxy = hive::get_journal_proxy();
        journal_proxy.start().get();

        // extent_store
        auto& extent_store = hive::get_extent_store();
        extent_store.start().get();

        // extent_store_proxy
        auto& extent_store_proxy = hive::get_extent_store_proxy();
        extent_store_proxy.start(std::ref(*_hive_config), std::ref(extent_store)).get();

        // stream_cache
        start_stream_cache().get();

        // token_service 
        auto& token_service = hive::get_token_service();
        token_service.start(_hive_config).get();

        // replay_commitlog 
        replay_commitlog().get();

        // streaming service for migrate 
        auto send_chunk_limit = _hive_config->stream_chunk_limit_num_per_cpu();
        hive::stream_session::init_streaming_service(send_chunk_limit).get();

        // migration_manager //tododl:green need change name to migration_service??
        auto& migration_manager = hive::get_migration_manager();
        migration_manager.start(_hive_config).get(); 

        // migrate_forward_proxy 
        auto& migrate_forward_proxy = hive::get_migrate_forward_proxy();
        migrate_forward_proxy.start(*_hive_config).get(); 

        // object_service
        auto& object_service = hive::get_object_service();
        object_service.start().get(); 

        // swarm service
        auto& swarm_service = hive::get_swarm_service();
        swarm_service.start().get();

        // swarm proxy
        auto& swarm_proxy = hive::get_swarm_proxy();
        swarm_proxy.start().get();

        // cache service
        auto& memcache_service = memcache::get_memcached_service();
        memcache_service.start().get();

        // job coordinator
        auto& job_coordinator = hive::get_job_coordinator_service();
        job_coordinator.start().get();

        // httpd_service
        start_httpd_service().get();

        // udp_service
        start_udp_service().get();

        // rpc service
        start_rpc_server().get();

//#endif

    }).then_wrapped([this](auto f){
        try{
            f.get();
            std::cout << "==> hive_service::start_services, done" << std::endl;
            logger.info("==> hive_service::start_services, done");
            hive_service_status = NORMAL;
            return make_ready_future<>();
        }catch(...) {
            std::ostringstream out;
            out << "[start_services] error, exception:" << std::current_exception() << std::endl;
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
} //start_services()

future<> hive_service::close_services(){
    print_log("\n\n==> hive_service close_services start");
    if(hive_service_status != NORMAL) {
        print_log("--> hive_service close_services done with NORMAL state");
        return make_ready_future<>(); 
    }

    return seastar::async([this] {
        //stop rpc server
	      stop_rpc_server().get();

        //stop httpd server
        print_log("--> stop hive_http_server begin");
        _hive_httpd.get_http_server().stop().get();
        print_log("--> stop hive_http_server end");

        //////////////////
        // extent_journal
        //////////////////
        print_log("--> stop drain_manager begin...");
        auto& stream_service = hive::get_stream_service();
        stream_service.invoke_on_all([](auto& shard_stream_service){
            return shard_stream_service.stop_drain_manager();
        }).get();
        print_log("--> stop drain_manager end");

         //stop journal_service  --- commitlog
        print_log("--> stop journal_service begin...");
        hive::get_journal_service().stop().get();
        print_log("--> stop journal_service end");

        //////////////////
        // extent_store        
        //////////////////
        print_log("--> stop cache begin...");
        hive::get_cache().stop().get();
        print_log("--> stop cache end");

        hive_service_status = IDLE;
        print_log("--> stop hive_messaging_service begin...");
        hive::get_messaging_service().stop().get();
        print_log("--> stop hive_messaging_service end");

        print_log("==> hive_service close_services done\n\n");
    }).then_wrapped([](auto f){
        try {
            f.get(); 
            return make_ready_future<>();
        } catch (...) {
            std::ostringstream out;
            out << "[close_services] error, exception: " << std::current_exception();
            sstring error_info = out.str();
            std::cout << "error, " << error_info << std::endl;
            logger.error(error_info.c_str());
            return make_exception_future<>(error_info);
        }
    });
} //close_services()

future<> hive_service::do_stop_rpc_server() {
    auto tserver = _thrift_server;
    _thrift_server = {};
    if (tserver) {
        return tserver->stop().then([tserver] {
            auto& hs = get_local_hive_service();
            hs._thrift_server_start = false;
            logger.info("Hive thrift server stopped");
        });
    }
    return make_ready_future<>();
}

future<> hive_service::start_rpc_server() {
    return run_with_api_lock(sstring("start_rpc_server"), [] (hive_service& hs) {
        if (hs._thrift_server) {
            return make_ready_future<>();
        }

        auto tserver = make_shared<distributed<hive_thrift_server>>();
        hs._thrift_server = tserver;

        //
        auto cfg  = hs.get_hive_config();
        auto port = cfg->hive_rpc_port();
        auto addr = cfg->hive_rpc_address();
        auto keepalive = cfg->hive_rpc_keepalive();
        return dns::gethostbyname(addr).then([&hs, tserver, addr, port, keepalive] (dns::hostent e) {
            auto ip = e.addresses[0].in.s_addr;
            return tserver->start().then([tserver, port, addr, ip, keepalive] {
                // do not stop anything
                return tserver->invoke_on_all(&hive_thrift_server::listen, ipv4_addr{ip, port}, keepalive);
            });
        }).then([&hs, addr, port] {
            hs._thrift_server_start = true;
            std::cout << "Hive Thrift server listening on " << addr << ":" << port << " ..." << std::endl;
            logger.info("Hive Thrift server listening on {}:{} ...", addr, port);
        });
    });
}

future<> hive_service::stop_rpc_server() {
    return run_with_api_lock(sstring("stop_rpc_server"), [] (hive_service& hs) {
        return hs.do_stop_rpc_server();
    });
}

future<bool> hive_service::is_rpc_server_running() {
    return run_with_no_api_lock([] (hive_service& hs) {
        return bool(hs._thrift_server);
    });
}

bool hive_service::is_thrift_server_start() {
    return _thrift_server_start;
}

} //namespace hive
