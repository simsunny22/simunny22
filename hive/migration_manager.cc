#include "hive/migration_manager.hh"
#include "log.hh"
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "hive/context/context_service.hh"
#include "hive/hive_config.hh"
#include "hive/extent_store.hh"
#include "hive/file_store.hh"
#include "hive/stream/stream_plan.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/stream_state.hh"
#include "hive/hive_service.hh"

#include "hive/store/extent_store_proxy.hh"

namespace hive {

static logging::logger logger("migration_manager");

distributed<migration_manager> _the_migration_manager;
using namespace exceptions;

std::ostream& operator<<(std::ostream& out, const migrate_result_entry& migrate_result) {
    return out << "{\"migrate_result_entry\":{"
        << "\"volume_id\":\"" << migrate_result.volume_id
        << "\", \"extent_group_id\":\"" << migrate_result.extent_group_id
        << "\", \"src_disk_id\":\"" << migrate_result.src_disk_id 
        << "\", \"dst_disk_id\":\"" << migrate_result.dst_disk_id 
        << "\", \"src_file_path\":\"" << migrate_result.src_file_path 
        << "\", \"dst_file_path\":\"" << migrate_result.dst_file_path 
        << "\", \"copy_file_success\":" << migrate_result.copy_file_success 
        << ", \"delete_src_file_success\":" << migrate_result.delete_src_file_success 
        << ", \"message\":\"" << migrate_result.delete_src_file_success 
        << "\"}}";
}

static inline bool is_me(gms::inet_address from){
    return from == utils::fb_utilities::get_broadcast_address();
}

migration_manager::migration_manager(lw_shared_ptr<db::config> config) 
: _config(config)
, _stream_plan_limit_num(config->stream_plan_limit_num_per_cpu()) 
{
    logger.info("constructor start, cpu_id:{}", engine().cpu_id());


    auto periodic = _config->periodic_print_migrate_stats_in_s();
    if(periodic > 0) {
        _timer.set_callback(std::bind(&migration_manager::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(1)
            , std::experimental::optional<lowres_clock::duration>{std::chrono::seconds(periodic)});
    }
}

migration_manager::~migration_manager() {}

void migration_manager::on_timer(){
    print_stats_info();
}

void migration_manager::print_stats_info() {
    logger.error("shard:{}, periodic_print_migrate_stats_in_s, ==> total:{}, successful:{}, failed:{}",
        engine().cpu_id(), _stats.total, _stats.successful, _stats.failed);
}

future<> migration_manager::stop() {
    return make_ready_future<>();
}

unsigned migration_manager::shard_of(const sstring extent_group_id){
    auto token = dht::global_partitioner().get_token(bytes_view(to_bytes(extent_group_id)));
    return dht::shard_of(token);
}

sstring build_description(migrate_params_entry migrate_params){
    
    sstring str_offset = to_sstring(migrate_params.offset);
    sstring str_length = to_sstring(migrate_params.length);
    sstring description = "migrate_extent_group{volume_id:"
    + migrate_params.volume_id
    + ",extent_group_id:"
    + migrate_params.extent_group_id
    + ",src_disk_id:"
    + migrate_params.src_disk_id
    + ",dst_disk_id:"
    + migrate_params.dst_disk_id
    + ",offset:"
    + str_offset
    + ",length:"
    + str_length 
    + "}";

   return description;
}

sstring build_description_ex(migrate_params_entry migrate_params){
    sstring description = "migrate_extent_journal{volume_id:"
    + migrate_params.volume_id
    + ", src_node_ip:"
    + migrate_params.src_node_ip
    + ", dst_node_ip:"
    + migrate_params.dst_node_ip
    + "}";

   return description;
}

future<disk_context_entry> get_disk_context(sstring disk_id){
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id);
}

sstring get_file_path(disk_context_entry& disk_context, sstring extent_group_id){
    sstring file_path = hive_tools::build_extent_group_file_path(
        disk_context.get_mount_path(),
        extent_group_id);
    return file_path;
}

future<> migration_manager::stream_extent_group(migrate_params_entry migrate_params) {
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);
    return get_disk_context(migrate_params.dst_disk_id).then([this, migrate_params]
            (auto dst_disk_context)mutable{
        return get_disk_context(migrate_params.src_disk_id).then([this, migrate_params, dst_disk_context]
                (auto src_disk_context)mutable{
            sstring src_file_path = get_file_path(src_disk_context, migrate_params.extent_group_id);
            sstring dst_file_path = get_file_path(dst_disk_context, migrate_params.extent_group_id);
            gms::inet_address peer_addr(dst_disk_context.get_ip());
            auto my_address = utils::fb_utilities::get_broadcast_address();

            if(peer_addr == my_address){
                return this->stream_extent_group_to_self(migrate_params.extent_group_id, src_file_path, dst_file_path);            
            }else{
                migrate_params.src_file_path = src_file_path;
                migrate_params.dst_file_path = dst_file_path;
                return this->stream_extent_group_to_remote(migrate_params, peer_addr);
            }
        });
    });
}

future<> migration_manager::stream_extent_group_to_self(sstring extent_group_id
                                                      , sstring src_file
                                                      , sstring dst_file){
    logger.debug("{} start, extent_group_id:{}, src_file:{}, dst_file:{}"
        , __func__, extent_group_id, src_file, dst_file);
   
    return hive::copy_file(src_file, dst_file).then([src_file, dst_file](){
        logger.debug("stream_extent_group_to_self done, src_file:{}, dst_file:{}", src_file, dst_file);
        return make_ready_future<>();
    });

    //auto& local_extent_store = hive::get_local_extent_store();
    //auto shard_id = local_extent_store.shard_of(extent_group_id);
    //auto& extent_store = hive::get_extent_store();
    //return extent_store.invoke_on(shard_id, [this, extent_group_id, src_file, dst_file]
    //        (auto& shard_extent_store){
    //    return shard_extent_store.with_monad(extent_group_id, [src_file, dst_file](){
    //        return hive::copy_file(src_file, dst_file).then([src_file, dst_file](){
    //            logger.debug("stream_extent_group_to_self done, src_file:{}, dst_file:{}", src_file, dst_file);
    //            return make_ready_future<>();
    //        });
    //    });
    //});
}

future<> migration_manager::stream_extent_group_to_remote(migrate_params_entry migrate_params
                                                        , gms::inet_address peer_addr){
    logger.debug("{} start, migrate_params:{}", __func__, migrate_params);
    sstring description = build_description(migrate_params);
    auto transfer_plan = make_lw_shared<stream_plan>(description);
    transfer_plan->transfer_files(peer_addr, migrate_params);

    return transfer_plan->execute().then_wrapped([this, migrate_params](auto&& f){
       try{
           stream_state state = f.get0();
           //tododl:yellow need to handle this state?
       }catch(...){
           std::ostringstream out;
           out << "error while stream_extent_group_to_remote, exception:" << std::current_exception();
           sstring error_info = out.str();
           logger.error(error_info.c_str()); 
           return make_exception_future<>(std::runtime_error(error_info));
       }
       logger.debug("stream_extent_group_to_remote, done, migrate_params:{}", migrate_params);
       return make_ready_future<>();
    });
}

sstring migration_manager::build_ids_str(std::set<sstring> set_ids){
    sstring ids_str = "";
    int32_t count = 0;
    for(auto id : set_ids){
        ids_str += (0==count++) ? id : (":" + id);
    }
    return ids_str;
}

future<bool> ensure_driver_node_is_me(sstring volume_id, sstring driver_node_ip){
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.get_or_pull_volume_context(volume_id).then(
            [driver_node_ip](auto volume_context){
        auto driver_node = volume_context.get_driver_node();
        if(driver_node.get_ip() == driver_node_ip){
            return make_ready_future<bool>(true);
        }else{
            return make_ready_future<bool>(false);
        }
    });
}

future<migrate_result_entry> migration_manager::migrate_extent_group(migrate_params_entry migrate_params) {
    logger.debug("[{}] start, migrate_params:{}, current_limit_num:{}", __func__, migrate_params, _stream_plan_limit_num.current());

    //1 check drive node is me
    auto volume_id      = migrate_params.volume_id;
    auto driver_node_ip = migrate_params.driver_node_ip;
    return ensure_driver_node_is_me(volume_id, driver_node_ip).then([migrate_params](auto is_me){
        if(!is_me){
            std::ostringstream out;
            out << "[migrate_extent_group] ensure driver node is me failed";
            out << ", migrate_params:" << migrate_params;
            sstring error_info = out.str(); 
            logger.error(error_info.c_str());
            throw std::runtime_error(error_info);
        }
        return make_ready_future<>();
    }).then([this, migrate_params](){
        //2 ensure disk_ids context exist
        std::set<sstring> set_ids;
        set_ids.insert(migrate_params.src_disk_id);
        set_ids.insert(migrate_params.dst_disk_id);
        auto& context_service = hive::get_local_context_service();
        return context_service.ensure_disk_context(set_ids).then([this, set_ids](auto exist)mutable{
            if(!exist) {
                std::ostringstream out;
                out << "[migrate_extent_group] ensure disk context failed, disk_ids:" << this->build_ids_str(set_ids);
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);
            }
            return make_ready_future<>();
        });
    }).then([this, migrate_params](){
        //3 migrate through extent_store_proxy 
#if 0
        return _stream_plan_limit_num.wait().then([this, migrate_params](){
            auto& local_extent_store_proxy = hive::get_local_extent_store_proxy();
            auto shard_id = local_extent_store_proxy.shard_of(migrate_params.extent_group_id);
            auto& extent_store_proxy = hive::get_extent_store_proxy();
            return extent_store_proxy.invoke_on(shard_id, [this, migrate_params](auto& shard_proxy)mutable{
                lw_shared_ptr<migrate_result_entry> result_ptr = make_lw_shared<migrate_result_entry>(
                    migrate_params.volume_id
                    , migrate_params.extent_group_id
                    , migrate_params.src_disk_id
                    , migrate_params.dst_disk_id
                    , migrate_params.src_file_path
                    , migrate_params.dst_file_path);
                logger.debug("[migrate_extent_group] will migrate_with_monad");
                return this->migrate_with_monad(shard_proxy, migrate_params, result_ptr);
            });
        }).finally([this](){
            _stream_plan_limit_num.signal();  
        });
#endif
        return _stream_plan_limit_num.wait().then([this, migrate_params](){
            lw_shared_ptr<migrate_result_entry> result_ptr = make_lw_shared<migrate_result_entry>(
                migrate_params.volume_id
                , migrate_params.extent_group_id
                , migrate_params.src_disk_id
                , migrate_params.dst_disk_id
                , migrate_params.src_file_path
                , migrate_params.dst_file_path);

            logger.debug("[migrate_extent_group] will migrate_with_monad");
            auto& local_proxy = hive::get_local_extent_store_proxy();
            return this->migrate_with_monad(local_proxy, migrate_params, result_ptr);
        }).finally([this](){
            _stream_plan_limit_num.signal();  
        });

    });
}

static future<gms::inet_address> get_disk_address(sstring disk_id){
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then([](auto disk_context){
        auto disk_ip = disk_context.get_ip();
        return make_ready_future<gms::inet_address>(gms::inet_address(disk_ip));
    });
}

future<migrate_result_entry> migration_manager::migrate_with_monad(extent_store_proxy& shard_proxy
                          , migrate_params_entry migrate_params
                          , lw_shared_ptr<migrate_result_entry> result_ptr){
//TODO:red use seastar::async
//TODO:monad move to extent_store_proxy

    //of extent_store_proxy
    auto extent_group_id = migrate_params.extent_group_id;
    logger.debug("[{}] start, monad_waiters_count:{}", __func__, shard_proxy.get_monad_waiters_count(extent_group_id));
    logger.debug("[migrate_with_monad] inter monad, migrate_params:{}", migrate_params);
    //0 delete extent_group_context
    auto migrate_test = get_local_hive_service().get_hive_config()->migrate_extent_group_test();
    extent_group_context_entry old_context_entry;
    if(migrate_test){
       old_context_entry = get_extent_group_context(extent_group_id); //tododl:test
    }

    _stats.total++;
    return delete_extent_group_context(extent_group_id).then(
            [this, &shard_proxy, migrate_test, migrate_params, result_ptr](){

        return get_disk_address(migrate_params.src_disk_id).then([&shard_proxy, migrate_params, result_ptr]
                (auto disk_address){
            std::unordered_set<gms::inet_address> targets = {disk_address};
            smd_migrate_extent_group smd_data(
                migrate_params.driver_node_ip
                , migrate_params.container_name
                , migrate_params.intent_id
                , migrate_params.volume_id
                , migrate_params.extent_group_id
                , migrate_params.src_disk_id
                , migrate_params.dst_disk_id
                , migrate_params.offset
                , migrate_params.length
            );

            //1 migrate_extent_group
            return shard_proxy.migrate_extent_group(std::move(smd_data), targets).then(
                    [migrate_params, result_ptr, targets](auto success){
                if(success == targets){
                    logger.debug("[migrate_with_monad] copy file done, migrate_params:{}", migrate_params);
                    result_ptr->copy_file_success = true;
                    return make_ready_future<>();
                }else{
                    auto error_info = sprint("migrate_extent_group failed, targets.size:{}, success.size:{}"
                       , targets.size(), success.size()); 
                    logger.error(error_info.c_str());
                    throw std::runtime_error(error_info);
                }
            });
        }).then([this,&shard_proxy, migrate_test, migrate_params](){
            //2 commit to metadata for copy file
            if(migrate_test){
                return make_ready_future<>(); 
            }else {
                auto metadata_url = shard_proxy.get_config()->pithos_server_url();
                return commit_to_metadata_for_copy(metadata_url, migrate_params);
            }
        });
    }).then([this, &shard_proxy, migrate_test, migrate_params, result_ptr](){
        //3 delete extent_group_file
        return get_disk_address(migrate_params.src_disk_id).then([&shard_proxy, migrate_params, result_ptr]
                (auto disk_address){
            std::unordered_set<gms::inet_address> targets = {disk_address};
            smd_delete_extent_group smd_data(
                migrate_params.extent_group_id
                , {migrate_params.src_disk_id} 
            );

            return shard_proxy.delete_extent_group(std::move(smd_data), targets)
                    .then([migrate_params, result_ptr, targets](auto success)mutable{
                if(success == targets){
                    logger.debug("[migrate_with_monad] delete file done, migrate_params:{}", migrate_params);
                    result_ptr->delete_src_file_success = true;
                    return make_ready_future<>();
                }else{
                    auto error_info = sprint("delete_extent_group failed, targets.size:{}, success.size:{}"
                       , targets.size(), success.size()); 
                    logger.error(error_info.c_str());
                    throw std::runtime_error(error_info);
                }

                logger.debug("[migrate_with_monad] delete file done, migrate_params:{}", migrate_params);
                result_ptr->delete_src_file_success = true;
                return make_ready_future<>();
            });
        }).then([this, &shard_proxy, migrate_test, migrate_params](){
            //4 commit to metadata for delete file  
            if(migrate_test){
                return make_ready_future<>(); 
            }else{
                auto metadata_url = shard_proxy.get_config()->pithos_server_url();
                return commit_to_metadata_for_delete(metadata_url, migrate_params);
            }
        });
    }).then_wrapped([this, &shard_proxy, migrate_test, migrate_params, old_context_entry, result_ptr](auto f)mutable{
        //handle exception
        try{
            f.get();
            _stats.successful++;
            logger.debug("[migrate_with_monad] done, migrate_params:{}", migrate_params);
            if(migrate_test){
                return this->reset_extent_group_context(old_context_entry, migrate_params).then([result_ptr](){
                    return make_ready_future<migrate_result_entry>(*result_ptr);
                });
            }else{
                return make_ready_future<migrate_result_entry>(*result_ptr);
            }
        }catch(...){
            _stats.failed++;
            std::ostringstream out;
            out << "[migrate_with_monad] error, catch exception:" << std::current_exception(); 
            sstring error_info = out.str();
            logger.error(error_info.c_str());
            if(migrate_test){
                return this->reset_extent_group_context(old_context_entry).then([error_info](){
                    return make_exception_future<migrate_result_entry>(std::runtime_error(error_info));
                });
            }else{
                return make_exception_future<migrate_result_entry>(std::runtime_error(error_info));
            }
        }
    });
}

future<> migration_manager::delete_extent_group_context(sstring extent_group_id){
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.remove_on_every_shard(extent_group_id);
}

//<--for test hive only
extent_group_context_entry migration_manager::get_extent_group_context(sstring extent_group_id){
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.get_extent_group_context(extent_group_id);
}

future<> migration_manager::reset_extent_group_context(extent_group_context_entry old_context_entry
                                                     , migrate_params_entry migrate_params){

    sstring src_disk_id = migrate_params.src_disk_id;
    sstring dst_disk_id = migrate_params.dst_disk_id;
    sstring disk_ids = old_context_entry.get_disk_ids();

    std::vector<sstring> vt_disk_ids = hive_tools::split_to_vector(disk_ids, ":");
    sstring new_disk_ids = "";
    for(size_t i = 0; i < vt_disk_ids.size(); i++){
        sstring old_disk_id = vt_disk_ids.at(i);
        sstring new_disk_id = (old_disk_id == src_disk_id) ? dst_disk_id : old_disk_id;
        if(0 == i){
            new_disk_ids += new_disk_id; 
        }else{
            new_disk_ids += ":" + new_disk_id;
        }
    }

    auto extent_group_id = old_context_entry.get_extent_group_id();
    auto vclock = old_context_entry.get_vclock();
    extent_group_context_entry new_context_entry(extent_group_id
                                               , new_disk_ids
                                               , vclock); 

    logger.debug("[{}] for test mode context info 1:[extent_group_id:{}, disk_ids:{}, vclock:{}]"
        , __func__, new_disk_ids, vclock);
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.set_extent_group_context(new_context_entry);
}

future<> migration_manager::reset_extent_group_context(extent_group_context_entry context_entry){
    logger.debug("[{}] for test mode context info 2:[extent_group_id:{}, disk_ids:{}, vclock:{}]"
        , __func__, context_entry.get_extent_group_id(), context_entry.get_disk_ids(), context_entry.get_vclock());
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.set_extent_group_context(context_entry);
}
//for test-->

future<> migration_manager::commit_to_metadata_for_copy(sstring metadata_url
                                                      , migrate_params_entry migrate_params){
    sstring metadata_uri = metadata_url + "/v1/actions/commit_copy_extent_group";
    hive::Json commit_json = hive::Json::object {
        {"extent_group_id", migrate_params.extent_group_id.c_str()},
        {"src_disk_id",     migrate_params.src_disk_id.c_str()},
        {"dst_disk_id",     migrate_params.dst_disk_id.c_str()},
        {"container_name",  migrate_params.container_name.c_str()},
        {"intent_id",       migrate_params.intent_id.c_str()},
    };
    auto commit_body = commit_json.dump();
  
    logger.debug("{} start, uri:{}, body:{}", __func__, metadata_uri, commit_body);
    hive::HttpClient client;
    return do_with(std::move(client), [metadata_uri, commit_body](auto& client){
        return client.post(metadata_uri, commit_body).then_wrapped([metadata_uri, commit_body](auto f){
            try {
                auto response = f.get0(); 
                if(!response.is_success()){
                    auto error_info = sprint("http response error code:{}", response.status()); 
                    throw std::runtime_error(error_info);
                }
                return make_ready_future<>();
            } catch (...) {
                std::ostringstream out;
                out << "commit_to_metadata_for_copy failed";
                out << ", uri:" << metadata_uri;
                out << ", body:" << commit_body;
                out << ", exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);    
            }

        });
    });
}

future<> migration_manager::commit_to_metadata_for_delete(sstring metadata_url
                                                      , migrate_params_entry migrate_params){
    sstring metadata_uri = metadata_url + "/v1/actions/commit_delete_extent_group";
    hive::Json commit_json = hive::Json::object {
        {"extent_group_id", migrate_params.extent_group_id.c_str()},
        {"disk_id",         migrate_params.src_disk_id.c_str()},
        {"container_name",  migrate_params.container_name.c_str()},
        {"intent_id",       migrate_params.intent_id.c_str()},
    };
    auto commit_body = commit_json.dump();

    logger.debug("{} start, uri:{}, body:{}", __func__, metadata_uri, commit_body);
    hive::HttpClient client;
    return do_with(std::move(client), [metadata_uri, commit_body](auto& client){
        return client.post(metadata_uri, commit_body).then_wrapped([metadata_uri, commit_body](auto f){
            try {
                auto response = f.get0(); 
                if(!response.is_success()){
                    auto error_info = sprint("http response error code:{}", response.status()); 
                    throw std::runtime_error(error_info);
                }
                return make_ready_future<>();
            } catch (...) {
                std::ostringstream out;
                out << "commit_to_metadata_for_delete failed";
                out << ", uri:" << metadata_uri;
                out << ", body:" << commit_body;
                out << ", exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);    
            }
        });
    });
}

future<> migration_manager::commit_to_metadata_for_replicate(std::map<sstring, sstring> replicate_params){

    auto metadata_url = get_local_hive_service().get_hive_config()->pithos_server_url();
    sstring metadata_uri = metadata_url + "/v1/actions/commit_replicate_extent_group";
    hive::Json commit_json = hive::Json::object {
        {"extent_group_id", replicate_params["extent_group_id"].c_str()},
        {"src_disk_id",     replicate_params["src_disk_id"].c_str()},
        {"dst_disk_id",     replicate_params["dst_disk_id"].c_str()},
        {"container_name",  replicate_params["container_name"].c_str()},
        {"repair_disk_id",  replicate_params["repair_disk_id"].c_str()},
    };
    auto commit_body = commit_json.dump();
  
    logger.debug("{} start, uri:{}, body:{}", __func__, metadata_uri, commit_body);
    hive::HttpClient client;
    return do_with(std::move(client), [metadata_uri, commit_body](auto& client){
        return client.post(metadata_uri, commit_body).then_wrapped([metadata_uri, commit_body](auto f){
            try {
                auto response = f.get0(); 
                if(!response.is_success()){
                    auto error_info = sprint("http response error code:{}", response.status()); 
                    throw std::runtime_error(error_info);
                }
                return make_ready_future<>();
            } catch (...) {
                std::ostringstream out;
                out << "commit_to_metadata_for_replicate failed";
                out << ", uri:" << metadata_uri;
                out << ", body:" << commit_body;
                out << ", exception:" << std::current_exception();
                sstring error_info = out.str();
                logger.error(error_info.c_str());
                throw std::runtime_error(error_info);    
            }
        });
    });
}

////////////////////////////////
// migrate extent journal
///////////////////////////////

future<> migration_manager::touch_commitlog_file(migrate_scene scene, sstring commitlog_file_name, size_t truncate_size){
    sstring commitlog_directory = "";
    if(migrate_scene::PRIMARY_TO_PRIMARY == scene
        || migrate_scene::SECONDARY_TO_PRIMARY == scene){
        commitlog_directory = _config->primary_commitlog_directory(); 
    }else if(migrate_scene::PRIMARY_TO_SECONDARY == scene
        || migrate_scene::SECONDARY_TO_SECONDARY == scene){
        commitlog_directory = _config->secondary_commitlog_directory(); 
    }else {
        auto error_info = "[touch_commitlog_file]error, unknown commitlog file";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info));
    }

    sstring file_path = commitlog_directory + hive_config::file_seperator + commitlog_file_name;
    logger.debug("[{}] start, file_path:{}", __func__, file_path);
    return hive::create_file(file_path, truncate_size).finally([file_path]{
        logger.debug("[{}] done, file_path:{}", __func__, file_path);
    }); 
}

future<> migration_manager::write_extent_journal_chunk(migrate_chunk chunk){
    logger.debug("[{}] start, chunk:{}", __func__, chunk);
    sstring commitlog_directory = "";
    if(migrate_scene::PRIMARY_TO_PRIMARY == chunk.scene
     ||migrate_scene::SECONDARY_TO_PRIMARY== chunk.scene){
        commitlog_directory = _config->primary_commitlog_directory(); 
    }else if(migrate_scene::SECONDARY_TO_SECONDARY == chunk.scene
          || migrate_scene::PRIMARY_TO_SECONDARY == chunk.scene){
        commitlog_directory = _config->secondary_commitlog_directory(); 
    }else{
        auto error_info = "[write_extent_journal_chunk] error, unknown scene type";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info)); 
    }
  
    sstring file_path = commitlog_directory + hive_config::file_seperator + chunk.commitlog_file_name;
    logger.debug("[{}] start, file_name:{}, file_path:{}, offset:{}, length:{}"
        , __func__, chunk.commitlog_file_name, file_path, chunk.offset, chunk.length);

    return do_with(std::move(chunk.data), [file_path, offset=chunk.offset, length=chunk.length]
            (auto& data){
        return hive::write_file(file_path, offset, length, data);
    });
}


future<std::vector<sstring>> migration_manager::get_commitlog_files(migrate_params_entry migrate_params){
    sstring commitlog_directory = ""; 
    if( migrate_scene::PRIMARY_TO_PRIMARY == migrate_params.scene
     || migrate_scene::PRIMARY_TO_SECONDARY == migrate_params.scene){
        commitlog_directory = _config->primary_commitlog_directory(); 
    }else if(migrate_scene::SECONDARY_TO_SECONDARY == migrate_params.scene
          || migrate_scene::SECONDARY_TO_PRIMARY == migrate_params.scene){
        commitlog_directory = _config->secondary_commitlog_directory(); 
    }else{
        auto error_info = "[get_commitlog_files] error, unknow migrate scene"; 
        logger.error(error_info);
        return make_exception_future<std::vector<sstring>>(std::runtime_error(error_info));
    }
    
    auto volume_id = migrate_params.volume_id;
    return hive_tools::list_files(commitlog_directory).then([volume_id](auto commitlog_files){
        if(!volume_id.empty()){ 
            //filter commitlog files by volume_id
            auto i = std::remove_if(commitlog_files.begin(), commitlog_files.end(), [volume_id](auto commitlog_file){
                auto parsed_name = hive_tools::parse_commitlog_file_name(commitlog_file);
                if(parsed_name.volume_id != volume_id){
                    return true;
                }
                return false;
            });
            if(i != commitlog_files.end()){
                commitlog_files.erase(i, commitlog_files.end());
            }
        }

        return make_ready_future<std::vector<sstring>>(std::move(commitlog_files));
    });
}

future<> migration_manager::add_commitlog_files_to_plan(lw_shared_ptr<stream_plan> plan
                                                      , std::vector<sstring> commitlog_files
                                                      , migrate_params_entry migrate_params){

    std::vector<future<>> futures;
    gms::inet_address peer_addr(migrate_params.dst_node_ip);
    for(auto file : commitlog_files){
        auto pos = file.find_last_of('/');
        sstring name = file.substr(pos+1);
        migrate_params.commitlog_file_name = name;
        migrate_params.commitlog_file_path = file;
        migrate_params.offset = 0;

        auto fut = file_size(file).then([migrate_params, plan, peer_addr](uint64_t size)mutable{
            migrate_params.length = size; 
            plan->transfer_files(peer_addr, migrate_params);
        });

        futures.push_back(std::move(fut)); 
    }

    return when_all(futures.begin(), futures.end()).then([](auto ret){
        try{
            for(auto& f : ret) {
                f.get(); 
            }
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[add_commitlog_files_to_plan] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

//only one plan have multiple files, easy to timeout and full of network bandwith
future<> migration_manager::migrate_extent_journal_concurrent(migrate_params_entry migrate_params){
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);
    
    if(migrate_params.type != migrate_type::MIGRATE_EXTENT_JOURNAL){
        auto error_info = "[migrate_extent_journal_concurrent] error, migrate_params type is not MIGRATE_EXTENT_JOURNAL";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info)); 
    }

    // 1. collect commitlog files
    return get_commitlog_files(migrate_params).then([this, migrate_params](auto commitlog_files){
        sstring description = build_description_ex(migrate_params);
        auto transfer_plan = make_lw_shared<stream_plan>(description);
        // 2. add commitlog file to plan
        return this->add_commitlog_files_to_plan(transfer_plan, commitlog_files, migrate_params).then(
                [transfer_plan, migrate_params](){
            // 3. execute plan
            return transfer_plan->execute().then_wrapped([migrate_params](auto&& f){
               try{
                   stream_state state = f.get0();
                   //tododl:yellow need to handle this state?
               }catch(...){
                   std::ostringstream out;
                   out << "error while migrate_extent_journal_concurrent, exception:" << std::current_exception();
                   sstring error_info = out.str();
                   logger.error(error_info.c_str()); 
                   return make_exception_future<>(std::runtime_error(error_info));
               }
               logger.debug("[migrate_extent_journal_concurrent] done, migrate_params:{}", migrate_params);
               return make_ready_future<>();
            });
        });
    });
}

//each plan has one commitlog file and run in order
future<> migration_manager::migrate_extent_journal_ordered(migrate_params_entry migrate_params){
    logger.debug("[{}] start, migrate_params:{}", __func__, migrate_params);
    
    if(migrate_params.type != migrate_type::MIGRATE_EXTENT_JOURNAL){
        auto error_info = "[migrate_extent_journal_ordered] error, migrate_params type is not MIGRATE_EXTENT_JOURNAL";
        logger.error(error_info);
        return make_exception_future<>(std::runtime_error(error_info)); 
    }

    // 1. collect commitlog files
    return get_commitlog_files(migrate_params).then([this, migrate_params](auto commitlog_files)mutable{
        return do_with(std::move(commitlog_files), [this, migrate_params](auto& commitlog_files)mutable{
            // 2. for each execute plans, each plan only have one file 
            return do_for_each(commitlog_files.begin(), commitlog_files.end(), [this, migrate_params](auto file)mutable{
                logger.debug("[migrate_extent_journal_ordered] start trans file:{}", file);
                auto pos = file.find_last_of('/');
                sstring name = file.substr(pos+1);
                migrate_params.commitlog_file_name = name;
                migrate_params.commitlog_file_path = file;
                migrate_params.offset = 0;

                return file_size(file).then([migrate_params, file](uint64_t size)mutable{
                    migrate_params.length = size; 
                    sstring description = build_description_ex(migrate_params);
                    gms::inet_address peer_addr(migrate_params.dst_node_ip);
                    auto plan = make_lw_shared<stream_plan>(description);
                    plan->transfer_files(peer_addr, migrate_params);
                    return plan->execute().then_wrapped([migrate_params,file](auto&& f){
                        try{
                            //tododl:yellow need to handle this state?
                            stream_state state = f.get0();
                            logger.debug("[migrate_extent_journal_ordered] start trans file:{}", file);
                            return make_ready_future<>();
                        }catch(...){
                            std::ostringstream out;
                            out << "error while migrate_extent_journal_ordered, exception:" << std::current_exception();
                            sstring error_info = out.str();
                            logger.error(error_info.c_str()); 
                            return make_exception_future<>(std::runtime_error(error_info));
                        }
                    });
                });
            });//do_for_each
        });
    }).then_wrapped([migrate_params](auto f){
        try {
            f.get(); 
            logger.debug("[migrate_extent_journal_ordered] done, migrate_params:{}", migrate_params);
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[migrate_extent_journal_ordered] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    }); //get_commitlog_files

}//migrate_extent_journal_ordered



// ================================================================
// for replicate migrate
// ================================================================
future<bool> migration_manager::is_driver_node(sstring volume_id){
    auto& local_context_service = hive::get_local_context_service();
    return local_context_service.get_or_pull_volume_context(volume_id).then(
        [](auto volume_context){
            auto driver_node_ip = volume_context.get_driver_node().get_ip();
            return make_ready_future<bool>(is_me(driver_node_ip));
    });
}

future<sstring> migration_manager::select_src_disk_id(sstring src_disk_ids){
    auto disk_ids = hive_tools::split_to_vector(src_disk_ids, ":");
    auto it = disk_ids.begin();
    auto end = disk_ids.end();
    bool stop = false;
    lw_shared_ptr<sstring> disk_ptr = make_lw_shared<sstring>();
    return do_with(std::move(it), std::move(end), std::move(disk_ids), std::move(stop), disk_ptr
        , [](auto& it, auto& end, auto& disk_ids, auto& stop, auto disk_ptr){

        auto stop_fun = [&it, &end, &stop](){
            return (it==end || stop);
        };
        auto action_fun = [&it, &end, &stop, disk_ptr](){
            auto& context_service = hive::get_local_context_service();
            return context_service.get_or_pull_disk_context(*it).then_wrapped([&it, &stop, disk_ptr](auto f){
                try{
                    f.get();
                    *disk_ptr = *it;
                    stop = true;
                }catch(...){}
            }).finally([&it](){
                it++;
            });
        };
        return do_until(std::move(stop_fun), std::move(action_fun));
    }).then([disk_ptr, src_disk_ids](){
        if(disk_ptr->empty()){
              std::ostringstream out;
              out << "[replicate_extent_group] select src_disk_id error";
              out << ", disk_ids:" << src_disk_ids;
              sstring err_info = out.str();
              logger.error(err_info.c_str());
              return make_exception_future<sstring>(std::runtime_error(err_info));
        }
        return make_ready_future<sstring>(*disk_ptr);  
    });
}

//future<> migration_manager::replicate_with_monad(extent_store_proxy& shard_proxy, migrate_params_entry replicate_params){
//    auto extent_group_id = replicate_params.extent_group_id;
//    return shard_proxy.with_monad(extent_group_id, [this, replicate_params, &shard_proxy](){
//        logger.debug("[replicate_with_monad] replciate_extent_group start, replcate_params:{}", replicate_params);
//        return shard_proxy.migrate_extent_group(replicate_params).then_wrapped([replicate_params](auto f){
//             try{
//                f.get();
//                logger.debug("[replicate_with_monad] replciate_extent_group success, replcate_params:{}", replicate_params);
//                return make_ready_future<>();
//            }catch(...){
//                std::ostringstream out;
//                out << "[replicate_with_monad] replciate_extent_group faili:" << std::current_exception();
//                sstring err_info = out.str();
//                logger.error(err_info.c_str());
//                return make_exception_future<>(std::runtime_error(err_info));
//            }
//        });
//    });
//}

future<> migration_manager::replicate_with_monad(extent_store& shard_store, migrate_params_entry replicate_params){
    auto extent_group_id = replicate_params.extent_group_id;
    return shard_store.with_monad(extent_group_id, [this, replicate_params]()mutable{
        return this->stream_extent_group(replicate_params);
    });
}


#if 0
future<> migration_manager::replicate_extent_group(migrate_params_entry replicate_params){
    logger.debug("[replicate_extent_group] is driver start");
    return is_driver_node(replicate_params.volume_id).then([replicate_params](auto is_me){
        if(!is_me){
            std::ostringstream out;
            out << "[replicate_extent_group] ensure driver node is me failed";
            out << ", replicate_params" << replicate_params;
            sstring err_info = out.str();
            logger.error(err_info.c_str());
            throw std::runtime_error(err_info);
        }   
        return make_ready_future<>();
    }).then([this, &replicate_params]()mutable{
        logger.debug("[replicate_extent_group] select_src_disk start");
        //2 ensure disk_ids context exist
        return select_src_disk_id(replicate_params.src_disk_id).then(
                [this, &replicate_params](sstring src_disk_id)mutable{
            logger.debug("[replicate_extent_group] select_src_disk done, disk_id:{}",src_disk_id);
            replicate_params.src_disk_id = src_disk_id;
            std::set<sstring> set_ids;
            set_ids.insert(replicate_params.src_disk_id);
            set_ids.insert(replicate_params.dst_disk_id);
            auto& context_service = hive::get_local_context_service();
            logger.debug("[replicate_extent_group] ensure_disk_context start, replicate_params:{}", replicate_params);
            return context_service.ensure_disk_context(set_ids).then([this, set_ids](auto exist)mutable{
                if(!exist) {
                    std::ostringstream out;
                    out << "[replicate_extent_group] ensure disk context failed, disk_ids:" << this->build_ids_str(set_ids);
                    sstring error_info = out.str();
                    logger.error(error_info.c_str());
                    throw std::runtime_error(error_info);
                }
                return make_ready_future<>();
            });
        });
    }).then([this, &replicate_params](){
        logger.debug("[replicate_extent_group] replicate extent group start, replicate_params:{}", replicate_params);
        return _stream_plan_limit_num.wait().then([this, replicate_params](){
            auto extent_group_id = replicate_params.extent_group_id;
            auto& local_extent_store_proxy = hive::get_local_extent_store_proxy();
            auto shard_id = local_extent_store_proxy.shard_of(extent_group_id);
            auto& extent_store_proxy = hive::get_extent_store_proxy();
            return extent_store_proxy.invoke_on(shard_id, [this, replicate_params](auto& shard_proxy){
                logger.debug("[replicate_extent_group] replicate_with_monad start, replicate_params:{}", replicate_params);
                return this->replicate_with_monad(shard_proxy, replicate_params);
            });
        }).finally([this](){
             _stream_plan_limit_num.signal();
        });
    });               
}
#endif

future<> migration_manager::replicate_extent_group(std::map<sstring, sstring> replicate_params){
    logger.debug("[{}] start, replicate_params:{}", __func__, hive_tools::format(replicate_params));

    return _stream_plan_limit_num.wait().then([this, replicate_params]()mutable{
        auto extent_group_id = replicate_params["extent_group_id"];   
        auto src_disk_id = replicate_params["src_disk_id"];   
        auto dst_disk_id = replicate_params["dst_disk_id"];   
        auto container_name = replicate_params["container_name"];   
        auto repair_disk_id = replicate_params["repair_disk_id"];   

        migrate_params_entry params(
            "empty_volume_id"
            , extent_group_id
            , src_disk_id
            , dst_disk_id
            , 0
            , hive_config::extent_group_size
        );

        auto& local_store = hive::get_local_extent_store();
        auto shard_id = local_store.shard_of(extent_group_id);
        auto& extent_store = hive::get_extent_store();
        return extent_store.invoke_on(shard_id, [this, params](auto& shard_store){
            return this->replicate_with_monad(shard_store, params);
        }).then([this, replicate_params](){
            //return commit_to_metadata_for_replicate(replicate_params);
            return make_ready_future<>();
        }).finally([this](){
             _stream_plan_limit_num.signal();
        });
    });               
}


} //namespace hive
