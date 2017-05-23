#pragma once

#include "core/distributed.hh"
#include "gms/inet_address.hh"
#include "seastar/core/shared_ptr.hh"
#include "db/config.hh"
#include "hive/stream/migrate_params_entry.hh"
#include "hive/stream/stream_plan.hh"
#include "hive/stream/migrate_chunk.hh"
#include "hive/context/extent_group_context_entry.hh"

#include "hive/store/extent_store_proxy.hh"

namespace hive{

class migrate_result_entry{
public:
    sstring volume_id;
    sstring extent_group_id;
    sstring src_disk_id;
    sstring dst_disk_id;
    sstring src_file_path;
    sstring dst_file_path;
    bool copy_file_success   = false;
    bool delete_src_file_success = false;
    sstring message = "";

    migrate_result_entry(sstring volume_id_
                       , sstring extent_group_id_
                       , sstring src_disk_id_
                       , sstring dst_disk_id_
                       , sstring src_file_path_
                       , sstring dst_file_path_
                       , sstring message = "")
                           : volume_id(volume_id_)
                           , extent_group_id(extent_group_id_)
                           , src_disk_id(src_disk_id_)
                           , dst_disk_id(dst_disk_id_)
                           , src_file_path(src_file_path_)
                           , dst_file_path(dst_file_path_)
                           , message(message)
                           {}

    friend std::ostream& operator<<(std::ostream& out, const migrate_result_entry& migrate_result);
};

struct migrate_stats {
    uint64_t total = 0;
    uint64_t successful = 0;
    uint64_t failed = 0;
};

class migration_manager: public seastar::async_sharded_service<migration_manager> {
private:
    lw_shared_ptr<db::config> _config;
    semaphore _stream_plan_limit_num;

    migrate_stats _stats;
    timer<lowres_clock> _timer;

private:
    void init();
    
    void on_timer();
    void print_stats_info();

    future<> stream_extent_group_to_self(sstring extent_group_id, sstring src_file, sstring dst_file);

    future<> stream_extent_group_to_remote(migrate_params_entry migrate_params
                                         , gms::inet_address peer_addr);

    future<migrate_result_entry> migrate_with_monad(extent_store_proxy& shard_proxy
                          , migrate_params_entry migrate_params
                          , lw_shared_ptr<migrate_result_entry> result_ptr);

    future<> delete_extent_group_context(sstring extent_group_id);

    future<> commit_to_metadata_for_copy(sstring metadata_url
                                       , migrate_params_entry migrate_params);

    future<> commit_to_metadata_for_delete(sstring metadata_url
                                         , migrate_params_entry migrate_params);

    future<> commit_to_metadata_for_replicate(std::map<sstring, sstring> replicate_params);

    sstring build_ids_str(std::set<sstring> set_ids);

    future<std::vector<sstring>> get_commitlog_files(migrate_params_entry migrate_params);
    future<> add_commitlog_files_to_plan(lw_shared_ptr<stream_plan> plan
                                   , std::vector<sstring> commitlog_files
                                   , migrate_params_entry migrate_params);

    //for test hive
    extent_group_context_entry get_extent_group_context(sstring extent_group_id);
    future<> reset_extent_group_context(extent_group_context_entry old_context_entry
                                      , migrate_params_entry migrate_params);
    future<> reset_extent_group_context(extent_group_context_entry context_entry);

    //for replicate
    future<bool> is_driver_node(sstring volume_id);
    future<sstring> select_src_disk_id(sstring src_disk_ids);
    future<> replicate_with_monad(extent_store& shard_store, migrate_params_entry replicate_params);


public:
    migration_manager(lw_shared_ptr<db::config> config);
    ~migration_manager();

    future<> stop();

    unsigned shard_of(const sstring extent_group_id);
   
    //callback by extent_store_proxy 
    future<> stream_extent_group(migrate_params_entry migrate_params); 

    //callback by migrate_forward_proxy
    future<migrate_result_entry> migrate_extent_group(migrate_params_entry migrate_params);
  
    //callback by migrate_forward_proxy
    future<> migrate_extent_journal_concurrent(migrate_params_entry migrate_params);    
    future<> migrate_extent_journal_ordered(migrate_params_entry migrate_params);    

    future<> touch_commitlog_file(migrate_scene scene, sstring commitlog_file_name, size_t truncate_size);    
    future<> write_extent_journal_chunk(migrate_chunk chunk);

    //for replicate
    //future<> replicate_extent_group(migrate_params_entry replicate_params);
    future<> replicate_extent_group(std::map<sstring, sstring> replicate_params);

}; //class migration_manager

extern distributed<migration_manager> _the_migration_manager;

inline distributed<migration_manager>& get_migration_manager() {
    return _the_migration_manager;
}

inline migration_manager& get_local_migration_manager() {
    return _the_migration_manager.local();
}

}//namespace hive
