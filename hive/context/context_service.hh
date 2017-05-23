#ifndef __HIVE_CONTEXT_H__
#define __HIVE_CONTEXT_H__

#include <map>
#include <memory>
#include "seastar/core/sstring.hh"
#include "seastar/core/future.hh"
#include "seastar/core/distributed.hh"

#include "log.hh"
#include "hive/http/seawreck.hh"
#include "hive/context/disk_context_entry.hh"
#include "hive/context/volume_context_entry.hh"
#include "hive/context/extent_group_context_entry.hh"
#include "db/config.hh"

#include "hive/http/json11.hh"


namespace hive {
class context_service;

enum context_type {
    VOLUME_CONTEXT = 0,
    DISK_CONTEXT   = 1,
    EXTENT_GROUP_CONTEXT = 2
};
                                                         
//////////////////////////////////
// ++ class context_service
//////////////////////////////////
class context_service {
private:
    //std::map<sstring, shared_ptr<context_entry>> _context_map;
    std::map<sstring, std::shared_ptr<context_entry>> _context_map;
    std::unique_ptr<db::config> _config;

    std::set<sstring> _disk_ids;
    std::set<sstring> _volume_ids;
private: 
    std::shared_ptr<context_entry>  parse_volume_context_json(hive::Json volume_context_json); 
    std::shared_ptr<context_entry>  parse_disk_context_json(hive::Json disk_context_json);
    std::shared_ptr<context_entry>  parse_extent_group_context_json(hive::Json context_json);

    sstring  build_request_body_for_volume(std::set<sstring>& volume_ids);
    sstring  build_json_body_for_disk(std::set<sstring>& disk_ids);
    sstring  build_request_body_for_extent_group(std::set<sstring>& extent_group_ids, sstring container_name);
    sstring  build_extent_group_ids_str(std::set<sstring> extent_group_ids);

    future<sstring> pull_extent_group_context(sstring volume_id, std::set<sstring> extent_group_ids);
    future<sstring> pull_volume_context(std::set<sstring> volume_ids);
    future<sstring> pull_disk_context(sstring server_url, std::set<sstring> disk_ids);

    future<sstring> do_pull_context(sstring uri, sstring json_body);

    bool disk_context_exist(std::set<sstring> disk_ids);
	  future<> set_all_shard(context_type type, sstring key, std::shared_ptr<context_entry> value);

    std::vector<disk_context_entry> get_disk_context(std::set<sstring> disk_id);
public:
    context_service(const db::config& config);
    ~context_service();
 
    future<> start();
    future<> stop();

    future<> set(context_type type, sstring key, std::shared_ptr<context_entry> value);

    future<> remove_all();
    future<> remove_all_on_every_shard();

    future<> remove(sstring context_key);
    future<> remove_on_every_shard(sstring context_key);
 
    sstring  get_context_value(sstring key);

    future<volume_context_entry>       get_or_pull_volume_context(sstring volume_id);
    future<disk_context_entry>         get_or_pull_disk_context(sstring disk_id);
    future<std::vector<disk_context_entry>> get_or_pull_disk_context(std::set<sstring> disk_ids);
    future<extent_group_context_entry> get_or_pull_extent_group_context(sstring volume_id, sstring extent_group_id);

    volume_context_entry       get_volume_context(sstring volume_id);
    disk_context_entry         get_disk_context(sstring disk_id);
    extent_group_context_entry get_extent_group_context(sstring extent_group_id);

    future<> save_volume_context(sstring volume_contexts_json);
    future<> save_disk_context(sstring disk_contexts_json);
    future<> save_extent_group_context(sstring contexts_json);

    future<bool> ensure_disk_context(std::set<sstring> disk_ids);
    
    future<> set_extent_group_context(extent_group_context_entry context_entry);
    future<> update_extent_group_context(sstring extent_group_id, sstring disk_ids, int64_t vclock);

    std::vector<sstring> get_disk_ids();
};

extern distributed<context_service> _the_context_service;

inline distributed<context_service>& get_context_service() {
    return _the_context_service;
}

inline context_service& get_local_context_service() {
    return _the_context_service.local();
}

inline shared_ptr<context_service> get_local_shared_context_service() {
    return _the_context_service.local_shared();
}

} //namespacehive
#endif // __HIVE_CONTEXT_H__





