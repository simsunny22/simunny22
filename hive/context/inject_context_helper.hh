#ifndef __HIVE_INJECT_CONTEXT_HELPER_H__
#define __HIVE_INJECT_CONTEXT_HELPER_H__

#include <map>
#include <db/config.hh>
#include "seastar/core/sstring.hh"
#include "seastar/core/future.hh"
#include "seastar/core/distributed.hh"

namespace hive {

class inject_context_helper {
private:
    future<> inject_disk_context_by_config(lw_shared_ptr<db::config> config);    
    future<> inject_disk_context(sstring disks);
    sstring build_context_json_for_disk(sstring ctx_disks);

    future<> inject_volume_context_by_config(lw_shared_ptr<db::config> config);    
    future<> inject_volume_context(uint32_t volume_count, sstring volume_driver_node_ip, sstring volume_replica_node_ips);
    sstring build_context_json_for_volume(uint32_t volume_count, sstring dirver_node_ip, sstring replica_node_ips);

    future<> inject_extent_group_context_by_config(lw_shared_ptr<db::config> config);    
    future<> inject_extent_group_context(uint32_t volume_count
                                       , uint32_t extent_group_count_per_volume
                                       , sstring extent_group_target_disk_ids);
    sstring build_context_json_for_extent_group(uint32_t volume_count
                                              , uint32_t extent_group_count_per_volume
                                              , sstring replica_disk_ids);

public:
    inject_context_helper();
    ~inject_context_helper();

    future<> inject_context_by_config(lw_shared_ptr<db::config> config);
    future<> inject_context(sstring disks
                          , uint32_t volume_count
                          , sstring volume_driver_node_ip
                          , sstring volume_replica_node_ips
                          , uint32_t extent_group_count_per_volume
                          , sstring extent_group_target_disk_ids);

};

} //namespace hive

#endif // __HIVE_INJECT_CONTEXT_HELPER_H_
