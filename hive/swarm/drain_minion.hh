#pragma once

#include <iostream>
#include <functional>
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include "gms/inet_address.hh"

#include "hive/extent_revision.hh"
#include "hive/group_revision_set.hh"
#include "hive/drain/drain_task_group.hh"
#include "hive/journal/volume_revision_set.hh"
#include "hive/journal/revision_data.hh"
#include "hive/metadata_service/metadata_service.hh"

namespace hive {

class drain_minion {
private:
    void set_drain_task_status(drain_task_group& task_group, drain_task_status status);

    //--- for create or replicate extent group 
    future<> create_extent_group(drain_extent_group_task& task);
    future<gms::inet_address> select_get_extent_group_src_node(sstring disk_ids_str);
    future<sstring> get_disk_mount_path(sstring disk_id);
    future<> replicate_extent_group(drain_extent_group_task& task);
    future<> prepare_for_task(drain_extent_group_task& task);
    future<> prepare_for_task_group(drain_task_group& task_group);
    future<> commit_for_create(drain_task_group& task_group);

    //--- for get journal data
    future<gms::inet_address> select_get_journal_data_src_node(std::vector<sstring> journal_nodes);
    future<std::map<sstring, std::vector<revision_data>>> get_journal_data(drain_task_group& task_group);

    //--- for write extent group 
    future<> write_extent_group(
        drain_extent_group_task& task
        , std::vector<revision_data> revision_datas);

    future<> write_extent_groups(
        drain_task_group& task_group
        , std::map<sstring, std::vector<revision_data>> revision_datas);

    future<> commit_for_write(drain_task_group& task_group);
public:
    drain_minion();
    ~drain_minion();

    future<drain_task_group> perform_drain(drain_task_group task_group);

    friend std::ostream& operator<<(std::ostream& out, const drain_minion& minion);
}; 


} //namespace hive

