#pragma once

#include "core/shared_ptr.hh"
#include "core/future-util.hh"
#include "db/config.hh"
#include "log.hh"

#include "hive/hive_request.hh"
#include "hive/extent_revision_set.hh"
#include "hive/extent_datum.hh"

#include "hive/store/extent_store_proxy.hh"
#include "hive/store/hint_entry.hh"
#include "hive/hive_plan.hh"


namespace hive{

class object_stream {
private:
    //for write
    future<write_plan> get_write_plan(sstring object_id, uint64_t offset, uint64_t length);
    future<> execute_create_actions(sstring object_id, std::vector<create_action> create_actions);
    future<> do_create_extent_group(sstring object_id, create_action action);
    future<> commit_for_create_extent_group(sstring object_id, std::vector<create_action> create_actions);

    future<std::vector<extent_revision>> 
    build_extent_revisions(hive_write_command write_cmd, std::vector<write_action> actions);

    future<std::map<gms::inet_address, sstring>> get_extent_group_targets(sstring disk_ids);
    std::vector<sstring> get_disk_ids(std::map<gms::inet_address, sstring>& targets);
    std::unordered_set<gms::inet_address> get_disk_addrs(std::map<gms::inet_address, sstring>& targets);

    future<std::experimental::optional<hint_entry>> do_write_extent_group_end(
        sstring object_id
        , sstring extent_group_id
        , std::map<gms::inet_address, sstring>& targets
        , std::unordered_set<gms::inet_address>& success );


    future<std::experimental::optional<hint_entry>> do_write_object(extent_revision revision);
    future<> commit_for_write_object(std::vector<hint_entry> hints);
    future<> remove_extent_group_context(hint_entry& hint);
    future<> execute_write_actions(std::vector<extent_revision> revisions);
    future<uint64_t> commit_write_object(sstring object_id, uint64_t offset, uint64_t length);

    //for read
    future<read_plan> get_read_plan(sstring object_id, uint64_t offset, uint64_t length);
    future<std::vector<hive_read_subcommand>> build_read_subcommand(sstring volume_id, read_plan plan);
    
    future<extent_datum> load_extent_store_without_cache(hive_read_subcommand read_subcmd);
    future<extent_datum> load_extent_store_data(hive_read_subcommand read_subcmd);

    future<std::tuple<uint64_t, bytes>> do_read_object(uint64_t order_id, hive_read_subcommand read_subcmd);
    future<bytes> execute_subcommads(uint64_t total_length, std::vector<hive_read_subcommand> read_subcmds);


public:
    object_stream();
    ~object_stream();

    future<uint64_t> write_object(hive_write_command write_cmd);
    future<bytes> read_object(hive_read_command read_cmd);

}; //class object_stream

}//namespace hive
