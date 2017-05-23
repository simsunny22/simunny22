#pragma once

#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/timer.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include <limits>
#include <cstddef>
#include "timestamp.hh"
#include <list>
#include <seastar/core/rwlock.hh>
#include "hive/extent_datum.hh"
#include "hive/extent_revision_set.hh"
#include "hive/file_store.hh"
#include "hive/hive_shared_mutex.hh"
#include "hive/journal/revision_data.hh"

namespace query{
	class hive_read_command;
	class hive_result;
}

namespace hive {

class extent_store;

extern distributed<extent_store> _the_extent_store;
inline distributed<extent_store>& get_extent_store() {
    return _the_extent_store;
}
inline extent_store& get_local_extent_store() {
    return _the_extent_store.local();
}

class extent_store : public enable_lw_shared_from_this<extent_store>{
private:
    std::map<sstring, hive_shared_mutex> _monad_queue;
    //std::map<sstring, int64_t> _monad_timestamp;

    //timer<lowres_clock> _timer;

    //void on_timer();
    //void wash_monad_queue();
    //bool check_context(sstring extent_group_id, sstring target_disk_id);
    //future<> write_revisions(sstring path, extent_revision_set& revision_set, sstring target_disk_id);
    future<> write_revisions(sstring file_path, std::vector<extent_revision>& revisions); 
    future<> write_revisions(sstring file_path, std::vector<revision_data> revision_datas); 

    //for monad
    //int64_t get_or_create_mutex_timestamp(sstring extent_group_id);
    //void update_mutex_timestamp_for_test(sstring extent_group_id);
    
   
public:
    extent_store();
    extent_store(extent_store&&) = delete;
    ~extent_store();

    future<> stop();
    unsigned shard_of(const sstring extent_group_id);
    
    //future<lw_shared_ptr<hive_result>> read_extent_group(lw_shared_ptr<hive_read_command> read_cmd); //tododl:yellow abandon
    future<bytes> read_extent_group(
        sstring extent_group_id
        , uint64_t offset
        , uint64_t length
        , sstring disk_mount_path);

    //future<> rwrite_extent_group(extent_revision_set revision_set, sstring target_disk_id);

    //TODO:yellow need abandon??
    future<> rwrite_extent_group(sstring extent_group_id
                               , sstring disk_mount_path
                               , std::vector<extent_revision>& revisions);

    future<> rwrite_extent_group(sstring extent_group_id
                               , sstring disk_mount_path
                               , std::vector<revision_data> revision_datas);


    future<> create_extent_group(sstring extent_group_id, sstring disk_ids); //abandon

    future<> create_extent_group_ex(sstring extent_group_id, sstring path);
    future<> create_extent_group_with_data(sstring extent_group_id, sstring disk_mount_path, bytes data); 
    future<> delete_extent_group(sstring extent_group_id, sstring path);

    //for migrate
    future<> write_extent_group_exactly(sstring extent_group_id, size_t offset, size_t length
        , bytes data, sstring target_disk_id);

    future<> touch_extent_group_file(sstring extent_group_id, sstring target_disk_id, size_t truncate_size);

    //for monad
    hive_shared_mutex& get_or_create_mutex(sstring extent_group_id);
   
    //ps:this monad already move to extent_store_proxy
    template <typename Func>
    futurize_t<std::result_of_t<Func ()>> with_monad(sstring extent_group_id, Func&& func){
        auto& mutex = get_or_create_mutex(extent_group_id);
        //update_mutex_timestamp_for_test(extent_group_id);
        return with_hive_lock(mutex, std::forward<Func>(func)).finally([this, extent_group_id](){
             //update_mutex_timestamp_test(extent_group_id);
        }); 
    }

    //for test
    future<sstring> get_extent_group_md5(sstring extent_group_id, uint64_t offset, uint64_t length, sstring disk_id);


    //for dltest1 used in rwrite_volume_handler_test.cc
    semaphore write_journal_test = 1;

};// class extent_store

} //namespace hive

