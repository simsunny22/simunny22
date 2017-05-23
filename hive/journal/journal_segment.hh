#pragma once
#include <unordered_map>
#include <vector>

#include "core/shared_ptr.hh"
#include "core/future.hh"
#include "core/sstring.hh"
#include "core/thread.hh"
#include "checked-file-impl.hh"

#include "hive/journal/volume_revision.hh"
#include "hive/journal/revision_data.hh"
#include "hive/hive_shared_mutex.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/volume_revision_view.hh"
#include "hive/drain/drain_extent_group_task.hh"

namespace hive{

class revision_index{
public:
    sstring  segment_id;
    uint64_t offset_in_segment;
    sstring  volume_id;
    uint64_t offset_in_volume;
    sstring  extent_id;
    uint64_t offset_in_extent;
    uint64_t data_length;
    uint64_t serialize_length;
    uint64_t vclock;

    revision_index(
            sstring  segment_id, 
            uint64_t offset_in_segment,
            sstring  volume_id,
            uint64_t offset_in_volume,
            sstring  extent_id,
            uint64_t offset_in_extent,
            uint64_t data_length,
	    uint64_t serialize_length,
            uint64_t vclock)
        : segment_id(segment_id)
        , offset_in_segment(offset_in_segment)
        , volume_id(volume_id) 
        , offset_in_volume(offset_in_volume) 
        , extent_id(extent_id)
        , offset_in_extent(offset_in_extent)
        , data_length(data_length)
        , serialize_length(serialize_length)
        , vclock(vclock){}

    bool operator ==(const revision_index& other){
        return other.segment_id == segment_id
            && other.offset_in_segment == offset_in_segment
            && other.volume_id == volume_id
            && other.offset_in_volume == offset_in_volume
            && other.extent_id == extent_id
            && other.offset_in_extent== offset_in_extent
            && other.data_length == data_length
            && other.serialize_length == serialize_length
            && other.vclock == vclock;
    }

    friend std::ostream& operator<<(std::ostream& out, const revision_index& index) {
        return out << "{revision_index:["
                   << "volume_id:" << index.volume_id
                   << ", offset_in_volume:" << index.offset_in_volume
                   << ", segment_id:" << index.segment_id
                   << ", offset_in_segment:" << index.offset_in_segment
                   << ", extent_id:" << index.extent_id
                   << ", offset_in_extent:" << index.offset_in_extent
                   << ", data_length:" << index.data_length
                   << ", serialize_length:" << index.serialize_length
                   << ", vclock:" << index.vclock
                   << "]}";
    }
};


enum class commitlog_flag{
   PRIMARY = 0,
   SECONDARY,
};


class primary_journal;
class secondary_journal;

// ================================================
// commitlog
// ================================================
enum sync_mode {
    UNKOWN,
    PERIODIC,
    BATCH
};

class segment_commitlog{
private:
    sstring _volume_id;
    sstring _path;
    sstring _segment_id; //file_name
    file    _fd;
    uint64_t _max_size;
    uint64_t _offset = 0;
    bool   _can_delete = false;
    commitlog_flag _flag;

    sync_mode _sync_mode;
    timer<lowres_clock> _timer;


    semaphore _write_flush_semaphore; 
    uint64_t _write_step = 0; 
    uint64_t _flush_step = 0;

    void set_timer();
    void on_timer();

    future<> sync_data(uint64_t write_size);
public:
    static future<lw_shared_ptr<segment_commitlog>> 
    make_primary_segment_commitlog(
        sstring volume_id, 
        sstring segment_id,
        sstring path,  
        uint64_t max_size);

    static future<lw_shared_ptr<segment_commitlog>> 
    make_secondary_segment_commitlog(
        sstring volume_id,
        sstring segment_id,
        sstring path,
        uint64_t max_size);

    segment_commitlog(
        sstring volume_id,
        sstring path, 
        sstring segment_id, 
        file fd,
        uint64_t max_size, 
        commitlog_flag flag);


    ~segment_commitlog();

    future<> write_primary_commitlog(std::unique_ptr<char[], free_deleter> bufptr, sstring extent_id, uint64_t align_size);
    future<> write_secondary_commitlog(bytes data, uint64_t align_offset, uint64_t align_size);

    future<volume_revision> read_commitlog(revision_index index);
    //future<extent_revision> read_commitlog(revision_index index);
    future<std::unique_ptr<char[], free_deleter>> read_commitlog_for_drain();

    bool should_flush();
    uint64_t get_offset();
    void set_offset(uint64_t offset);
    bool need_rollback_memtable(uint64_t offset);
    sstring get_last_extent_id();

    future<> discard_commitlog();

};

// ================================================
// memtable
// ================================================
class segment_memtable{
private:
    sstring _volume_id;
    std::unordered_map<sstring, std::vector<revision_index>> _index_map; //extent_id -> indexs
    //for drain add 
    std::vector<revision_index> _order_indexs;
    //std::unordered_map<sstring, std::vector<revision_index>> _group_index_map; //extent_group_id -> indexs
public:
    static future<lw_shared_ptr<segment_memtable>> make_segment_memtable(sstring volume_id);
    segment_memtable(sstring volume_id);

    bool empty();
    
    future<> insert_index(sstring extent_id, revision_index index);
    future<std::vector<revision_index>> get_indexs(sstring extent_id);
    future<std::vector<revision_index>> get_indexs();
    future<std::vector<revision_index>> get_order_indexs(){
        return make_ready_future<std::vector<revision_index>>(_order_indexs);
    }

    //future<std::vector<revision_index>> get_indexs_by_extent_group_id(sstring extent_group_id);
    //const std::unordered_map<sstring, std::vector<revision_index>>& get_indexs_by_extent_group_id(){
    //   return _group_index_map; 
    //}

    void rollback_for_sync(uint64_t offset);
    void rollback_for_rebuild(uint64_t vclock);

};

// ================================================
// journal segment
// ================================================
class journal_segment : public enable_shared_from_this<journal_segment> {
    using shared_commitlog = lw_shared_ptr<segment_commitlog>;
    using shared_memtable  = lw_shared_ptr<segment_memtable>;
private:
    sstring _volume_id;
    sstring _seg_id;
    shared_commitlog _seg_commitlog;
    shared_memtable  _seg_memtable;
    uint64_t _occupancy; //total revision size
    hive_shared_mutex _mutex;

    uint64_t get_commitlog_offset();
    void rollback_commitlog_offset(uint64_t offset);

    std::vector<revision_data>
    do_get_revision_data(
        std::vector<extent_group_revision>& revision_map,
        std::unique_ptr<char[], free_deleter>&  bufptr);

public:
    static future<lw_shared_ptr<journal_segment>> make_primary_segment(
        sstring volume_id
    );
    static future<lw_shared_ptr<journal_segment>> make_secondary_segment(
        sstring volume_id,
        sstring segment_id, 
        sstring path,
        uint64_t size
    );
    static future<lw_shared_ptr<journal_segment>> do_make_primary_segment(
        sstring volume_id
        , sstring segment_id
        , sstring path
        , uint64_t max_size
    );
    static future<lw_shared_ptr<segment_commitlog>> make_secondary_commitlog(
        sstring volume_id
        , sstring segment_id
        , sstring path
        , uint64_t max_size
    );

    journal_segment(
        sstring volume_id
        , sstring journal_segment_id
        , shared_commitlog commitlog_ptr
        , shared_memtable memtable_ptr
    );

    journal_segment(journal_segment&& segment) noexcept;
    ~journal_segment();

    sstring get_id() { return _seg_id; }

    //future<> write_primary_segment(extent_revision revision);
    future<> write_primary_segment(volume_revision revision);

    future<> sync_secondary_segment(
        sstring segment_id,
        uint64_t offset_in_segment,
        uint64_t offset_in_volume, 
        sstring extent_id,
        uint64_t offset_in_extent,
        uint64_t data_length,
        uint64_t serialize_length,
        uint64_t vclock, 
        bytes data
     );

    future<std::vector<volume_revision>> read_segment(hive_read_subcommand read_subcmd);
    //future<std::map<sstring, std::vector<extent_revision>>> read_segment_for_drain();

    future<> discard_primary_segment();
    future<> discard_secondary_segment();

    template <typename Func>
    futurize_t<std::result_of_t<Func ()>> with_monad(Func&& func){
        return with_hive_lock(_mutex, std::forward<Func>(func));
    }

    bool should_flush();
    bool empty();
    uint64_t get_occupancy();
    void rollback_for_rebuild(uint64_t vclock);

    //future<std::vector<extent_revision>> get_data_by_extent_group_id(sstring extent_greup_id);
    
    future<std::vector<volume_revision_view>> scan();

    future<std::map<sstring, std::vector<revision_data>>>
    get_revision_data(std::map<sstring, std::vector<extent_group_revision>> revision_map);
};



}//hive
