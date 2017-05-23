#ifndef __HIVE_MEMORY_CACHE_MEMORY__
#define __HIVE_MEMORY_CACHE_MEMORY__

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <experimental/string_view>
#include <string>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <cryptopp/md5.h> 

#include "core/temporary_buffer.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/align.hh"
#include "core/timer.hh"
#include "core/reactor.hh"
#include "db/config.hh"

#include "hive/cache/item_key.hh"
#include "hive/cache/memory_slab.hh"
#include "hive/cache/ssd_cache.hh"

namespace bi = boost::intrusive;
namespace hive {

using version_type = uint64_t;

static constexpr uint64_t default_refrequence_count = 1;
static constexpr uint64_t default_task_count = 5;

struct item_insertion_data;

class item : public slab_item_base {
public:
    static constexpr uint8_t field_alignment = alignof(void*);
private:
    using hook_type = bi::unordered_set_member_hook<>;
    hook_type _cache_link;

    version_type _version;
    size_t   _key_hash;
    uint32_t _value_size;
    uint16_t _ref_count;
    uint8_t  _key_size;
    uint64_t _frequence_count;
    bool     _flush_queued;
    char     _data[];

    //value md5
    //type _value_digest;
public:
    item() = default;
    item(item_key&& key, temporary_buffer<char>&& value, version_type version = 1, bool flushqueued = false);
    
    const std::experimental::string_view key() const;
    
    temporary_buffer<char> datum();
    
    version_type version() const {
        return _version;
    }
    
    void touch();
    
    uint64_t incr_frequence_count();
    
    bool set_cache_flush_manager_queued(bool queued);
    
    bool cache_flush_manager_queued() const;
    
    // for boost::intrusive_ptr
    bool is_unlocked() const;
    
    size_t key_size() const;
    
    size_t value_size() const;
    
    uint64_t frequence_count() const;
    
    friend bool operator==(const item& a, const item &b);
    friend size_t hash_value(const item &i);
    friend void intrusive_ptr_add_ref(item* it);
    friend void intrusive_ptr_release(item* it);
    friend class memory_cache;
    friend class item_key_cmp;

}; //class item 
using item_ptr = boost::intrusive_ptr<item>;

struct item_key_cmp {
private:
    bool compare(const item_key& key, const item& it) const {
        return (it._key_hash == key.hash()) 
            && (it._key_size == key.key().size()) 
            && (memcmp(it._data, key.key().c_str(), it._key_size) == 0); 
    }
public:
    bool operator()(const item_key& key, const item& it) const {
        return compare(key, it);
    }

    bool operator()(const item& it, const item_key& key) const {
        return compare(key, it);
    }
}; //struct item_key_cmp

struct cache_stats {
    size_t _get_total_counts {};
    size_t _set_total_counts {};
    size_t _first_level_get_hits {};
    size_t _first_level_get_misses {};
    size_t _second_level_get_hits {};
    size_t _second_level_get_misses {};
    size_t _set_adds {};
    size_t _set_replaces {};
    size_t _delete_misses {};
    size_t _delete_hits {};
    size_t _first_level_evicted {};
    size_t _second_level_evicted {};
    size_t _bytes {};
    size_t _resize_failure {};
    size_t _size {};

    void operator+=(const cache_stats& o) {
        _get_total_counts += o._get_total_counts;
        _set_total_counts += o._set_total_counts;
        _first_level_get_hits += o._first_level_get_hits;
        _first_level_get_misses += o._first_level_get_misses;
        _second_level_get_hits += o._second_level_get_hits;
        _second_level_get_misses += o._second_level_get_misses;
        _set_adds += o._set_adds;
        _set_replaces += o._set_replaces;
        _delete_misses += o._delete_misses;
        _delete_hits += o._delete_hits;
        _first_level_evicted += o._first_level_evicted;
        _second_level_evicted += o._second_level_evicted;
        _bytes += o._bytes;
        _resize_failure += o._resize_failure;
        _size += o._size;
    }

    friend std::ostream& operator<<(std::ostream& out, const cache_stats& stats);
}; //struct cache_stats

class memory_cache {
private:
    using cache_type = bi::unordered_set<item
                                       , bi::member_hook<item, item::hook_type, &item::_cache_link>
                                       , bi::power_2_buckets<true>
                                       , bi::constant_time_size<true> >;

    using cache_iterator = typename cache_type::iterator;
    static constexpr size_t initial_bucket_count = 1 << 10; 
    static constexpr float load_factor = 0.75f;

    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type  _cache;
    cache_stats _stats;
    timer<lowres_clock> _timer;
    
    void on_timer();
    
    // functions
    inline cache_iterator find(const item_key& key);
    inline cache_iterator add_overriding(cache_iterator i, item_insertion_data& insertion);
    inline void add_new(item_insertion_data& insertion);
    
    template<bool IsInCache=true, bool Release=true>
    void erase(item& item_ref);
    
    void maybe_rehash();
    future<item_ptr> get_from_ssd_cache(const item_key& key);
public:
    memory_cache(const db::config& cfg);
    ~memory_cache();

    future<> init();
    future<> stop();
    future<> flush_all();
    
    size_t size();
    size_t bucket_count();
    
    future<bool>     set(item_insertion_data& insertion);
    future<item_ptr> get(const item_key& key);
    future<bool>     remove(const item_key key);
    
    future<> run_flush(item_insertion_data insertion);
    bool set_cache_flush_manager_queued(const item_key key,bool queued);
    //reload data from ssd to memory
    future<item_ptr> reload_from_ssd(const item_key& key);

}; //class memory_cache

} //namespace hive 

#endif

