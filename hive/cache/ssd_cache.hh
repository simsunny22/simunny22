#ifndef __HIVE_SSD_CACHE__
#define __HIVE_SSD_CACHE__

#include <signal.h>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <string>
#include <experimental/string_view>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include "core/temporary_buffer.hh"
#include "core/align.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "db/config.hh"

#include "hive/cache/item_key.hh"
#include "hive/cache/ssd_slab.hh"
#include "hive/hive_directories.hh"


namespace hive {

uint64_t get_cpu_id();

class ssd_item : public slab_ssd_item_base {
private:
    using hook_type = bi::unordered_set_member_hook<>;
    hook_type _cache_link;

    version_type _version;
    char     _key[default_key_length];
    uint64_t _key_hash;
    uint64_t _key_size;
    uint16_t _ref_count;
    uint64_t _offset;  

public:
    ssd_item() = default;
    ssd_item(uint64_t offset, item_key&& key, version_type version = 1);
    ssd_item(uint64_t offset);

    future<> persist_data(temporary_buffer<char>&& data);

    const std::string key() const;

    future<temporary_buffer<char>> datum();

    const uint64_t offset() const;

    const version_type version() const;

    //touch item for lru position
    void touch();
 
    //for boost::intrusive_ptr
    bool is_unlocked() const;

    friend bool operator==(const ssd_item& a, const ssd_item &b);
    friend uint64_t hash_value(const ssd_item &i);
    friend void intrusive_ptr_add_ref(ssd_item* it);
    friend void intrusive_ptr_release(ssd_item* it);
    friend class ssd_cache;
    friend class ssd_item_key_cmp;
}; //class ssd_item
using ssd_item_ptr = boost::intrusive_ptr<ssd_item>;

struct ssd_item_key_cmp {
private:
    bool compare(const item_key& key, const ssd_item& it) const {
        return (key.hash() == it._key_hash) 
            && (memcmp(it._key, key.key().c_str(), it._key_size) == 0);
    }
public:
    bool operator()(const item_key& key, const ssd_item& it) const {
        return compare(key, it);
    }
    bool operator()(const ssd_item& it, const item_key& key) const {
      return compare(key, it);
    }
}; //struct ssd_item_key_cmp

class ssd_cache {
private:
    using cache_type = bi::unordered_set<ssd_item
                                       , bi::member_hook<ssd_item, ssd_item::hook_type, &ssd_item::_cache_link>
                                       , bi::power_2_buckets<true>
                                       , bi::constant_time_size<true> >;

    using cache_iterator = typename cache_type::iterator;
    static constexpr uint64_t initial_bucket_count = 1 << 10; 
    static constexpr float load_factor = 0.75f;

    uint64_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _cache;
    std::unique_ptr<db::config> _cfg;

    hive::directories _dirs;

    inline future<> add_overriding(cache_iterator i, item_insertion_data&& insertion);
    inline future<> add_new(item_insertion_data&& insertion);
    
    template<bool IsInCache = true, bool Release = true>
    void erase(ssd_item& item_ref);
    void maybe_rehash();

    future<> touch_cache_file(sstring cache_home_dir, uint64_t truncate_size);
public:
    ssd_cache(const db::config& cfg);
    ~ssd_cache();

    future<> init();
    future<> stop();
    future<> flush_all();
    
    // cache operator
    void touch(const item_key key);
    
    future<bool> set(item_insertion_data insertion);
    
    future<std::pair<ssd_item_ptr, temporary_buffer<char>>> get(const item_key& key);
    
    future<bool> remove(const item_key& key);
    
    inline cache_iterator find(const item_key& key);
    
    uint64_t size();
    
    uint64_t bucket_count();

}; //class ssd_cache

} // namespace hive 

#endif
