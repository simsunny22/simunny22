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
#include "core/sstring.hh"
#include "core/temporary_buffer.hh"
#include "log.hh"

#include "hive/cache/ssd_cache.hh"
#include "hive/cache/item_key.hh"
#include "hive/cache/ssd_slab.hh"
#include "hive/cache/ssd_item_data.hh"
#include "hive/cache/cache.hh"
#include "hive/hive_directories.hh"
#include "hive/file_store.hh"


namespace hive {
static logging::logger logger("ssd_cache");
static __thread ssd_slab_allocator<ssd_item>* st_ssd_slab;

////////////////////////
// for global
////////////////////////
//a util function copy a std::string to a char array and null terminating the reuslt
void copy_string(const std::string src, char* dst, uint64_t dst_size) {
    strncpy(dst, src.c_str(), dst_size);
    dst[dst_size - 1] = '\0';
}

sstring get_local_shard_cache_file(sstring cache_home){
    uint64_t cpu_id = engine().cpu_id();
    sstring cache_file = sprint("%s/cache_%ld", cache_home, cpu_id); 
    return cache_file;
}

bool operator==(const ssd_item& a, const ssd_item &b) {
    return a.key() == b.key();
}

uint64_t hash_value(const ssd_item &i) {
    return i._key_hash;
}

inline void intrusive_ptr_add_ref(ssd_item* it) {
    assert(it->_ref_count >= 0);
    ++it->_ref_count;
    if (it->_ref_count == 2) {
        st_ssd_slab->lock_item(it);
    }
}

inline void intrusive_ptr_release(ssd_item* it) {
    --it->_ref_count;
    if (it->_ref_count == 1) {
        st_ssd_slab->unlock_item(it);
    } else if (it->_ref_count == 0) {
        st_ssd_slab->free(it);
    }
    assert(it->_ref_count >= 0);
}

////////////////////////
// for class ssd_item
///////////////////////
ssd_item::ssd_item(uint64_t offset, item_key&& key, version_type version)
    : _version(version)
    , _key_hash(std::hash<std::string>()(key.key()))
    , _key_size(key.key().size())
    , _ref_count(0U)
    , _offset(offset)
{
    copy_string(key.key(), _key, default_key_length);
}

ssd_item::ssd_item(uint64_t offset)
    :_offset(offset)
{}

future<> ssd_item::persist_data(temporary_buffer<char>&& data) {
    ssd_item_data datum_(_offset
                       , data.size()
                       , _version
                       , sstring(key().c_str(), key().size())
                       , std::move(data));

    db::config& cfg = get_local_cache().get_config();
    sstring cache_file = get_local_shard_cache_file(cfg.hive_cache_home()); 
    uint64_t slab_size = cfg.hive_cache_slab_size();
    return write_file(cache_file, _offset, datum_, slab_size);
}

const std::string ssd_item::key() const {
    return std::string{_key};
}

future<temporary_buffer<char>> ssd_item::datum() {
    db::config& cfg = get_local_cache().get_config();
    uint64_t slab_size = cfg.hive_cache_slab_size();
    uint64_t size = get_ssd_item_data_size(slab_size);

    sstring cache_file = get_local_shard_cache_file(cfg.hive_cache_home()); 
    return read_file(cache_file, _offset, size, slab_size).then([&] (auto&& item_data) {
        return make_ready_future<temporary_buffer<char>>(std::move(item_data._data));
    });
}

const uint64_t ssd_item::offset() const {
    return _offset;
}

const version_type ssd_item::version() const {
    return _version;
}

//touch item for lru position
void ssd_item::touch() {
    st_ssd_slab->touch(this);
}

//for boost::intrusive_ptr
bool ssd_item::is_unlocked() const {
    return _ref_count == 1;
}

///////////////////////////
// for class ssd_cache
//////////////////////////
ssd_cache::ssd_cache(const db::config& config)
    : _buckets(new cache_type::bucket_type[initial_bucket_count])
    , _cache(cache_type::bucket_traits(_buckets, initial_bucket_count))
    , _cfg(std::make_unique<db::config>(config))
{
    uint64_t slab_size = _cfg->hive_cache_slab_size();
    uint64_t max_slab_size = get_ssd_item_data_size(slab_size);
    uint64_t per_cpu_ssd_size = _cfg->hive_cache_second_level_size_per_cpu();

    auto erase_func = [this](ssd_item& item_ref){ 
                          erase<true, false>(item_ref); 
                      };
    st_ssd_slab = new ssd_slab_allocator<ssd_item>(per_cpu_ssd_size
                                                 , max_slab_size 
                                                 , std::move(erase_func) );

}

ssd_cache::~ssd_cache() {
    if(nullptr != st_ssd_slab){
        delete st_ssd_slab;
    }
}

future<> ssd_cache::init(){
    //touch cache file
    sstring cache_home = _cfg->hive_cache_home();
    uint64_t per_cpu_ssd_size = _cfg->hive_cache_second_level_size_per_cpu();
    return touch_cache_file(cache_home, per_cpu_ssd_size);
}

future<> ssd_cache::touch_cache_file(sstring cache_home_dir, uint64_t truncate_size){
    return _dirs.touch_and_lock(cache_home_dir).then([this, cache_home_dir, truncate_size](){
        sstring cache_file = get_local_shard_cache_file(cache_home_dir);
        return hive::create_file(cache_file, truncate_size, true);
    });
}

future<> ssd_cache::stop(){
    return flush_all();
}

future<> ssd_cache::flush_all() {
    _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (ssd_item* it) {
        erase<false>(*it);
    });
    return make_ready_future<>();
}

template<bool IsInCache, bool Release>
void ssd_cache::erase(ssd_item& item_ref) {
    if (IsInCache) {
        _cache.erase(_cache.iterator_to(item_ref));
    }
    if (Release) {
        intrusive_ptr_release(&item_ref);
    }
}

void ssd_cache::maybe_rehash() {
    if (_cache.size() >= _resize_up_threshold) {
        auto new_size = _cache.bucket_count() * 2;
        auto old_buckets = _buckets;
        try {
            _buckets = new cache_type::bucket_type[new_size];
        } catch (const std::bad_alloc& e) {
            return;
        }
        _cache.rehash(typename cache_type::bucket_traits(_buckets, new_size));
        delete[] old_buckets;
        _resize_up_threshold = _cache.bucket_count() * load_factor;
    }
}

void ssd_cache::touch(const item_key key) {
    auto i = find(key);
    if (i == _cache.end()) {
        return;
    }
    i->touch();
}

future<bool> ssd_cache::remove(const item_key& key) {
    auto i = find(key);
    if (i == _cache.end()) {
        return make_ready_future<bool>(false);
    }
    auto& item_ref = *i;
    erase(item_ref);
    return make_ready_future<bool>(true);
}

uint64_t ssd_cache::size() {
    return _cache.size();
}

uint64_t ssd_cache::bucket_count() {
    return _cache.bucket_count();
}

inline ssd_cache::cache_iterator
ssd_cache::find(const item_key& key) {
    return _cache.find(key, std::hash<item_key>(), ssd_item_key_cmp());
}

inline future<> ssd_cache::add_overriding(ssd_cache::cache_iterator i
                                        , item_insertion_data&& insertion) {
    auto& old_item = *i;
    erase(old_item);
    
    auto new_item = st_ssd_slab->create(std::move(insertion.key), insertion.version);
    intrusive_ptr_add_ref(new_item);

    return new_item->persist_data(std::move(insertion.data)).then_wrapped([this, new_item](auto f){
        try{
            f.get(); 
            auto ret = _cache.insert(*new_item);
            if(!ret.second){
                logger.error("[add_overriding] error, _cache.size:{}", _cache.size()); 
                throw std::runtime_error("ssd_cache::add_overriding insert _cache failed");
            }
            return make_ready_future<>();
        }catch(...){
            this->erase<false, true>(*new_item); 
            throw; 
        }
    });
}

inline future<> ssd_cache::add_new(item_insertion_data&& insertion) {
    auto new_item = st_ssd_slab->create(std::move(insertion.key), insertion.version);
    intrusive_ptr_add_ref(new_item);
    return new_item->persist_data(std::move(insertion.data)).then_wrapped([this,new_item](auto f){
        try{
            f.get(); 
            auto ret = this->_cache.insert(*new_item);
            if(!ret.second){
                logger.error("[add_new] error, _cache.size:{}", _cache.size()); 
                throw std::runtime_error("ssd_cache::add_new insert _cache failed");
            }
            this->maybe_rehash();
            return make_ready_future<>();
        }catch(...){
            this->erase<false, true>(*new_item); 
            throw;
        }
    });
}

future<bool> ssd_cache::set(item_insertion_data insertion) {
    auto i = find(insertion.key);
    if (i != _cache.end()) {
        return add_overriding(i, std::move(insertion)).then([this] () {
          return make_ready_future<bool>(true);
        });
    } else {
        return add_new(std::move(insertion)).then([this] () {
            return make_ready_future<bool>(false);
        });
    }
}

future<std::pair<ssd_item_ptr, temporary_buffer<char>>>
ssd_cache::get(const item_key& key) {
    auto i = find(key);
    if (i == _cache.end()) {
        return make_ready_future<std::pair<ssd_item_ptr, temporary_buffer<char>>>
            (std::make_pair(nullptr, temporary_buffer<char>()));
    }

    // get value from ssd
    return i->datum().then([this, i] (auto value) {
        uint64_t slab_size = _cfg->hive_cache_slab_size();
        if (value.size() <= slab_size) {
            ssd_item_ptr itemptr(&(*i));
            return make_ready_future<std::pair<ssd_item_ptr, temporary_buffer<char>>>
                (std::make_pair(std::move(itemptr), std::move(value)));
        } else {
            logger.error("[get] error, value.size({}) not equal to slab_size({})", value.size(), slab_size);
            return make_ready_future<std::pair<ssd_item_ptr, temporary_buffer<char>>>
                (std::make_pair(nullptr, temporary_buffer<char>()));
        }
    });
}


} //namespace hive 
