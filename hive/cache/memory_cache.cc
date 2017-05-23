#include <algorithm>
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

#include "log.hh"
#include "core/future-util.hh"
#include "core/future.hh"
#include "core/align.hh"
#include "hive/cache/item_key.hh"
#include "hive/cache/memory_slab.hh"
#include "hive/cache/memory_cache.hh"
#include "hive/cache/cache_flush_manager.hh"
#include "hive/cache/cache.hh"

namespace hive {

static logging::logger logger("memory_cache");

static __thread memory_slab_allocator<item>* st_memory_slab = nullptr;
static __thread cache_flush_manager* st_cache_flush_manger = nullptr; 
static __thread ssd_cache* st_ssd_cache = nullptr; 

////////////////////// 
// for class item
//////////////////////
item::item(item_key&& key, temporary_buffer<char>&& value, version_type version, bool flush2ssd)
: _version(version)
, _key_hash(key.hash())
, _value_size(value.size())
, _ref_count(0U)
, _key_size(key.key().size())
, _frequence_count(0U)
, _flush_queued(flush2ssd) {
    memcpy(_data, key.key().c_str(), _key_size);
    memcpy(_data + align_up(_key_size, field_alignment), value.get(), _value_size);
    // calculate value digest
    //CryptoPP::Weak::MD5 hash;
    //hash.CalculateDigest(reinterpret_cast<unsigned char*>(_value_digest.data()), reinterpret_cast<const unsigned char*>(value.begin()), value.size());
}

const std::experimental::string_view 
item::key() const {
    return std::experimental::string_view(_data, _key_size);
}

temporary_buffer<char> item::datum() {
    char *p = _data + align_up(_key_size, field_alignment);
    temporary_buffer<char> data(_value_size);
    std::copy_n(p, _value_size, data.get_write());
    //judge datum md5
    //CryptoPP::Weak::MD5 hash;
    //item::type digest;
    //hash.CalculateDigest(reinterpret_cast<unsigned char*>(digest.data()), reinterpret_cast<const unsigned char*>(data.begin()), data.size());
    //logger.info("[cache item] datum item md5:{} data_md5:{}", _value_digest, digest);
    //assert(digest == _value_digest);

    return data;
}

void item::touch() {
    if(nullptr != st_ssd_cache){
        st_ssd_cache->touch(item_key{key().to_string()});
    }
}

uint64_t item::incr_frequence_count() {
    ++_frequence_count;
    if (!_flush_queued && _frequence_count >= default_refrequence_count) {
        // submit item to flush manager
        db::config& cfg = get_local_cache().get_config();
        if (cfg.hive_cache_second_level_enabled()) {
            st_cache_flush_manger->submit(std::move(item_ptr(this)));
        }
    }
    return _frequence_count;
}

bool item::set_cache_flush_manager_queued(bool flushed) {
    return _flush_queued = flushed;
}

bool item::cache_flush_manager_queued() const {
    return _flush_queued;
}

//for boost::intrusive_ptr
bool item::is_unlocked() const {
    return _ref_count == 1;
}

size_t item::key_size() const {
    return _key_size;
}

size_t item::value_size() const {
    return _value_size;
}

uint64_t item::frequence_count() const {
    return _frequence_count;
}


//////////////////
// for global 
/////////////////
std::ostream& operator<<(std::ostream& out, const cache_stats& stats){
    out <<"{cache_stats:[";
    out <<"get_total_counts:"         << stats._get_total_counts;
    out <<", set_total_counts:"       << stats._set_total_counts;
    out <<", first_level_get_hits:"   << stats._first_level_get_hits;
    out <<", first_level_get_misses:" << stats._first_level_get_misses;
    out <<", second_level_get_hits:"  << stats._second_level_get_hits;
    out <<", second_level_get_misses:"<< stats._second_level_get_misses;
    out <<", set_adds:"               << stats._set_adds;
    out <<", set_replaces:"           << stats._set_replaces;
    out <<", delete_misses:"          << stats._delete_misses;
    out <<", delete_hits:"            << stats._delete_hits;
    out <<", first_level_evicted:"    << stats._first_level_evicted;
    out <<", second_level_evicted:"   << stats._second_level_evicted;
    out <<", bytes:"                  << stats._bytes;
    out <<", resize_failure:"         << stats._bytes;
    out <<", size:"                   << stats._size;
    out <<"]}";
    return out;
}

bool operator==(const item& a, const item &b) { 
    return (a._key_hash == b._key_hash) 
        && (a._key_size == b._key_size) 
        && (memcmp(a._data, b._data, a._key_size) == 0); 
}

size_t hash_value(const item &i) {
    return i._key_hash;
}

void intrusive_ptr_add_ref(item* it) {
    assert(it->_ref_count >= 0);
    ++it->_ref_count;
    if (2 == it->_ref_count) {
        st_memory_slab->lock_item(it); 
    }
}

void intrusive_ptr_release(item* it) {
    --it->_ref_count;
    if (1 == it->_ref_count) {
        st_memory_slab->unlock_item(it);
    }else if(0 == it->_ref_count) {
        st_memory_slab->free(it);
    }
    assert(it->_ref_count >= 0);
}

inline uint64_t item_size(uint64_t slab_size) {
    constexpr uint64_t field_alignment = alignof(void*);
    return sizeof(item)
         + align_up(default_key_length, field_alignment) 
         + align_up(slab_size, field_alignment);
}

/////////////////////
// for class cache
////////////////////
memory_cache::memory_cache(const db::config& cfg)
    : _buckets(new cache_type::bucket_type[initial_bucket_count])
    , _cache(cache_type::bucket_traits(_buckets, initial_bucket_count)) 
{
    uint64_t first_level_size = cfg.hive_cache_first_level_size_per_cpu();
    uint64_t slab_size = cfg.hive_cache_slab_size();
    auto erase_func = [this](item& item_ref) {  
                          erase<true, false>(item_ref); 
                          _stats._first_level_evicted++; 
                      };

    st_memory_slab = new memory_slab_allocator<item>(first_level_size, item_size(slab_size), std::move(erase_func));
   
    //ssd_cache need
    if(cfg.hive_cache_second_level_enabled()){
        st_ssd_cache = new ssd_cache(cfg);
        st_cache_flush_manger = new cache_flush_manager(this);
        st_cache_flush_manger->start(default_task_count);
    }

    //start on_timer callback
    auto print_in_s = cfg.periodic_print_cache_stats_in_s();
    if (print_in_s > 0) {
        _timer.set_callback(std::bind(&memory_cache::on_timer, this));
        _timer.arm(lowres_clock::now()+std::chrono::seconds(1)
                 , std::experimental::optional<lowres_clock::duration> {
                   std::chrono::seconds(print_in_s)});
    }
}

memory_cache::~memory_cache() {
    if(nullptr != st_memory_slab){
        delete(st_memory_slab);
    }
    if(nullptr != st_ssd_cache){
        delete(st_ssd_cache);
    }
    if(nullptr != st_cache_flush_manger){
        delete(st_cache_flush_manger);
    }
}

future<> memory_cache::init(){
    if(nullptr != st_ssd_cache){
        return st_ssd_cache->init(); 
    }
    return make_ready_future<>();
}

future<> memory_cache::stop(){
   return flush_all().then([this](){
       if(nullptr != st_cache_flush_manger){
           return st_cache_flush_manger->stop(); 
       }else{
           return make_ready_future<>(); 
       }
   }).then([](){
       if(nullptr != st_ssd_cache){
           return st_ssd_cache->stop(); 
       }else {
           return make_ready_future<>();    
       }
   });
}

void memory_cache::on_timer() {
    auto shard_id = engine().cpu_id();
    logger.error("shard:{}, periodic_print_cache_stats_in_s, start, ================", shard_id);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> get total counts: {}", shard_id, _stats._get_total_counts);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> set total counts: {}", shard_id, _stats._set_total_counts);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> first level hits: {}", shard_id, _stats._first_level_get_hits);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> first level missed: {}", shard_id, _stats._first_level_get_misses);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> second level hits: {}", shard_id, _stats._second_level_get_hits);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, ==> second level missed: {}", shard_id, _stats._second_level_get_misses);
    logger.error("shard:{}, periodic_print_cache_stats_in_s, end,   ================", shard_id);
}

inline memory_cache::cache_iterator
memory_cache::find(const item_key& key) {
    return _cache.find(key, std::hash<item_key>(), item_key_cmp());
}

inline memory_cache::cache_iterator 
memory_cache::add_overriding(memory_cache::cache_iterator i, item_insertion_data& insertion) {
    auto& old_item = *i;
    erase(old_item);

    auto new_item = st_memory_slab->create(std::move(insertion.key)
                                  , std::move(insertion.data)
                                  , insertion.version);
    intrusive_ptr_add_ref(new_item);
    auto& item_ref = *new_item;
    auto ret = _cache.insert(item_ref);
    if(!ret.second){
        logger.error("[add_overriding] error, insert _cache failed, _cache.size:{}", _cache.size()); 
        erase<false, true>(item_ref);
        throw std::runtime_error("memory_cache::add_overriding insert _cache failed");
    }
    return ret.first;
}

inline void memory_cache::add_new(item_insertion_data& insertion) {
    auto new_item = st_memory_slab->create(std::move(insertion.key)
                                  , std::move(insertion.data)
                                  , insertion.version);
    intrusive_ptr_add_ref(new_item);
    auto& item_ref = *new_item;
    auto ret = _cache.insert(item_ref);
    if(!ret.second){
        logger.error("[add_new] error, insert _cache failed, _cache.size:{}", _cache.size()); 
        erase<false, true>(item_ref);
        throw std::runtime_error("memory_cache::add_new insert _cache failed");
    }

    maybe_rehash();
}

template<bool IsInCache, bool Release>
void memory_cache::erase(item& item_ref) {
    if (IsInCache) {
        _cache.erase(_cache.iterator_to(item_ref));
    }
    if (Release) {
        intrusive_ptr_release(&item_ref);
    }
}

void memory_cache::maybe_rehash() {
    if (_cache.size() >= _resize_up_threshold) {
        auto new_size = _cache.bucket_count() * 2;
        auto old_buckets = _buckets;
        try {
            _buckets = new cache_type::bucket_type[new_size];
        } catch (...) {
            _stats._resize_failure++;
            logger.error("[may_be_rehash] error, exception:{}", std::current_exception());
            return;
        }
        _cache.rehash(typename cache_type::bucket_traits(_buckets, new_size));
        delete[] old_buckets;
        _resize_up_threshold = _cache.bucket_count() * load_factor;
    }
}

future<> memory_cache::flush_all() {
    // flush memory
    _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (item* it) {
        erase<false>(*it);
    });
    
    // flush ssd
    if(nullptr != st_ssd_cache){
        return st_ssd_cache->flush_all();
    }else {
        return make_ready_future<>(); 
    }
}

future<bool> memory_cache::set(item_insertion_data& insertion) {
    _stats._set_total_counts++; 
    auto i = find(insertion.key);
    if (i != _cache.end()) {
        add_overriding(i, insertion);
        _stats._set_replaces++;
        return make_ready_future<bool>(true);
    } else {
        add_new(insertion);
        _stats._set_adds++;
        return make_ready_future<bool>(false); 
    }
}

future<item_ptr> memory_cache::get(const item_key& key) {
    _stats._get_total_counts++; 
    auto i = find(key);
    if (i == _cache.end()) {
        _stats._first_level_get_misses++;
        return get_from_ssd_cache(key);
    }else{
        _stats._first_level_get_hits++;
        i->incr_frequence_count();
        
        item_ptr itemptr(&(*i));
        return make_ready_future<item_ptr>(std::move(itemptr));
    }
}

future<item_ptr> memory_cache::get_from_ssd_cache(const item_key& key) {
    if(nullptr == st_ssd_cache){
        return make_ready_future<item_ptr>(nullptr);
    }
    
    return st_ssd_cache->get(key).then([this, key] (auto ret) mutable {
        if (!ret.first) {
            return make_ready_future<bool>(false);
        }
        //insert to memory and get it back
        item_insertion_data data;
        data.key = key;
        data.data = std::move(ret.second);
        data.version = ret.first->version();
        //insert first
        return this->set(data).then([](auto ret){ 
            return make_ready_future<bool>(true); 
        });
    }).then_wrapped([this, &key] (auto f) {
        try{
            auto result = f.get0(); 
            if(result){
                _stats._second_level_get_hits++; 
                return this->get(key);
            }
        }catch(...){
            logger.error("[get] error, exception:{}", std::current_exception());  
        } 
        _stats._second_level_get_misses++;
        return make_ready_future<item_ptr>(nullptr);
    });
}

future<> memory_cache::run_flush(item_insertion_data insertion) {
    if(nullptr == st_ssd_cache){
        return make_ready_future<>(); 
    }

    return st_ssd_cache->set(std::move(insertion)).then([](auto success){
        if(success){
            return make_ready_future<>(); 
        }else{
            return make_exception_future<>(std::runtime_error("set ssd cache failed"));
        }
    });
}

bool memory_cache::set_cache_flush_manager_queued(const item_key key, bool queued) {
    auto i = find(key);
    if (i == _cache.end()) {
        logger.warn("[{}] can not find the item, key:{}", __func__, key.key());
        //when call the function remove of cache_flush_manager maybe set a not exists key
        return false;
    }
    return i->set_cache_flush_manager_queued(queued);
}

//FIXME: Is need use delete hand off
future<bool> memory_cache::remove(const item_key key) {
    auto i = find(key);
    if (i == _cache.end()) {
        _stats._delete_misses++;
    } else {
        auto& item_ref = *i;
        erase(item_ref);
        _stats._delete_hits++;
    }
   
    if(nullptr == st_cache_flush_manger || nullptr == st_ssd_cache){
        return make_ready_future<bool>(true); 
    }
    
    return st_cache_flush_manger->remove(key).then([key]()mutable{
        return st_ssd_cache->remove(key).then([key](auto ret) {
            return make_ready_future<bool>(ret);
        });
    });
}

size_t memory_cache::size() {
    return _cache.size();
}

size_t memory_cache::bucket_count() {
    return _cache.bucket_count();
}

} // namespace hive 
