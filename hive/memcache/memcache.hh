/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014-2015 Cloudius Systems
 */

#pragma once
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include <sstream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "memcached.hh"
#include "log.hh"
#include <unistd.h>

namespace bi = boost::intrusive;

namespace memcache {

static logging::logger logger("memcache");

extern __thread slab_allocator<item>* slab;
struct remote_origin_tag;
struct local_origin_tag;
class item;

template<typename T>
using optional = boost::optional<T>;

static constexpr double default_slab_growth_factor = 1.25;
static constexpr uint64_t default_slab_page_size = 1UL*MB;
static constexpr uint64_t default_per_cpu_slab_size = 0UL; // zero means reclaimer is enabled.

template<typename T>
using optional = boost::optional<T>;

class item : public slab_item_base {
public:
    using version_type = uint64_t;
    using time_point = expiration::time_point;
    using duration = expiration::duration;
    static constexpr uint8_t field_alignment = alignof(void*);
private:
    using hook_type = bi::unordered_set_member_hook<>;
    // TODO: align shared data to cache line boundary
    version_type _version;
    hook_type _cache_link;
    bi::list_member_hook<> _timer_link;
    size_t _key_hash;
    expiration _expiry;
    uint32_t _value_size;
    uint32_t _slab_page_index;
    uint16_t _ref_count;
    uint8_t _key_size;
    uint8_t _ascii_prefix_size;
    char _data[]; // layout: data=key, (data+key_size)=ascii_prefix, (data+key_size+ascii_prefix_size)=value.
    friend class cache;
public:
    item(uint32_t slab_page_index, item_key&& key, sstring&& ascii_prefix,
         temporary_buffer<char>&& value, expiration expiry, version_type version = 1)
        : _version(version)
        , _key_hash(key.hash())
        , _expiry(expiry)
        , _value_size(value.size())
        , _slab_page_index(slab_page_index)
        , _ref_count(0U)
        , _key_size(key.key().size())
        , _ascii_prefix_size(ascii_prefix.size())
    {
        assert(_key_size <= std::numeric_limits<uint8_t>::max());
        assert(_ascii_prefix_size <= std::numeric_limits<uint8_t>::max());
        // storing key
        memcpy(_data, key.key().c_str(), _key_size);
        // storing ascii_prefix
        memcpy(_data + align_up(_key_size, field_alignment), ascii_prefix.c_str(), _ascii_prefix_size);
        // storing value
        memcpy(_data + align_up(_key_size, field_alignment) + align_up(_ascii_prefix_size, field_alignment),
               value.get(), _value_size);
    }

    item(const item&) = delete;
    item(item&&) = delete;

    clock_type::time_point get_timeout() {
        return _expiry.to_time_point();
    }

    version_type version() {
        return _version;
    }

    const std::experimental::string_view key() const {
        return std::experimental::string_view(_data, _key_size);
    }

    const std::experimental::string_view ascii_prefix() const {
        const char *p = _data + align_up(_key_size, field_alignment);
        return std::experimental::string_view(p, _ascii_prefix_size);
    }

    const temporary_buffer<char> value() const {
        const char *p = _data + align_up(_key_size, field_alignment) +
                 align_up(_ascii_prefix_size, field_alignment);
        temporary_buffer<char> data(_value_size);
        std::copy_n(p, _value_size, data.get_write());
        return std::move(data);
    }

    //const std::experimental::string_view value() const {
    //    const char *p = _data + align_up(_key_size, field_alignment) +
    //        align_up(_ascii_prefix_size, field_alignment);
    //    return std::experimental::string_view(p, _value_size);
    //}

    size_t key_size() const {
        return _key_size;
    }

    size_t ascii_prefix_size() const {
        return _ascii_prefix_size;
    }

    size_t value_size() const {
        return _value_size;
    }

    optional<uint64_t> data_as_integral() {
        auto str = value().get();
        if (str[0] == '-') {
            return {};
        }

        auto len = _value_size;

        // Strip trailing space
        while (len && str[len - 1] == ' ') {
            len--;
        }

        try {
            return {boost::lexical_cast<uint64_t>(str, len)};
        } catch (const boost::bad_lexical_cast& e) {
            return {};
        }
    }

    // needed by timer_set
    bool cancel() {
        return false;
    }

    // Methods required by slab allocator.
    uint32_t get_slab_page_index() const {
        return _slab_page_index;
    }
    bool is_unlocked() const {
        return _ref_count == 1;
    }

    friend bool operator==(const item &a, const item &b) {
         return (a._key_hash == b._key_hash) &&
            (a._key_size == b._key_size) &&
            (memcmp(a._data, b._data, a._key_size) == 0);
    }

    friend std::size_t hash_value(const item &i) {
        return i._key_hash;
    }

    friend inline void intrusive_ptr_add_ref(item* it) {
        assert(it->_ref_count >= 0);
        ++it->_ref_count;
        if (it->_ref_count == 2) {
            slab->lock_item(it);
        }
    }

    friend inline void intrusive_ptr_release(item* it) {
        --it->_ref_count;
        if (it->_ref_count == 1) {
            slab->unlock_item(it);
        } else if (it->_ref_count == 0) {
            slab->free(it);
        }
        assert(it->_ref_count >= 0);
    }

    friend class item_key_cmp;
};

struct item_key_cmp
{
private:
    bool compare(const item_key& key, const item& it) const {
        return (it._key_hash == key.hash()) &&
            (it._key_size == key.key().size()) &&
            (memcmp(it._data, key.key().c_str(), it._key_size) == 0);
    }
public:
    bool operator()(const item_key& key, const item& it) const {
        return compare(key, it);
    }

    bool operator()(const item& it, const item_key& key) const {
        return compare(key, it);
    }
};

struct cache_stats {
    size_t _get_hits {};
    size_t _get_misses {};
    size_t _set_adds {};
    size_t _set_replaces {};
    size_t _cas_hits {};
    size_t _cas_misses {};
    size_t _cas_badval {};
    size_t _delete_misses {};
    size_t _delete_hits {};
    size_t _incr_misses {};
    size_t _incr_hits {};
    size_t _decr_misses {};
    size_t _decr_hits {};
    size_t _expired {};
    size_t _evicted {};
    size_t _bytes {};
    size_t _resize_failure {};
    size_t _size {};
    size_t _reclaims{};

    void operator+=(const cache_stats& o) {
        _get_hits += o._get_hits;
        _get_misses += o._get_misses;
        _set_adds += o._set_adds;
        _set_replaces += o._set_replaces;
        _cas_hits += o._cas_hits;
        _cas_misses += o._cas_misses;
        _cas_badval += o._cas_badval;
        _delete_misses += o._delete_misses;
        _delete_hits += o._delete_hits;
        _incr_misses += o._incr_misses;
        _incr_hits += o._incr_hits;
        _decr_misses += o._decr_misses;
        _decr_hits += o._decr_hits;
        _expired += o._expired;
        _evicted += o._evicted;
        _bytes += o._bytes;
        _resize_failure += o._resize_failure;
        _size += o._size;
        _reclaims += o._reclaims;
    }
};

class cache {
private:
    using cache_type = bi::unordered_set<item,
        bi::member_hook<item, item::hook_type, &item::_cache_link>,
        bi::power_2_buckets<true>,
        bi::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    static constexpr size_t initial_bucket_count = 1 << 10;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _cache;
    seastar::timer_set<item, &item::_timer_link> _alive;
    timer<clock_type> _timer;
    // delta in seconds between the current values of a wall clock and a clock_type clock
    clock_type::duration _wc_to_clock_type_delta;
    cache_stats _stats;
    timer<clock_type> _flush_timer;
private:
    size_t item_size(item& item_ref) {
        constexpr size_t field_alignment = alignof(void*);
        return sizeof(item) +
            align_up(item_ref.key_size(), field_alignment) +
            align_up(item_ref.ascii_prefix_size(), field_alignment) +
            item_ref.value_size();
    }

    size_t item_size(item_insertion_data& insertion) {
        constexpr size_t field_alignment = alignof(void*);
        auto size = sizeof(item) +
            align_up(insertion.key.key().size(), field_alignment) +
            align_up(insertion.ascii_prefix.size(), field_alignment) +
            insertion.data.size();
#ifdef __DEBUG__
        static bool print_item_footprint = true;
        if (print_item_footprint) {
            print_item_footprint = false;
            std::cout << __FUNCTION__ << ": " << size << "\n";
            std::cout << "sizeof(item)      " << sizeof(item) << "\n";
            std::cout << "key.size          " << insertion.key.key().size() << "\n";
            std::cout << "value.size        " << insertion.data.size() << "\n";
            std::cout << "ascii_prefix.size " << insertion.ascii_prefix.size() << "\n";
        }
#endif
        return size;
    }

    template <bool IsInCache = true, bool IsInTimerList = true, bool Release = true>
    void erase(item& item_ref);

    void expire() {
        using namespace std::chrono;
    
        //
        // Adjust the delta on every timer event to minimize an error caused
        // by a wall clock adjustment.
        //
        _wc_to_clock_type_delta =
            duration_cast<clock_type::duration>(clock_type::now().time_since_epoch() - system_clock::now().time_since_epoch());
    
        auto exp = _alive.expire(clock_type::now());
        while (!exp.empty()) {
            auto item = &*exp.begin();
            exp.pop_front();
            erase<true, false>(*item);
            _stats._expired++;
        }
        _timer.arm(_alive.get_next_timeout());
    }

    inline
    cache_iterator find(const item_key& key) {
        return _cache.find(key, std::hash<item_key>(), item_key_cmp());
    }

    template <typename Origin>
    inline
    cache_iterator add_overriding(cache_iterator i, item_insertion_data& insertion) {
        auto& old_item = *i;
        uint64_t old_item_version = old_item._version;

        erase(old_item);

        size_t size = item_size(insertion);
        auto new_item = slab->create(size, Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry, old_item_version + 1);
        intrusive_ptr_add_ref(new_item);

        auto insert_result = _cache.insert(*new_item);
        assert(insert_result.second);
        if (insertion.expiry.ever_expires() && _alive.insert(*new_item)) {
            _timer.rearm(new_item->get_timeout());
        }
        _stats._bytes += size;
        return insert_result.first;
    }

    template <typename Origin>
    inline
    void add_new(item_insertion_data& insertion) {
        size_t size = item_size(insertion);
        auto new_item = slab->create(size, Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry);
        intrusive_ptr_add_ref(new_item);
        auto& item_ref = *new_item;
        _cache.insert(item_ref);
        if (insertion.expiry.ever_expires() && _alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
        }
        _stats._bytes += size;
        maybe_rehash();
    }

    void maybe_rehash() {
        if (_cache.size() >= _resize_up_threshold) {
            auto new_size = _cache.bucket_count() * 2;
            auto old_buckets = _buckets;
            try {
                _buckets = new cache_type::bucket_type[new_size];
            } catch (const std::bad_alloc& e) {
                _stats._resize_failure++;
                return;
            }
            _cache.rehash(typename cache_type::bucket_traits(_buckets, new_size));
            delete[] old_buckets;
            _resize_up_threshold = _cache.bucket_count() * load_factor;
        }
    }
public:
    cache(uint64_t per_cpu_slab_size, uint64_t slab_page_size)
        : _buckets(new cache_type::bucket_type[initial_bucket_count])
        , _cache(cache_type::bucket_traits(_buckets, initial_bucket_count))
    {
        using namespace std::chrono;
    
        _wc_to_clock_type_delta =
            duration_cast<clock_type::duration>(clock_type::now().time_since_epoch() - system_clock::now().time_since_epoch());
    
        _timer.set_callback([this] { expire(); });
        _flush_timer.set_callback([this] { flush_all(); });
    
        // initialize per-thread slab allocator.
        slab = new slab_allocator<item>(default_slab_growth_factor, per_cpu_slab_size, slab_page_size,
                [this](item& item_ref) { erase<true, true, false>(item_ref); _stats._evicted++; });
    #ifdef __DEBUG__
        static bool print_slab_classes = true;
        if (print_slab_classes) {
            print_slab_classes = false;
            slab->print_slab_classes();
        }
    #endif
    }

    ~cache() {
       flush_all();
    }

    void flush_all() {
        _flush_timer.cancel();
        _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (item* it) {
            erase<false, true>(*it);
        });
    }

    void flush_at(uint32_t time) {
        auto expiry = expiration(get_wc_to_clock_type_delta(), time);
        _flush_timer.rearm(expiry.to_time_point());
    }

    template <typename Origin = local_origin_tag>
    bool set(item_insertion_data insertion);

    template <typename Origin = local_origin_tag>
    bool add(item_insertion_data insertion);

    template <typename Origin = local_origin_tag>
    bool replace(item_insertion_data insertion);

    bool remove(const item_key key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._delete_misses++;
            return false;
        }
        _stats._delete_hits++;
        auto& item_ref = *i;
        erase(item_ref);
        return true;
    }

    item_ptr get(const item_key key) {
//        std::cout << "memcache get key: " << key.key() << " cpuid: " << engine().cpu_id() << std::endl;
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._get_misses++;
            return nullptr;
        }
        _stats._get_hits++;
        auto& item_ref = *i;
        return item_ptr(&item_ref);
    }

    template <typename Origin = local_origin_tag>
    cas_result cas(item_insertion_data insertion, item::version_type version);

    size_t size() {
        return _cache.size();
    }

    size_t bucket_count() {
        return _cache.bucket_count();
    }

    cache_stats stats() {
        _stats._size = size();
        return _stats;
    }

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr, bool> incr(item_key key, uint64_t delta);

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr, bool> decr(item_key key, uint64_t delta);

    std::pair<unsigned, foreign_ptr<lw_shared_ptr<std::string>>> print_hash_stats() {
        static constexpr unsigned bits = sizeof(size_t) * 8;
        size_t histo[bits + 1] {};
        size_t max_size = 0;
        unsigned max_bucket = 0;
    
        for (size_t i = 0; i < _cache.bucket_count(); i++) {
            size_t size = _cache.bucket_size(i);
            unsigned bucket;
            if (size == 0) {
                bucket = 0;
            } else {
                bucket = bits - count_leading_zeros(size);
            }
            max_bucket = std::max(max_bucket, bucket);
            max_size = std::max(max_size, size);
            histo[bucket]++;
        }
    
        std::stringstream ss;
    
        ss << "size: " << _cache.size() << "\n";
        ss << "buckets: " << _cache.bucket_count() << "\n";
        ss << "load: " << sprint("%.2lf", (double)_cache.size() / _cache.bucket_count()) << "\n";
        ss << "max bucket occupancy: " << max_size << "\n";
        ss << "bucket occupancy histogram:\n";
    
        for (unsigned i = 0; i < (max_bucket + 2); i++) {
            ss << "  ";
            if (i == 0) {
                ss << "0: ";
            } else if (i == 1) {
                ss << "1: ";
            } else {
                ss << (1 << (i - 1)) << "+: ";
            }
            ss << histo[i] << "\n";
        }
        return {engine().cpu_id(), make_foreign(make_lw_shared<std::string>(ss.str()))};
    }

    future<> stop() { return make_ready_future<>(); }
    clock_type::duration get_wc_to_clock_type_delta() { return _wc_to_clock_type_delta; }
};

struct remote_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        //return ref;
        //FIXME: yutao (remote core use it)
        return std::move(ref);
    }
};

struct local_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return std::move(ref);
    }
};

template <bool IsInCache, bool IsInTimerList, bool Release>
void cache::erase(item& item_ref) {
    if (IsInCache) {
        _cache.erase(_cache.iterator_to(item_ref));
    }
    if (IsInTimerList) {
        if (item_ref._expiry.ever_expires()) {
            _alive.remove(item_ref);
        }
    }
    _stats._bytes -= item_size(item_ref);
    if (Release) {
        // memory used by item shouldn't be freed when slab is replacing it with another item.
        intrusive_ptr_release(&item_ref);
    }
}

template <typename Origin>
bool cache::set(item_insertion_data insertion) {
//    std::cout << "memcache set key: " << insertion.key.key() << " cpuid: " << engine().cpu_id() << std::endl;
    auto i = find(insertion.key);
    if (i != _cache.end()) {
        add_overriding<Origin>(i, insertion);
        _stats._set_replaces++;
        return true;
    } else {
        add_new<Origin>(insertion);
        _stats._set_adds++;
        return false;
    }
}

template <typename Origin>
bool cache::add(item_insertion_data insertion) {
    auto i = find(insertion.key);
    if (i != _cache.end()) {
        return false;
    }
    _stats._set_adds++;
    add_new<Origin>(insertion);
    return true;
}

template <typename Origin>
bool cache::replace(item_insertion_data insertion) {
    auto i = find(insertion.key);
    if (i == _cache.end()) {
        return false;
    }

    _stats._set_replaces++;
    add_overriding<Origin>(i, insertion);
    return true;
}

template <typename Origin>
cas_result cache::cas(item_insertion_data insertion, item::version_type version) {
    auto i = find(insertion.key);
    if (i == _cache.end()) {
        _stats._cas_misses++;
        return cas_result::not_found;
    }
    auto& item_ref = *i;
    if (item_ref._version != version) {
        _stats._cas_badval++;
        return cas_result::bad_version;
    }
    _stats._cas_hits++;
    add_overriding<Origin>(i, insertion);
    return cas_result::stored;
}

template <typename Origin>
std::pair<item_ptr, bool> cache::incr(item_key key, uint64_t delta) {
    auto i = find(key);
    if (i == _cache.end()) {
        _stats._incr_misses++;
        return {item_ptr{}, false};
    }
    auto& item_ref = *i;
    _stats._incr_hits++;
    auto value = item_ref.data_as_integral();
    if (!value) {
        return {boost::intrusive_ptr<item>(&item_ref), false};
    }
    sstring value_ = to_sstring(*value + delta);
    item_insertion_data insertion {
        .key = Origin::move_if_local(key),
        .ascii_prefix = sstring(item_ref.ascii_prefix().data(), item_ref.ascii_prefix_size()),
        .data = temporary_buffer<char>(value_.c_str(), value_.length()),
        .expiry = item_ref._expiry
    };
    i = add_overriding<local_origin_tag>(i, insertion);
    return {boost::intrusive_ptr<item>(&*i), true};
}

template <typename Origin>
std::pair<item_ptr, bool> cache::decr(item_key key, uint64_t delta) {
    auto i = find(key);
    if (i == _cache.end()) {
        _stats._decr_misses++;
        return {item_ptr{}, false};
    }
    auto& item_ref = *i;
    _stats._decr_hits++;
    auto value = item_ref.data_as_integral();
    if (!value) {
        return {boost::intrusive_ptr<item>(&item_ref), false};
    }
    sstring value_ = to_sstring(*value - std::min(*value, delta));
    item_insertion_data insertion {
        .key = Origin::move_if_local(key),
        .ascii_prefix = sstring(item_ref.ascii_prefix().data(), item_ref.ascii_prefix_size()),
        .data = temporary_buffer<char>(value_.c_str(), value_.length()),
        .expiry = item_ref._expiry
    };
    i = add_overriding<local_origin_tag>(i, insertion);
    return {boost::intrusive_ptr<item>(&*i), true};
}

} /* namespace memcache */
