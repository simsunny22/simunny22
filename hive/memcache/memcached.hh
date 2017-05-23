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
#ifndef __MEMCACHED_HH
#define __MEMCACHED_HH

#include <boost/intrusive_ptr.hpp>
#include "core/sstring.hh"
#include "core/timer-set.hh"

namespace memcache {

class item;
class cache;

using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
//using item_ptr = boost::intrusive_ptr<item>;

enum class cas_result {
    not_found, stored, bad_version
};

class item_key {
private:
    sstring _key;
    size_t _hash;
public:
    item_key() = default;
    item_key(item_key&) = default;
    item_key(sstring key)
        : _key(key)
        , _hash(std::hash<sstring>()(key))
    {}
    item_key(item_key&& other)
        : _key(std::move(other._key))
        , _hash(other._hash)
    {
        other._hash = 0;
    }
    size_t hash() const {
        return _hash;
    }
    const sstring& key() const {
        return _key;
    }
    bool operator==(const item_key& other) const {
        return other._hash == _hash && other._key == _key;
    }
    void operator=(item_key&& other) {
        _key = std::move(other._key);
        _hash = other._hash;
        other._hash = 0;
    }
};

using clock_type = lowres_clock;

//
// "Expiration" is a uint32_t value.
// The minimal value of _time is when "expiration" is set to (seconds_in_a_month
// + 1).
// In this case _time will have a value of
//
// (seconds_in_a_month + 1 - Wall_Clock_Time_Since_Epoch)
//
// because lowres_clock now() initialized to zero when the application starts.
//
// We will use a timepoint at LLONG_MIN to represent a "never expire" value
// since it will not collide with the minimum _time value mentioned above for
// about 290 thousand years to come.
//
static constexpr clock_type::time_point never_expire_timepoint = clock_type::time_point(clock_type::duration::min());

struct expiration {
    using time_point = clock_type::time_point;
    using duration   = time_point::duration;

    static constexpr uint32_t seconds_in_a_month = 60U * 60 * 24 * 30;
    time_point _time = never_expire_timepoint;

    expiration() {}

    expiration(clock_type::duration wc_to_clock_type_delta, uint32_t s) {
        using namespace std::chrono;

        static_assert(sizeof(clock_type::duration::rep) >= 8, "clock_type::duration::rep must be at least 8 bytes wide");

        if (s == 0U) {
            return; // means never expire.
        } else if (s <= seconds_in_a_month) {
            _time = clock_type::now() + seconds(s); // from delta
        } else {
            //
            // seastar::reactor supports only a monotonic clock at the moment
            // therefore this may make the elements with the absolute expiration
            // time expire at the wrong time if the wall clock has been updated
            // during the expiration period. However the original memcached has
            // the same weakness.
            //
            // TODO: Fix this when a support for system_clock-based timers is
            // added to the seastar::reactor.
            //
            _time = time_point(seconds(s) + wc_to_clock_type_delta); // from real time
        }
    }

    bool ever_expires() {
        return _time != never_expire_timepoint;
    }

    time_point to_time_point() {
        return _time;
    }
};

// item insertion data
struct item_insertion_data {
    item_key key;
    sstring ascii_prefix;
    temporary_buffer<char> data;
    expiration expiry;
};

}

namespace std {

template <>
struct hash<memcache::item_key> {
    size_t operator()(const memcache::item_key& key) {
        return key.hash();
    }
};

} /* namespace std */

#endif
