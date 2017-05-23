#pragma once

#include <stdint.h>

namespace hive {

using segment_id_type = uint64_t;
using position_type = uint32_t;

struct replay_position {
    static const constexpr size_t max_cpu_bits = 10; // 1024 cpus. should be enough for anyone
    static const constexpr size_t max_ts_bits = 8 * sizeof(segment_id_type) - max_cpu_bits;
    static const constexpr segment_id_type ts_mask = (segment_id_type(1) << max_ts_bits) - 1;
    static const constexpr segment_id_type cpu_mask = ~ts_mask;

    segment_id_type id;
    position_type pos;

    replay_position(segment_id_type i = 0, position_type p = 0)
        : id(i), pos(p)
    {}

    replay_position(unsigned shard, segment_id_type i, position_type p = 0)
            : id((segment_id_type(shard) << max_ts_bits) | i), pos(p)
    {
        if (i & cpu_mask) {
            throw std::invalid_argument("base id overflow: " + std::to_string(i));
        }
    }

    bool operator<(const replay_position & r) const {
        return id < r.id ? true : (r.id < id ? false : pos < r.pos);
    }
    bool operator<=(const replay_position & r) const {
        return !(r < *this);
    }
    bool operator==(const replay_position & r) const {
        return id == r.id && pos == r.pos;
    }
    bool operator!=(const replay_position & r) const {
        return !(*this == r);
    }

    unsigned shard_id() const {
        return unsigned(id >> max_ts_bits);
    }
    segment_id_type base_id() const {
        return id & ts_mask;
    }
    replay_position base() const {
        return replay_position(base_id(), pos);
    }

    template <typename Describer>
    auto describe_type(Describer f) { return f(id, pos); }

    friend std::ostream& operator<<(std::ostream& out, const replay_position& s);
};

} //namespace hive
