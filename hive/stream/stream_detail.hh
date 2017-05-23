#pragma once

#include "query-request.hh"
#include "mutation_reader.hh"
#include "utils/UUID.hh"
#include <vector>
#include "range.hh"
#include "dht/i_partitioner.hh"

namespace hive {

struct stream_detail {
    using UUID = utils::UUID;
    UUID cf_id;
    stream_detail() = default;
    stream_detail(UUID cf_id_)
        : cf_id(std::move(cf_id_)) {
    }
};

} // namespace hive 
