#pragma once

#include "core/sstring.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include <vector>

#include "hive/stream/migrate_params_entry.hh"

namespace hive {

class stream_request {
public:
    migrate_params_entry params;

    stream_request() = default;
    stream_request(migrate_params_entry params_): params(params_){} 

    friend std::ostream& operator<<(std::ostream& os, const stream_request& r);
};

} // namespace hive 
