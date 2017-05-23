#pragma once

#include <iostream>
#include <functional>
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/future.hh"
#include "bytes.hh"
#include <vector>
#include <ostream>


namespace hive {

class datum_differ {
public:
    lw_shared_ptr<bytes> data;
    uint64_t offset;
    uint64_t length;
    datum_differ() = default;
    datum_differ(lw_shared_ptr<bytes> data_, uint64_t offset_):data(data_),offset(offset_)
    {
        length = data->size();
    }
    datum_differ(lw_shared_ptr<bytes> data_, uint64_t offset_, uint64_t length_):data(data_),offset(offset_), length(length_)
    {}
};

std::ostream& operator<<(std::ostream& os, const datum_differ& differ);

} //namespace hive

