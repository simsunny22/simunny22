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

enum class datum_origin{INIT, POPULATE, CACHE, EXTENT_STORE};

class datum{
public:
    uint64_t seq = 0;
    datum_origin origin = datum_origin::INIT;
    lw_shared_ptr<bytes> data= nullptr;
    datum(){}
    datum(uint64_t seq_, datum_origin origin_):seq(seq_), origin(origin_){}
    datum(uint64_t seq_, datum_origin origin_, lw_shared_ptr<bytes> data_):seq(seq_), origin(origin_), data(data_){}
    temporary_buffer<char> serialize();
    void deserialize(const char* buf, size_t size);
};

std::ostream& operator<<(std::ostream& os, const datum& datum);

} //namespace hive

