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

class test_message {
public:
    bytes    data;
    test_message() = default;
    test_message(bytes data_):data(std::move(data_))
    {}
};

std::ostream& operator<<(std::ostream& os, const test_message& chunk);

} //namespace hive

