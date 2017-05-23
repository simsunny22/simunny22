#pragma once

#include "hive/stream/stream_state.hh"
#include <seastar/core/sstring.hh>
#include <exception>

namespace hive {

class stream_exception : public std::exception {
public:
    stream_state state;
    sstring msg;
    stream_exception(stream_state s, sstring m)
        : state(std::move(s))
        , msg(std::move(m)) {
    }
    virtual const char* what() const noexcept override {
        return msg.c_str();
    }
};

} // namespace hive 
