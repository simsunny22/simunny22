#pragma once

#include <stdexcept>
#include "core/sstring.hh"
#include "core/print.hh"
#include "bytes.hh"

namespace hive {

enum class exception_code : int32_t {
    // 2xx: problem validating the request
    INVALID = 0x2000, 
    HIVE_IS_NOT_SERVING = 0x2001,

    // 3xx: hive exception
    TEST = 0x3000,
    SYNC_COMMITLOG_TIMEOUT = 0x3001,
    WRITE_TIMEOUT   = 0x1100,
    START_SLAVE_JOURNAL_TIMEOUT = 0x3000,

    // 4xx: not found exception
    NOT_FOUND = 0x4000, 

    //5xx: create file exception
    CREATE_FILE_ERROR = 0x5000,
    

    // 6xx: db error exception
    DB_ERROR = 0x6000, 

    // 7xx: metadata cache error exception
    METADATA_CACHE_ERROR = 0x7000,
};

class hive_exception : public std::exception { 
private:
    exception_code _code;
    sstring _msg;
protected:
    template<typename... Args>
    inline sstring prepare_message(const char* fmt, Args&&... args) noexcept {
        try {
            return sprint(fmt, std::forward<Args>(args)...);
        } catch (...) {
            return sstring();
        }
    }
public:
    hive_exception(exception_code code, sstring msg) noexcept
        : _code(code)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
    exception_code code() const { return _code; }
    sstring get_message() const { return what(); }
};

class test_exception : public hive_exception {
public:
    test_exception(sstring msg) noexcept
        : hive_exception{hive::exception_code::TEST, std::move(msg)}
    { }
};

class hive_request_timeout_exception : public hive_exception {
public:
    sstring volume_id;
    int32_t block_for; 
    int32_t received;
    hive_request_timeout_exception(
        hive::exception_code code, 
        sstring volume_id, 
        int32_t block_for, 
        int32_t received) noexcept
        : hive_exception{code, prepare_message("Operation timed out, volume_id:{}, block_for:{}, received:{}."
                                              , volume_id, block_for, received)}
        , volume_id(volume_id) 
        , block_for(block_for) 
        , received(received)
    { }
};

class not_found_exception: public hive_exception {
public:
    not_found_exception(sstring message) noexcept
        : hive_exception(exception_code::NOT_FOUND, std::move(message))
    { }
};

class db_error_exception: public hive_exception {
public:
    db_error_exception(sstring message) noexcept
        : hive_exception(exception_code::DB_ERROR, std::move(message))
    { }
};

class metadata_cache_error_exception: public hive_exception {
public:
    metadata_cache_error_exception(sstring message) noexcept
        : hive_exception(exception_code::METADATA_CACHE_ERROR, std::move(message))
    { }
};


class create_file_exception: public hive_exception {
public:
    create_file_exception(sstring message) noexcept
        : hive_exception(exception_code::CREATE_FILE_ERROR, std::move(message))
    { }
};

class request_validation_exception : public hive_exception {
public:
    using hive_exception::hive_exception;
};

class invalid_request_exception : public request_validation_exception {
public:
    invalid_request_exception(sstring cause) noexcept
        : request_validation_exception(exception_code::INVALID, std::move(cause))
    { }
};

class hive_is_not_serving: public request_validation_exception {
public:
    hive_is_not_serving(sstring cause) noexcept
        : request_validation_exception(exception_code::HIVE_IS_NOT_SERVING, std::move(cause))
    { }
};

//tododl:yellow define for compile the old code
struct journal_write_timeout_exception : public hive::hive_request_timeout_exception {
    journal_write_timeout_exception(int32_t received) 
        :hive_request_timeout_exception(hive::exception_code::WRITE_TIMEOUT, "tmp for complile", 0, received)
    {}
};

struct sync_commitlog_timeout_exception : public hive::hive_request_timeout_exception {
    sync_commitlog_timeout_exception(sstring volume_id, int32_t block_for, int32_t received) noexcept
        :hive_request_timeout_exception(exception_code::SYNC_COMMITLOG_TIMEOUT, volume_id, block_for, received)
    { }
};



struct datum_write_timeout_exception : public hive::hive_request_timeout_exception {
    datum_write_timeout_exception(sstring volume_id, int32_t block_for, int32_t received) 
        :hive_request_timeout_exception(hive::exception_code::WRITE_TIMEOUT, volume_id, block_for, received)
    { }
};

} //namespace hive
