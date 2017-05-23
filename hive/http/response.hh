
#ifndef __RESPONSE_HH__
#define __RESPONSE_HH__
//#pragma once

#include "hive/http/headers.hh"
#include "hive/http/cookies.hh"
#include "seastar/core/sstring.hh"

namespace hive {

class Response {
public:
    enum Status { 
        INVALID_CODE = 0,
        CONTINUE = 100,
        SWITCHING_PROTOCOLS = 101,
        OK = 200,
        CREATED = 201,
        ACCEPTED = 202,
        NON_AUTHORITATIVE_INFO = 203,
        NO_CONTENT = 204,
        RESET_CONTENT = 205 ,
        PARTIAL_CONTENT = 206,
        MULTIPLE_CHOICES = 300,
        MOVED_PERMANENTLY = 301,
        FOUND = 302,
        SEE_OTHER = 303,
        NOT_MODIFIED = 304,
        USE_PROXY = 305,
        TEMPORARY_REDIRECT = 307,
        BAD_REQUEST = 400,
        UNAUTHORIZED = 401,
        PAYMENT_REQUIRED = 402 ,
        FORBIDDEN = 403,
        NOT_FOUND = 404,
        METHOD_NOT_ALLOWED = 405,
        NOT_ACCEPTABLE = 406,
        PROXY_AUTHENTICATION_REQUIRED = 407,
        REQUEST_TIMEOUT = 408,
        CONFLICT = 409,
        GONE = 410,
        LENGTH_REQUIRED = 411,
        PRECONDITION_FAILED = 412,
        REQUEST_ENTITY_TOO_LARGE = 413,
        UNSUPPORTED_MEDIA_TYPE = 415,
        REQUESTED_RANGE_NOT_SATISFIABLE = 416,
        EXPECTATION_FAILED = 417,
        INTERNAL_SERVER_ERROR = 500,
        NOT_IMPLEMENTED = 501,
        BAD_GATEWAY = 502,
        SERVICE_UNAVAILABLE = 503,
        GATEWAY_TIMEOUT = 504,
        VERSION_NOT_SUPPORTED = 505,
    };

    Response(sstring const& text);
    Response() {}
    ~Response() {}
    Response(Response&& response) noexcept;
    Response& operator=(Response&& response) noexcept;

    operator bool() const noexcept {
        return INVALID_CODE != status_;
    }

	  bool is_success() const noexcept {
	      return status_ / 100U == 2U;
	  }

    Status status() const { return status_; }
    sstring const& data() const { return data_; }
    sstring const header(sstring const& name) const;
    Cookie const cookie(sstring const& name) const;

    void statusIs(Status status);
    void versionIs(sstring const& version);
    void dataIs(sstring const& data);
    void headerIs(sstring const& name, sstring const& value);
    void cookieIs(Cookie const& cookie);

private:
    Status status_ = INVALID_CODE;
    sstring data_;
    Headers headers_;
    Cookies cookies_;
};


} //namespace hive

#endif //__RESPONSE_HH__
