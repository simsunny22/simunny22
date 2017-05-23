#ifndef __REQUEST_HH__
#define __REQUEST_HH__
//#pragma once

//#include "http/Common.hpp"
#include "seastar/core/sstring.hh"
#include "hive/http/headers.hh"
#include "hive/http/uri.hh"

namespace hive {

class Request {
public:
    enum Method { GET, HEAD, POST, PUT, DELETE, TRACE, CONNECT };

    Method method() const { return method_; }
    Uri const& uri() const { return uri_; }
    sstring const& path() const { return uri_.path(); }
    sstring const& data() const { return data_; }
    sstring const header(sstring const& name) const;
    Headers const& headers() const { return headers_; }

    void methodIs(Method method);
    void uriIs(Uri const& path);
    void dataIs(sstring const& data);
    void headerIs(sstring const& name, sstring const& value);

private:
    Method method_ = GET;
    Uri uri_;
    sstring data_;
    Headers headers_;
};

} //namespace hive
#endif //__REQUEST_HH__
