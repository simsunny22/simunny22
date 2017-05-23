//#include "http/Common.hpp"
#include "hive/http/request.hh"

namespace hive {

sstring const Request::header(sstring const& name) const {
    return headers_.header(name);
}

void Request::methodIs(Method method) {
    method_ = method;
}

void Request::uriIs(Uri const& uri) {
    uri_ = uri;
}

void Request::dataIs(sstring const& data) {
    data_ = data;
}

void Request::headerIs(sstring const& name, sstring const& value) {
    headers_.headerIs(name, value);
}

}  //namespace hive
