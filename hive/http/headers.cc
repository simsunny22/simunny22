//#include "http/Common.hpp"
#include "hive/http/headers.hh"

namespace hive {

sstring const Headers::HOST("Host");
sstring const Headers::CONTENT_LENGTH("Content-Length");
sstring const Headers::ACCEPT_ENCODING("Accept-Encoding");
sstring const Headers::CONNECTION("Connection");

sstring const Headers::header(sstring const& name) const {
    auto i = header_.find(name);
    return (i == header_.end()) ? "" : i->second;
}

Headers::Headers(Headers&& headers) noexcept{
    if(this != &headers) {
        header_ = std::move(headers.header_);
    }
}
Headers& Headers::operator=(Headers&& headers) noexcept{
    if(this != &headers) {
        header_ = std::move(headers.header_);
    }
    return *this;
}

void Headers::headerIs(sstring const& name, sstring const& value) {
    header_.emplace(name, value);
}

}  //namespace hive
