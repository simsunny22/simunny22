#ifndef __HEADERS_HH__
#define __HEADERS_HH__
//#pragma once 

#include <map>
#include "seastar/core/sstring.hh"

namespace hive {

class Headers {
public:
    typedef std::map<sstring,sstring> Map;
    
    Headers(){}
    ~Headers(){}
    Headers(Headers&& headers) noexcept;
    Headers& operator=(Headers&& headers) noexcept;
    sstring const header(sstring const& name) const;
    Map::const_iterator begin() const { return header_.begin(); }
    Map::const_iterator end() const { return header_.end(); }
    void headerIs(sstring const& name, sstring const& value);

    static sstring const HOST;
    static sstring const CONTENT_LENGTH;
    static sstring const ACCEPT_ENCODING;
    static sstring const CONNECTION;
private:
    Map header_;
};

} //namespace hive

#endif //__HEADERS_HH__
