#ifndef __URI_HH__
#define __URI_HH__
//#pragma once

//#include "http/Common.hpp"
#include "seastar/core/sstring.hh"

namespace hive {

class Authority {
public:
    Authority(sstring const& user, sstring const& host, uint16_t port);  
    Authority();

    sstring const& user() const { return user_; }
    sstring const& host() const { return host_; }
    uint16_t port() const { return port_; }

    void userIs(sstring const& user);
    void hostIs(sstring const& host);
    void portIs(uint16_t port);
private:
    sstring user_;
    sstring host_;
    uint16_t port_;
};

class Uri {
public:
    Uri(char* const value);
    Uri(sstring const& value);
    Uri();
    
    sstring const& scheme() const { return scheme_; } 
    Authority const& authority() const { return authority_; }
    sstring const& path() const { return path_; }
    sstring const& host() const { return authority_.host(); }
    uint16_t port() const { return authority_.port(); }

    void schemeIs(sstring const& scheme);
    void authorityIs(Authority const& authority);
    void pathIs(sstring const& path);
private:
    sstring scheme_;
    Authority authority_;
    sstring path_;
};

} //namespace hive
#endif // __URI_HH__
