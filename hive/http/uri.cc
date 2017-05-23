//#include "http/Common.hpp"

#include "seastar/core/sstring.hh"
#include "hive/http/uri.hh"
#include "hive/http/parse.hh"

namespace hive {

static bool isReserved(char ch) {
    switch (ch) {
    case '/': return true;
    case '#': return true;
    case '?': return true;
    default: return false;
    }
}

static ParseResult<sstring> parseScheme(char const* str) {
    auto result = parseWhile(str, [](char ch) {
        return ch != ':' && !isReserved(ch);
    });
    result.ch = (result.ch[0] == ':') ? (result.ch+1) : (result.ch);
    return result;
}

static ParseResult<sstring> parseUser(char const* str) {
    auto result = parseWhile(str, [](char ch) {
        return ch != '@' && !isReserved(ch);
    });
    if (result.ch[0] == '@') {
        result.ch = result.ch+1;
    } else {
        result.ch = str;
        result.value = "";
    }
    return result;
}

static ParseResult<sstring> parseHost(char const* str) {
    return parseWhile(str, [](char ch) {
        return ch != ':' && !isReserved(ch);
    });
}

static ParseResult<uint16_t> parsePort(char const* str) {
    ParseResult<uint16_t> result;
    if (str[0] != ':') {
        result.value = 0;
        result.ch = str;
        return result;
    }
    auto tmp = parseWhile(str+1, [](char ch) {
        return !isReserved(ch);
    });
    result.value = uint16_t(strtol(tmp.value.c_str(), 0, 10));
    result.ch = tmp.ch; 
    return result; 
}

static ParseResult<Authority> parseAuthority(char const* str) {
    ParseResult<Authority> result{};
    if (str[0] == '\0' || str[0] != '/' || str[1] != '/') {
        result.ch = str;
        return result;
    } 
    
    auto user = parseUser(str+2); // For "//"
    auto host = parseHost(user.ch);
    auto port = parsePort(host.ch);

    result.value.userIs(user.value);
    result.value.hostIs(host.value);
    result.value.portIs(port.value);
    result.ch = port.ch;

    return result; 
}

static ParseResult<sstring> parsePath(char const* str) {
    // Return query/frag as part of path for now
    ParseResult<sstring> result = parseWhile(str, [](char ch) {
        return true; 
    }); 
/*
    ParseResult<sstring> result = parseWhile(str, [](char ch) {
        return ch != '/' && !isReserved(ch);
    }); 
    result.ch = (result.ch[0] == '?') ? (result.ch+1) : (result.ch);
*/
    return result;

}

static Uri parseUri(char const* str) {
    Uri uri;

    auto scheme = parseScheme(str);
    auto authority = parseAuthority(scheme.ch);
    auto path = parsePath(authority.ch);

    uri.schemeIs(scheme.value);
    uri.authorityIs(authority.value);
    uri.pathIs(path.value);
    return uri; 
}


Authority::Authority(sstring const& user, sstring const& host, uint16_t port) {
    user_ = user;
    host_ = host;
    port_ = port;
}

Authority::Authority() {
    port_ = 0;
}

void Authority::userIs(sstring const& user) {
    user_ = user;
}

void Authority::hostIs(sstring const& host) {
    host_ = host;
}

void Authority::portIs(uint16_t port) {
    port_ = port;
}

Uri::Uri(char* const value) {
    *this = parseUri(value);
}

Uri::Uri(sstring const& value) {
    *this = parseUri(value.c_str());
}

Uri::Uri() {
}

void Uri::schemeIs(sstring const& scheme) {
    scheme_ = scheme;
}

void Uri::authorityIs(Authority const& authority) {
    authority_ = authority;
}

void Uri::pathIs(sstring const& path) {
    path_ = path;
}

}  //namespace hive
