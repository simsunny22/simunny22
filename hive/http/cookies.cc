
//#include "http/Common.hpp"
#include "hive/http/cookies.hh"
#include "hive/http/parse.hh"

namespace hive {

ParseResult<sstring> parseName(char const* str) {
    return parseUntil(str, [](char ch) { return isspace(ch) || ch == '='; });
}

ParseResult<sstring> parseValue(char const* str) {
    return parseUntil(str, [](char ch) { return ch == ';' || ch == '='; });
}

ParseResult<sstring> parseSeparator(char const* str) {
    if (*str) {
        assert(*str==';'||*str=='='); 
        return parseWhitespace(str+1);
    } else {
        auto result = ParseResult<sstring>{};
        result.ch = str;
        return result;
    }
}

Cookie parseCookie(char const* str) {
    auto name = parseName(str);
    auto ch = parseSeparator(name.ch).ch;
    auto value = parseValue(ch);
    ch = parseSeparator(value.ch).ch;

    auto cookie = Cookie();
    cookie.nameIs(name.value);
    cookie.valueIs(value.value);
    while (*ch) {
        auto flag = parseValue(ch);
        if (flag.value == "Path") {
            ch = parseSeparator(flag.ch).ch;
            flag = parseValue(ch);
            cookie.pathIs(flag.value);
        } else if (flag.value == "HttpOnly") {
            cookie.httpOnlyIs(true);
        } else if (flag.value == "Secure") {
            cookie.secureIs(true);
        }
        ch = parseSeparator(flag.ch).ch;
    }
    return cookie;
}

Cookie::Cookie(sstring const& text) {
    *this = parseCookie(text.c_str());
}

Cookie const Cookies::cookie(sstring const& name) const {
    auto i = cookie_.find(name);
    return (i == cookie_.end()) ? Cookie() : i->second;
}

Cookies::Cookies(Cookies&& cookies) noexcept{
    if(this != &cookies) {
        cookie_ = std::move(cookies.cookie_);
    }
}

Cookies& Cookies::operator=(Cookies&& cookies) noexcept{
    if(this != &cookies) {
        cookie_ = std::move(cookies.cookie_);
    }
    return *this;
}

void Cookies::cookieIs(Cookie const& cookie) {
    cookie_[cookie.name()] = cookie;
}

} //namespace hive

