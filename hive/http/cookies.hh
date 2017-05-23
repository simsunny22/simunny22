#ifndef __COOKIES_HH__
#define __COOKIES_HH__

#include <map>
#include "seastar/core/sstring.hh"

namespace hive {

class Cookie {
public:
    Cookie(sstring const& text);
    Cookie() : httpOnly_(false), secure_(false) {};

    sstring const& name() const { return name_; }
    sstring const& value() const { return value_; }
    sstring const& path() const { return path_; }
    bool httpOnly() const { return httpOnly_; }
    bool secure() const { return secure_; }

    void nameIs(sstring const& name) { name_ = name; }
    void valueIs(sstring const& value) { value_ = value; }
    void pathIs(sstring const& path) { path_ = path; }
    void httpOnlyIs(bool httpOnly) { httpOnly_ = httpOnly; }
    void secureIs(bool secure) { secure_ = secure; }

private:
    sstring name_;
    sstring value_;
    sstring path_;
    bool httpOnly_;
    bool secure_;
};

class Cookies {
public:
    typedef std::map<sstring,Cookie> Map;
    
    Cookies() {}
    ~Cookies() {}
    Cookies(Cookies&& cookies) noexcept;
    Cookies& operator=(Cookies&& cookies) noexcept;
    Cookie const cookie(sstring const& name) const;
    Map::const_iterator begin() const { return cookie_.begin(); }
    Map::const_iterator end() const { return cookie_.end(); }

    void cookieIs(Cookie const& cookie);

    static sstring const HOST;
    static sstring const CONTENT_LENGTH;
    static sstring const ACCEPT_ENCODING;
    static sstring const CONNECTION;
private:
    Map cookie_;
};

}  //namespace hive
#endif //__COOKIES_HH__
