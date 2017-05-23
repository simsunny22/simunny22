#include "hive/http/response.hh"
#include "hive/http/error.hh"
#include "hive/http/parse.hh"


namespace hive {

static ParseResult<Response::Status> parseStatus(char const* str) {
    ParseResult<Response::Status> result{};
    auto code = parseToken(str);

    result.value = (Response::Status)std::atoi(code.value.c_str());
    result.ch = code.ch;
    return result;
}

Response parseResponse(char const* str) {
    // Parse an HTTP response 
    auto version = parseToken(str);
    auto code = parseStatus(version.ch);
    auto message = parseUntil(code.ch, [](char ch) { return ch == '\r'; });
    
    auto response = Response();
    if (version.value != "HTTP/1.1") {
        throw Error("bad HTTP version");
    }
      
    auto ch = parseCrLf(message.ch).ch;
    while (*ch != '\0' && *ch != '\r') {
        auto name = parseUntil(ch, [](char ch) { return ch == ':'; });
        if (*name.ch) {
            name.ch++; // For ":"
        }
        auto ws = parseWhile(name.ch, isspace);
        auto value = parseUntil(ws.ch, [](char ch) { return ch == '\r'; });   
        response.headerIs(name.value, value.value);
        if (name.value == "Set-Cookie") {
            response.cookieIs(Cookie(value.value));
        }
        ch = parseCrLf(value.ch).ch;
    }
    ch = parseCrLf(ch).ch;
    
    response.statusIs(code.value);
    response.dataIs(ch); 
    return response;
}

Response::Response(sstring const& response) {
     *this = parseResponse(response.c_str());
}

Response::Response(Response&& response) noexcept{
    if(this != &response) {
        status_  = response.status_;
        data_    = std::move(response.data_);
        headers_ = std::move(response.headers_);
        cookies_ = std::move(response.cookies_);
    }
}
Response& Response::operator=(Response&& response) noexcept{
    if(this != &response) {
        status_  = response.status_;
        data_    = std::move(response.data_);
        headers_ = std::move(response.headers_);
        cookies_ = std::move(response.cookies_);
    }
    return *this;
}

sstring const Response::header(sstring const& name) const {
    return headers_.header(name);
}

Cookie const Response::cookie(sstring const& name) const {
    return cookies_.cookie(name);
}

void Response::statusIs(Status status) {
    status_ = status;
}

void Response::dataIs(sstring const& data) {
    data_ = data;
}

void Response::headerIs(sstring const& name, sstring const& value) {
    headers_.headerIs(name, value);
}

void Response::cookieIs(Cookie const& cookie) {
    cookies_.cookieIs(cookie);
}

} //namespace hive
