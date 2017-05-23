
//#include "http/Common.hpp"
#include "seastar/core/sstring.hh"

namespace hive {

template <typename T>
class ParseResult {
public:
    T value;
    char const* ch;
};

template <typename F>
static inline ParseResult<sstring> parseUntil(char const* str, F func) {
    ParseResult<sstring> result{};
    char const* ch = str;
    for (; *ch && !func(*ch); ++ch) {}
    result.value = sstring(str,ch-str);
    result.ch = ch;
    return result;
}

template <typename F>
static inline ParseResult<sstring> parseWhile(char const* str, F func) {
    ParseResult<sstring> result{};
    char const* ch = str;
    for (; *ch && func(*ch); ++ch) {}
    result.value = sstring(str,ch-str);
    result.ch = ch;
    return result;
}

static inline ParseResult<sstring> parseToken(char const* str) {
    auto token = parseUntil(str, isspace);
    token.ch = parseWhile(token.ch, isspace).ch;
    return token;
}

static inline ParseResult<sstring> parseCrLf(char const* str) {
    auto cr = parseUntil(str, [](char ch) { return ch == '\r'; });
	if (*cr.ch == '\r') {
		cr.ch++;
	}
	return parseWhile(cr.ch, [](char ch) { return isspace(ch) && ch != '\r'; });
}

static inline ParseResult<sstring> parseWhitespace(char const* str) {
    return parseWhile(str, isspace);
}


} //namespace hive
