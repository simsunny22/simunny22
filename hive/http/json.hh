#pragma once

#include "seastar/core/sstring.hh"
#include <json/json.h>  //jsoncpp

namespace hive {

template<typename Map>
inline sstring map_to_json(const Map& map) {
    Json::Value root(Json::objectValue);
    for (auto&& kv : map) {
        root[kv.first] = Json::Value(kv.second);
    }
    Json::FastWriter writer;
    // Json::FastWriter unnecessarily adds a newline at the end of string.
    // There is a method omitEndingLineFeed() which prevents that, but it seems
    // to be too recent addition, so, at least for now, a workaround is needed.
    auto str = writer.write(root);
    if (str.length() && str.back() == '\n') {
        str.pop_back();
    }
    return str;
}

inline std::map<sstring, sstring> json_to_map(const sstring& raw) {
    Json::Value root;
    Json::Reader reader;
    reader.parse(std::string{raw}, root);
    std::map<sstring, sstring> map;
    for (auto&& member : root.getMemberNames()) {
        map.emplace(member, root[member].asString());
    }
    return map;
}

} //namespace hive
