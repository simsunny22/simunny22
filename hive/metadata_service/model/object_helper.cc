#include "object_helper.hh"

namespace hive{


future<> object_helper::update_object_size(sstring object_id, uint64_t new_size){
    sstring cql = "update "+_keyspace+".objects USING TIMESTAMP ? set size= ? where object_uuid=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(long_type->decompose((int64_t)new_size));
    query_values.push_back(utf8_type->decompose(object_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> object_helper::update_object_last_extent_group(sstring object_id, sstring extent_group_id){
    sstring cql = "update "+_keyspace+".objects USING TIMESTAMP ? set last_extent_group = ? where object_uuid=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(object_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}


}//namespace hive
