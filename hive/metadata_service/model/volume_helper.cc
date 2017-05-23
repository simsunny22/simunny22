#include "volume_helper.hh"

namespace hive{


future<> volume_helper::update_volume_vclock(sstring volume_id, int64_t vclock){
    sstring cql = "update "+_keyspace+".volumes USING TIMESTAMP ? set vclock = ? where volume_uuid=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(long_type->decompose(vclock));
    query_values.push_back(utf8_type->decompose(volume_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> volume_helper::update_volume_last_extent_group(sstring volume_id, sstring extent_group_id){
    sstring cql = "update "+_keyspace+".volumes USING TIMESTAMP ? set last_extent_group = ? where volume_uuid=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(volume_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}


}//namespace hive
