#include "extent_map_helper.hh"

namespace hive{

future<> extent_map_helper::create(sstring container_name, sstring extent_id, sstring extent_group_id){
    auto insert_params = make_extent_map_params(container_name, extent_id, extent_group_id);
    return _client.insert("", insert_params).then([](){
        return make_ready_future<>();
    });
}

std::unordered_map<std::string, variant> extent_map_helper::make_extent_map_params(sstring container_name, sstring extent_id, sstring extent_group_id){
    std::unordered_map<std::string, variant> insert_params;
    variant keyspace;
    keyspace.name    = "keyspace";
    keyspace.str_val = _keyspace;
    keyspace.type    = "str";

    variant table;
    table.name    = "table_name";
    table.str_val = "extent_map_of_" + container_name;
    table.type    = "str";

    variant v_id;
    v_id.name    = "extent_id";
    v_id.str_val = extent_id;
    v_id.type    = "str";

    variant eg_id;
    eg_id.name    = "extent_group_id";
    eg_id.str_val = extent_group_id;
    eg_id.type    = "str";

    auto timestamp = api::new_timestamp();
    variant mtime;
    mtime.name      = "mtime";
    mtime.int64_val = timestamp;
    mtime.type      = "int64";

    insert_params["keyspace"]   = keyspace;
    insert_params["table_name"] = table;
    insert_params["extent_id"]  = v_id;
    insert_params["extent_group_id"] = eg_id;
    insert_params["mtime"]   = mtime;

    return std::move(insert_params);
}

extent_map_entity extent_map_helper::convert_to_extent_map_entity(std::vector<variant> vt) {
  extent_map_entity entity;
  for (auto& it : vt) {
    if(it.name == "extent_id"){
        entity._extent_id = it.str_val;
    }else if( it.name == "extent_group_id"){
        entity._extent_group_id = it.str_val;
    }else if( it.name == "mtime"){
        entity._mtime = it.int64_val;
    }
  }
  return std::move(entity);
}


future<extent_map_entity> extent_map_helper::find(sstring container_name, sstring extent_id){
    sstring cql = "select * from "+_keyspace+".extent_map_of_"+container_name+ " where extent_id =?";
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(extent_id));
    return _client.query(cql, query_values).then([this] (auto result) {
        auto vt = result.begin();
        auto entity = this->convert_to_extent_map_entity(*vt);
        return make_ready_future<extent_map_entity>(std::move(entity));
    });
}

future<> extent_map_helper::remove(sstring container_name, sstring extent_id){
    sstring cql = "delete from "+_keyspace+".extent_map_of_"+container_name+ " USING TIMESTAMP ? "+" where extent_id=?";
    auto timestamp = api::new_timestamp();
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(extent_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

}//namespace hive
