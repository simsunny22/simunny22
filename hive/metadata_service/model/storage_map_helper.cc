#include "storage_map_helper.hh"

namespace hive{

future<> storage_map_helper::create(sstring disk_id, sstring extent_group_id, sstring container_name){
    int64_t hash = hive_tools::hash(extent_group_id, extent_group_hash_range);
    auto insert_params = make_storage_map_params(disk_id, hash, extent_group_id, container_name);
    return _client.insert("", insert_params).then([](){
        return make_ready_future<>();
    });
}

std::unordered_map<std::string, variant> storage_map_helper::make_storage_map_params(sstring disk_id, 
                                                                                     int64_t hash, 
                                                                                     sstring extent_group_id, 
                                                                                     sstring container_name){
    std::unordered_map<std::string, variant> insert_params;
    variant keyspace;
    keyspace.name    = "keyspace";
    keyspace.str_val = _keyspace;
    keyspace.type    = "str";

    variant table;
    table.name    = "table_name";
    table.str_val = "storage_map";
    table.type    = "str";


    variant d_id;
    d_id.name    = "disk_id";
    d_id.str_val = disk_id;
    d_id.type    = "str";

    variant h_id;
    h_id.name      = "extent_group_hash";
    h_id.int64_val = hash;
    h_id.type      = "int64";

    variant eg_id;
    eg_id.name    = "extent_group_id";
    eg_id.str_val = extent_group_id;
    eg_id.type    = "str";

    variant c_name;
    c_name.name    = "container_name";
    c_name.str_val = container_name;
    c_name.type    = "str";

    insert_params["keyspace"]   = keyspace;
    insert_params["table_name"] = table;
    insert_params["disk_id"]    = d_id;
    insert_params["extent_group_hash"] = h_id;
    insert_params["extent_group_id"] = eg_id;
    insert_params["container_name"]  = c_name;

    return std::move(insert_params);
}

storage_map_entity storage_map_helper::convert_to_storage_map_entity(std::vector<variant> vt) {
  storage_map_entity entity;
  for (auto& it : vt) {
    if(it.name == "disk_id"){
        entity._disk_id = it.str_val;
    }else if( it.name == "extent_group_hash"){
        entity._hash= it.int64_val;
    }else if( it.name == "extent_group_id"){
        entity._extent_group_id = it.str_val;
    }else if( it.name == "container_name"){
        entity._container_name = it.str_val;
    }
  }
  return std::move(entity);
}

future<storage_map_entity> storage_map_helper::find(sstring disk_id, sstring extent_group_id){
    int64_t hash = hive_tools::hash(extent_group_id, extent_group_hash_range);
    sstring cql = "select * from "+_keyspace+".storage_map where disk_id=? and extent_group_hash=? and extent_group_id=?";
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(disk_id));
    query_values.push_back(long_type->decompose((int64_t)hash));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([this] (auto result) {
        if (result.empty()){
            storage_map_entity entity;
            return make_ready_future<storage_map_entity>(std::move(entity));
        }else{
            auto vt = result.begin();
            auto entity = this->convert_to_storage_map_entity(*vt);
            return make_ready_future<storage_map_entity>(std::move(entity));
        }
    });
}

future<> storage_map_helper::remove(sstring disk_id, sstring extent_group_id){
    int64_t hash = hive_tools::hash(extent_group_id, extent_group_hash_range);
    sstring cql = "delete from "+_keyspace+".storage_map where disk_id=? and extent_group_hash=? and extent_group_id=?";
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(utf8_type->decompose(disk_id));
    query_values.push_back(long_type->decompose((int64_t)hash));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}


}//namespace hive
