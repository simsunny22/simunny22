#include "volume_map_helper.hh"

namespace hive{

future<> volume_map_helper::create(sstring container_name, sstring volume_id, uint64_t block_id, sstring extent_group_id){
    auto insert_params = make_volume_map_params(container_name, volume_id, block_id, extent_group_id);
    return _client.insert("", insert_params).then([](){
        return make_ready_future<>();
    });
}

std::unordered_map<std::string, variant> volume_map_helper::make_volume_map_params(sstring container_name, sstring volume_id, uint64_t block_id, sstring extent_group_id){
    std::unordered_map<std::string, variant> insert_params;
    variant keyspace;
    keyspace.name    = "keyspace";
    keyspace.str_val = _keyspace;
    keyspace.type    = "str";

    variant table;
    table.name    = "table_name";
    table.str_val = "volume_map_of_" + container_name;
    table.type    = "str";


    variant v_id;
    v_id.name    = "volume_uuid";
    v_id.str_val = volume_id;
    v_id.type    = "str";

    variant b_id;
    b_id.name      = "b_id";
    b_id.int64_val = block_id;
    b_id.type      = "int64";

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
    insert_params["volume_uuid"]  = v_id;
    insert_params["block_id"]   = b_id;
    insert_params["extent_group_id"] = eg_id;
    insert_params["mtime"]   = mtime;

    return std::move(insert_params);
}

volume_map_entity volume_map_helper::convert_to_volume_map_entity(std::vector<variant> vt) {
  volume_map_entity entity;
  for (auto& it : vt) {
    if(it.name == "volume_uuid"){
        entity._volume_id = it.str_val;
    }else if( it.name == "block_id"){
        entity._block_id = it.int64_val;
    }else if( it.name == "extent_group_id"){
        entity._extent_group_id = it.str_val;
    }else if( it.name == "mtime"){
        entity._mtime = it.int64_val;
    }
  }
  return std::move(entity);
}

future<volume_map_entity> volume_map_helper::find(sstring container_name, sstring volume_id, uint64_t block_id){
    sstring cql = "select * from "+_keyspace+".volume_map_of_"+container_name+ " where volume_uuid=? and block_id = ?";
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(utf8_type->decompose(volume_id));
    query_values.push_back(long_type->decompose((int64_t)block_id));
    return _client.query(cql, query_values).then([this] (auto result) {
        if (result.empty()){
            volume_map_entity entity;
            entity._extent_group_id = "undefined";
            return make_ready_future<volume_map_entity>(std::move(entity));
        }else{
            auto vt = result.begin();
            auto entity = this->convert_to_volume_map_entity(*vt);
            return make_ready_future<volume_map_entity>(std::move(entity));
        }
    });
}

future<std::vector<volume_map_entity>> volume_map_helper::find_by_volume_id(sstring container_name, sstring volume_id){
    sstring cql = "select * from "+_keyspace+".volume_map_of_"+container_name+ " where volume_uuid=?";
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(utf8_type->decompose(volume_id));
    return _client.query(cql, query_values).then([this] (auto results) {
        if (results.empty()){
	    std::vector<volume_map_entity> entities;
            return make_ready_future<std::vector<volume_map_entity>>(std::move(entities));
        }else{
	    std::vector<volume_map_entity> entities;
            for(auto& result : results){
                auto entity = this->convert_to_volume_map_entity(result);
                entities.push_back(entity);
            } 
            return make_ready_future<std::vector<volume_map_entity>>(std::move(entities));
        }
    });
}

future<volume_map_entity> volume_map_helper::update(sstring container_name, sstring volume_id, uint64_t block_id, sstring new_extent_group_id){
    volume_map_entity entity;
    return make_ready_future<volume_map_entity>(std::move(entity));
}

future<> volume_map_helper::remove(sstring container_name, sstring volume_id, uint64_t block_id){
    sstring cql = "delete from "+_keyspace+".volume_map_of_"+container_name+ " USING TIMESTAMP ? "+" where volume_uuid=? and block_id = ?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(volume_id));
    query_values.push_back(long_type->decompose((int64_t)block_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> volume_map_helper::remove(sstring container_name, sstring volume_id){
    sstring cql = "delete from "+_keyspace+".volume_map_of_"+container_name+ " where volume_uuid=?";
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(utf8_type->decompose(volume_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}


}//namespace hive
