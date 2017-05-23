#include "extent_group_map_helper.hh"
#include "hive/memcache/memcached_service.hh"
//#include "hive/metadata_service/metadata_service.hh"

namespace hive{

static logging::logger logger("extent_group_map_helper");

future<> extent_group_map_helper::create(sstring container_name, sstring owner_id, sstring extent_group_id, sstring disk_id){
    logger.debug("[{}] start, extent_group_id:{} disk_id:{}", __func__,  extent_group_id, disk_id);
    auto insert_params = make_extent_group_map_params(container_name, owner_id, extent_group_id, disk_id);
    return _client.insert("", insert_params).then([](){
        return make_ready_future<>();
    });
}

std::unordered_map<std::string, variant> extent_group_map_helper::make_extent_group_map_params(sstring container_name, sstring owner_id, sstring extent_group_id, sstring disk_id){
    std::unordered_map<std::string, variant> insert_params;
    variant keyspace;
    keyspace.name    = "keyspace";
    keyspace.str_val = _keyspace;
    keyspace.type    = "str";


    variant table;
    table.name    = "table_name";
    table.str_val = "extent_group_of_" + container_name;
    table.type    = "str";

    variant eg_id;
    eg_id.name    = "extent_group_id";
    eg_id.str_val = extent_group_id;
    eg_id.type    = "str";

    variant owner_id_;
    owner_id_.name    = "owner_id";
    owner_id_.str_val = owner_id;
    owner_id_.type    = "str";



    variant d_id;
    d_id.name    = "disk_id";
    d_id.str_val = disk_id;
    d_id.type    = "str";

    variant created;
    created.name     = "created";
    created.bool_val = false;
    created.type     = "bool";

    variant version;
    version.name      = "version";
    version.int64_val = 0;
    version.type      = "int64";




    auto timestamp = api::new_timestamp();
    variant mtime;
    mtime.name      = "mtime";
    mtime.int64_val = timestamp;
    mtime.type      = "int64";

    insert_params["keyspace"]   = keyspace;
    insert_params["table_name"] = table;
    insert_params["extent_group_id"] = eg_id;
    insert_params["owner_id"] = owner_id_;
    insert_params["disk_id"]  = d_id;
    insert_params["created"]  = created;
    insert_params["version"]  = version;
    insert_params["mtime"]    = mtime;

    return std::move(insert_params);
}

extent_group_entity extent_group_map_helper::convert_to_extent_group_entity(std::vector<variant> vt) {
    extent_group_entity entity;
    for (auto& it : vt) {
      if(it.name == "extent_group_id"){
          entity._extent_group_id = it.str_val;
      }else if( it.name == "disk_id"){
          entity._disk_id = it.str_val;
      }else if( it.name == "version"){
          entity._version = it.int64_val;
      }else if( it.name == "mtime"){
          entity._mtime = it.int64_val;
      }else if( it.name == "created"){
          entity._created = it.bool_val;
      }else if( it.name == "owner_id"){
          entity._owner_id = it.str_val;
      }else if( it.name == "extents"){
         std::string err;                                                                                                   
         auto extents_json = hive::Json::parse(it.str_val, err);
         std::vector<extent_entity> extents;
         for(auto& extent: extents_json["extents"].array_items()) {
             extent_entity entity1;
             entity1._extent_id = extent["extent_id"].string_value();
             entity1._offset    = extent["offset"].uint64_value();
             entity1._ref_count = extent["ref_count"].uint64_value();
             entity1._check_sum = extent["check_sum"].string_value();
             extents.push_back(entity1);
         }
         entity._extents = std::move(extents);
      }
    }
    return std::move(entity);
}

future<> extent_group_map_helper::test() {
    std::vector<extent_entity> extents;
    extent_entity entity;
    entity._extent_id  = "extent1";
    entity._offset    = 1048576;
    entity._ref_count = 1;
    entity._check_sum = "";
    extents.push_back(entity);

    std::stringstream buffer;                                                                              
    msgpack::pack(buffer, extents);                                                                    
    buffer.seekg(0);                                                                                       

    msgpack::object_handle oh1 = msgpack::unpack(buffer.str().data(), buffer.str().size());
    msgpack::object const& obj1 = oh1.get();
    std::vector<extent_entity> new_extents;
    obj1.convert(new_extents);
    for(auto& extent : new_extents) {
        std::cout << "offset="<< extent._offset<<std::endl;
        std::cout << "ref_count="<< extent._ref_count<<std::endl;
        std::cout << "check_sum="<< extent._check_sum<<std::endl;
    }
    return make_ready_future<>();
}

future<bool> extent_group_map_helper::test1() {
    std::vector<extent_entity> extents;
    for(int i=0; i<3; i++){
        extent_entity e_entity;
        e_entity._extent_id  = "extent#" + to_sstring(i);
        e_entity._offset    = 1048576;
        e_entity._ref_count = 1;
        e_entity._check_sum = "check_sum";
        extents.push_back(e_entity);
    }

    extent_group_entity entity;
    entity._extent_group_id = "group01";
    entity._disk_id = "disk01";
    entity._extents = extents;
    entity._owner_id = "vol#1";
    entity._created = false;
    entity._version = 123;
    entity._mtime = 456;

    std::stringstream buffer; 
    msgpack::pack(buffer, entity);
    buffer.seekg(0);                                                                                       

    auto& memcached = memcache::get_local_memcached_service();
    memcache::item_insertion_data insertion;
    memcache::item_key key(entity._extent_group_id);
    insertion.key = std::move(key);

    //insertion.data = buffer.str().data();

    //set expire
    //memcache::expiration expiration(memcached.get_wc_to_clock_type_delta(), metadata_cache_ttl);
    //insertion.expiry = expiration;

    //set value
logger.debug("[{}] extent_group_id:{} value_size:{}", __func__, entity._extent_group_id, insertion.data.size());
    return memcached.set(std::move(insertion));
}

future<> extent_group_map_helper::test2() {
    auto& memcached = memcache::get_local_memcached_service();

    sstring extent_group_id = "group01";
    return memcached.get(extent_group_id).then([extent_group_id](auto entity_ptr){
        if (!entity_ptr){
logger.debug("[{}] extent_group_id:{} value_size:{}", __func__, extent_group_id, entity_ptr->value().size());
            msgpack::object_handle oh1 = msgpack::unpack(entity_ptr->value().get(), entity_ptr->value().size());
            msgpack::object const& obj1 = oh1.get();
            extent_group_entity group_entity;
            obj1.convert(group_entity);

            std::cout << "extent_group_id="<< group_entity._extent_group_id<<std::endl;
            std::cout << "disk_id="<< group_entity._disk_id<<std::endl;
            std::cout << "owner_id="<< group_entity._owner_id<<std::endl;
            std::cout << "created="<< group_entity._created<<std::endl;
            std::cout << "version="<< group_entity._version<<std::endl;
            std::cout << "mtime="<< group_entity._mtime<<std::endl;
            for(auto& extent : group_entity._extents) {
              std::cout << "extent_id="<< extent._extent_id<<std::endl;
              std::cout << "offset="<< extent._offset<<std::endl;
              std::cout << "ref_count="<< extent._ref_count<<std::endl;
              std::cout << "check_sum="<< extent._check_sum<<std::endl;
            }

            return make_ready_future<>();

        }else{
logger.debug("[{}] get from cache failed extent_group_id:{} ", __func__, extent_group_id);
            return make_ready_future<>();
        }
    });
}

future<extent_group_entity> extent_group_map_helper::find(sstring container_name, sstring extent_group_id, sstring disk_id){
    sstring cql = "select * from "+_keyspace+".extent_group_of_"+container_name+ " where extent_group_id=? and disk_id=?";
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(disk_id));
    return _client.query(cql, query_values).then([this] (auto result) {
        if(result.empty()){
            extent_group_entity entity;
            return make_ready_future<extent_group_entity>(std::move(entity));
        }

        auto vt = result.begin();
        auto&& entity = this->convert_to_extent_group_entity(*vt);
        return make_ready_future<extent_group_entity>(std::move(entity));
    });
}

future<extent_group_entity> extent_group_map_helper::find(sstring container_name, sstring extent_group_id){
    sstring cql = "select * from "+_keyspace+".extent_group_of_"+container_name+ " where extent_group_id=?";
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([this] (auto result) {
        if(result.empty()){
            extent_group_entity entity;
            return make_ready_future<extent_group_entity>(std::move(entity));
        }

        auto vt = result.begin();
        auto&& entity = this->convert_to_extent_group_entity(*vt);
        return make_ready_future<extent_group_entity>(std::move(entity));
    });
}

future<> extent_group_map_helper::remove(sstring container_name, sstring extent_group_id){
    sstring cql = "delete from "+_keyspace+".extent_group_of_"+container_name+ " USING TIMESTAMP ? "+" where extent_group_id=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> extent_group_map_helper::remove(sstring container_name, sstring extent_group_id, sstring disk_id){
    sstring cql = "delete from "+_keyspace+".extent_group_of_"+container_name+ " USING TIMESTAMP ? "+" where extent_group_id=? and disk_id=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(disk_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> extent_group_map_helper::update_version(sstring container_name, sstring extent_group_id, sstring disk_id, uint64_t version){
    //sstring cql = "update "+_keyspace+".extent_group_of_"+container_name+ "  USING TIMESTAMP ? set version = ? where extent_group_id=? and disk_id=?";
    sstring cql = "update "+_keyspace+".extent_group_of_"+container_name+ "  USING TIMESTAMP ? set version = ? where extent_group_id=?";
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;                                                                            
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(long_type->decompose((int64_t)version));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    //query_values.push_back(utf8_type->decompose(disk_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> extent_group_map_helper::update_created(sstring container_name, sstring extent_group_id, sstring disk_id, bool created){
    sstring cql = "update "+_keyspace+".extent_group_of_"+container_name+ "  USING TIMESTAMP ? set created=? where extent_group_id=? and disk_id=?";
    //sstring cql = "update "+_keyspace+".extent_group_of_"+container_name+ "  USING TIMESTAMP ? set created=? where extent_group_id=?";
    //std::cout<<"update created cql:"<<cql<<std::endl;
    auto timestamp = _client.get_timestamp();
    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    query_values.push_back(boolean_type->decompose(created));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(disk_id));
    return _client.query(cql, query_values).then([] (auto result) {
        return make_ready_future<>();
    });
}

future<> extent_group_map_helper::update_extents(sstring container_name, sstring extent_group_id, sstring disk_id, std::vector<extent_entity> extents){
    sstring build_str = "{\"extents\":[";
    for(auto& extent : extents) {
        build_str += "{\"extent_id\":\"" + extent._extent_id + "\",";
        build_str += "\"offset\":" + to_sstring(extent._offset) + ",";
        build_str += "\"ref_count\":" + to_sstring(extent._ref_count) +",";
        build_str += "\"check_sum\":\"" + extent._check_sum+"\"},";
    }

    sstring extents_string = build_str.substr(0, build_str.length()-1);
    extents_string += "]}";

    sstring cql = "update "+_keyspace+".extent_group_of_"+container_name+ "  USING TIMESTAMP ? set extents=? where extent_group_id=? and disk_id=?";
    auto timestamp = _client.get_timestamp();

logger.debug("[{}] start, extent_group_id:{} disk_id:{} extents size:{} extents_string:{} timestamp:{}", __func__,  extent_group_id, disk_id, extents.size(), extents_string, timestamp);

    std::vector<bytes_opt> query_values;
    query_values.push_back(long_type->decompose((int64_t)timestamp));
    //query_values.push_back(utf8_type->decompose(buffer.str().data()));
    query_values.push_back(utf8_type->decompose(extents_string));
    query_values.push_back(utf8_type->decompose(extent_group_id));
    query_values.push_back(utf8_type->decompose(disk_id));
    return _client.query(cql, query_values).then([] (auto result) {
        logger.debug("[{}] add extent success ", __func__);
        return make_ready_future<>();
    });
}

future<std::vector<sstring>> extent_group_map_helper::get_extent_group_disks(sstring container_name, sstring extent_group_id){
    sstring cql = "select disk_id from "+_keyspace+".extent_group_of_"+container_name+ " where extent_group_id=?";
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([this, extent_group_id] (auto result) {
        std::vector<sstring> disks;
        for (auto& disk :result){
            for(auto& fv : disk){
                if( fv.name == "disk_id"){
                    disks.push_back(fv.str_val);
                }
            }
        }

        return make_ready_future<std::vector<sstring>>(std::move(disks));
    });

}

future<bool> extent_group_map_helper::get_created(sstring container_name, sstring extent_group_id){
    sstring cql = "select created from "+_keyspace+".extent_group_of_"+container_name+ " where extent_group_id=?";
    //std::cout<<"select created cql:"<<cql<<std::endl;
    //std::cout<<"select created container_name:"<<container_name<<",extent_group_id:"<<extent_group_id<<std::endl;
    std::vector<bytes_opt> query_values;
    query_values.push_back(utf8_type->decompose(extent_group_id));
    return _client.query(cql, query_values).then([this, extent_group_id] (auto results) {
        bool create = false;
        for (auto& result :results){
            for(auto& fv : result){
                if( fv.name == "created"){
                    if (fv.bool_val) {
                       create = true;
                    }
                }
            }
        }

std::cout<<"get created:"<<create<<std::endl;
        return make_ready_future<bool>(create);
    });

}
}//namespace hive
