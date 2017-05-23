#include "scylla_client.hh"

template <>
inline
shared_ptr<const abstract_type> data_type_for<double>() {
    return double_type;
}

namespace hive{
static logging::logger logger("scylla_client");

variant scylla_client::make_variant(const cql3::untyped_result_set::row& row_, sstring type, sstring name) {
    variant v = {"", "", "", 0, 0, false};
    if(type=="org.apache.cassandra.db.marshal.UTF8Type"){
        sstring col_val = row_.get_as<sstring>(name);
        v.name = name;
        v.type = "str";
        v.str_val = col_val;
    }else if(type=="org.apache.cassandra.db.marshal.LongType"){
        int64_t col_val = row_.get_as<int64_t>(name);
        v.name = name;
        v.type = "int64";
        v.int64_val = col_val;
    }else if(type=="org.apache.cassandra.db.marshal.BytesType"){
        sstring col_val = row_.get_as<sstring>(name);
        v.name = name;
        v.type = "blob";
        v.str_val = col_val;
    }else if(type=="org.apache.cassandra.db.marshal.DoubleType"){
        double col_val = row_.get_as<double>(name);
        v.name = name;
        v.type = "double";
        v.double_val = col_val;
    }else if(type=="org.apache.cassandra.db.marshal.BooleanType"){
        bool col_val = row_.get_as<bool>(name);
        v.name = name;
        v.type = "bool";
        v.bool_val = col_val;
    }else{
    }
    return std::move(v);
}

future<std::vector<std::vector<variant>>> scylla_client::query(sstring cqlstring, std::vector<bytes_opt> query_values) {
   if (_scylla_option._is_local) {
       return query_internal(cqlstring, query_values);
   } else {
       return query_by_http(cqlstring, query_values);
   }
}

future<std::vector<std::vector<variant>>> scylla_client::query_by_http(sstring cqlstring, std::vector<bytes_opt> query_values) {
    // use http api query
    std::vector<std::vector<variant>> result_rows;
    return make_ready_future<std::vector<std::vector<variant>>>(std::move(result_rows));
}

future<std::vector<std::vector<variant>>> scylla_client::query_internal(sstring cqlstring, std::vector<bytes_opt> query_values) {
    db::consistency_level consistency_level = wire_to_consistency(get_consistency_level());
    auto opts = cql3::query_options(
        consistency_level,
        stdx::nullopt,
        query_values,
        false,
        cql3::query_options::specific_options::DEFAULT, 
        cql_serialization_format::latest()
    );

    service::query_state qs(service::client_state::for_external_calls());
    return do_with(std::move(cqlstring), std::move(qs), std::move(opts),
            [this] (auto& cqlstring, auto& qs, auto& opts) mutable {
        auto& qp = cql3::get_local_query_processor();
        std::experimental::basic_string_view<char> query_string_view(cqlstring); 
        return qp.process(query_string_view, qs, opts).then_wrapped([this, cqlstring](auto fut) mutable {
            try {
                cql3::untyped_result_set result_set(fut.get0());
    
                std::vector<std::vector<variant>> result_rows;
                for (auto it=result_set.begin(); it!=result_set.end(); it++){
                    std::vector<variant> result_row;
                    for (auto col_spec : it->get_columns()) {
                        sstring col_name = col_spec->name->to_string();
                        sstring col_type_name = col_spec->type->name();

												if (it->has(col_name)){
                            variant v = this->make_variant(*it, col_type_name, col_name);
                            result_row.push_back(v);
                        }
                    }
                    result_rows.push_back(result_row);
                }
              return make_ready_future<std::vector<std::vector<variant>>>(std::move(result_rows));
            } catch (const std::exception& e) {
                sstring error_info = "db error query string:" + cqlstring;
                logger.error("[{}] db error cqlstring:{}", __func__, cqlstring);
                throw hive::db_error_exception(error_info);
            }
        });
    });
}

static atomic_cell make_atomic_cell_(api::timestamp_type timestamp, bytes value) {
    return atomic_cell::make_live(timestamp, std::move(value));
};

bytes scylla_client::get_value_by_type(variant& v, const column_definition& def) {
    if(v.type=="str"){
        return std::move(def.type->decompose(sstring(v.str_val)));
    }else if(v.type=="int64") {
        return std::move(def.type->decompose(v.int64_val));
    }else if(v.type=="blob") {
        return std::move(def.type->decompose(data_value(to_bytes(v.str_val))));
    }else if(v.type=="double"){
        return std::move(def.type->decompose(v.double_val));
    }else if(v.type=="bool"){
        return std::move(def.type->decompose(v.bool_val));
    }else{
        //TODO: error handling 
        return bytes();
    }
}

future<> scylla_client::insert(sstring cqlstring, std::unordered_map<std::string, variant> insert_params) {
   if (_scylla_option._is_local) {
       return insert_internal(cqlstring, insert_params);
   } else {
       return insert_by_http(cqlstring, insert_params);
   }
}

future<> scylla_client::insert_by_http(sstring cqlstring, std::unordered_map<std::string, variant> insert_params) {
  return make_ready_future<>();
}

future<> scylla_client::insert_internal(sstring cqlstring, std::unordered_map<std::string, variant> insert_params) {
    sstring keyspace              = insert_params["keyspace"].str_val;
    sstring table_name            = insert_params["table_name"].str_val;
    //int64_t revision              = insert_params["revision"].int64_val;
    int64_t consistency_level_num = get_consistency_level();
    db::consistency_level consistency_level = wire_to_consistency(consistency_level_num);
    database& db = service::get_local_storage_proxy().get_db().local();
    auto& cf = db.find_column_family(keyspace, table_name);
    const schema_ptr& s = cf.schema();
    
    std::vector<bytes> pks;
    for (auto &def : s->partition_key_columns()) {
        pks.push_back(get_value_by_type(insert_params[def.name_as_text()], def));
    }
    std::vector<bytes> cks;
    for (auto &def : s->clustering_key_columns()) {
        cks.push_back(get_value_by_type(insert_params[def.name_as_text()], def));
    }
    
    auto partition_key = partition_key::from_exploded(*s, pks);       //row key
    auto cluster_key   = clustering_key::from_exploded(*s, cks);      //clustering key
    exploded_clustering_prefix cluster_key_ = exploded_clustering_prefix(std::move(cks));

    mutation m(partition_key, s);
    api::timestamp_type timestamp = get_timestamp();
    
    
    for (auto &def : s->regular_columns()) {
        auto it = insert_params.find(def.name_as_text());
        if ( it == insert_params.end() )
            continue;

        m.set_clustered_cell(cluster_key_, def, make_atomic_cell_(timestamp, get_value_by_type(it->second, def)));
    }

    for(auto &static_def : s->static_columns()){
        auto it = insert_params.find(static_def.name_as_text());
        if ( it == insert_params.end() )
            continue;

        m.set_static_cell(static_def, make_atomic_cell_(timestamp, get_value_by_type(it->second, static_def)));
    }

    std::vector<mutation> mutations;
    mutations.emplace_back(m);
    
    return service::get_local_storage_proxy().mutate(mutations, consistency_level, nullptr).then_wrapped([this](auto fut){
        try {
            return make_ready_future<>();
        } catch (...) {
            sstring error_info = "db error insert_internal .";
            logger.error("[{}] db error", __func__);
            throw hive::db_error_exception(error_info);
        }
    });

}

api::timestamp_type scylla_client::get_timestamp() {
    auto current = api::new_timestamp();
    auto last = _last_timestamp_micros;
    auto result = last >= current ? last + 1 : current;
    _last_timestamp_micros = result;
    return result;
}
}//namespace hive
