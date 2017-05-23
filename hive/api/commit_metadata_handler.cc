#include "schema_builder.hh"
#include "commit_metadata_handler.hh"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/lexical_cast.hpp>
#include "types.hh" 

#include "service/storage_proxy.hh"
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

namespace hive{

static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);
    
    sstring header_value = ""; 
    if( itor != headers.end() ){
        header_value = itor->second;   
    }
    
    return header_value;
}

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, std::move(value));
};

lw_shared_ptr<request_params> commit_metadata_handler::parse_write_params(request& req){
    header_map& headers = req._headers;

    sstring keyspace        = get_header_value("keyspace",        headers);
    sstring volume_id       = get_header_value("volume_id", headers);
    sstring container_name  = get_header_value("container_name", headers);
    sstring extent_group_id = get_header_value("extent_group_id", headers);
    sstring disk_ids        = get_header_value("disk_ids",         headers);
    sstring extent_id       = get_header_value("extent_id",   headers);
    sstring extent_string   = get_header_value("extent_string",   headers);

    sstring block_id_       = get_header_value("block_id",   headers);
    int64_t block_id        = hive_tools::str_to_int64(block_id_);
    sstring vclock_         = get_header_value("vclock",   headers);
    int64_t vclock          = hive_tools::str_to_int64(vclock_);

    sstring m_time_         = get_header_value("m_time",   headers);
    int64_t m_time          = hive_tools::str_to_int64(m_time_);

    sstring start_at_block_ = get_header_value("start_at_block",   headers);
    int32_t start_at_block  = hive_tools::str_to_int64(start_at_block_);

    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("keyspace",        keyspace));
    params->insert(std::make_pair("container_name",  container_name));
    params->insert(std::make_pair("volume_id",       volume_id));
    params->insert(std::make_pair("extent_group_id", extent_group_id));
    params->insert(std::make_pair("disk_ids",        disk_ids));
    params->insert(std::make_pair("extent_id",       extent_id));
    params->insert(std::make_pair("extent_string",   extent_string));
    params->insert(std::make_pair("block_id",        block_id));
    params->insert(std::make_pair("vclock",          vclock));
    params->insert(std::make_pair("m_time",          m_time));
    params->insert(std::make_pair("start_at_block",  start_at_block));

    return std::move(params);
}

static api::timestamp_type new_timestamp() {
    static api::timestamp_type t = 0;
    return t++;
};

future<> write_group_cf(sstring keyspace,
                        sstring container_name, 
                        sstring extent_group_id, 
                        sstring disk_id, 
                        sstring extent_string, 
                        int64_t mtime) {
    
    database& db = service::get_local_storage_proxy().get_db().local();
    sstring table_name = "extent_group_of_" + container_name;

    auto& cf = db.find_column_family(keyspace, table_name);
    const schema_ptr& s = cf.schema();

    const column_definition& extents_def = *s->get_column_definition("extents");
    const column_definition& m_time_def  = *s->get_column_definition("mtime");

    auto partition_key = partition_key::from_exploded(*s, {to_bytes(extent_group_id)});       //row key
    auto cluster_key   = clustering_key::from_exploded(*s, {to_bytes(disk_id)});              //clustering key

    exploded_clustering_prefix cluster_key_ = exploded_clustering_prefix({to_bytes(disk_id)});

    mutation m(partition_key, s);
    m.partition().apply_insert(*s, cluster_key, new_timestamp());

    assert(extents_def.type->is_multi_cell());
    //mutation m2(key2, s);
    //set_type_impl::mutation set_mut_single{{}, {{ to_bytes("4"), make_atomic_cell({}) }}};
    //m2.set_clustered_cell(c_key, set_col, set_type->serialize_mutation_form(set_mut_single));
    

    list_type_impl::mutation mut;
    mut.cells.emplace_back(to_bytes(extent_string), make_atomic_cell(to_bytes("0")));

    auto&& ltype = static_pointer_cast<const list_type_impl>(extents_def.type);
    auto smut = ltype->serialize_mutation_form(mut);
    m.set_cell(cluster_key_, extents_def, atomic_cell_or_collection::from_collection_mutation(std::move(smut)));
    m.set_clustered_cell(cluster_key_, m_time_def, make_atomic_cell(long_type->decompose(mtime)));

    std::vector<mutation> mutations;
    mutations.emplace_back(m);
    return service::get_local_storage_proxy().mutate(mutations, db::consistency_level::ANY).then([]() mutable {
        return make_ready_future<>();
    });
}

future<> write_extent_cf(sstring keyspace, sstring container_name, sstring extent_id, sstring extent_group_id, int64_t mtime) {
    database& db = service::get_local_storage_proxy().get_db().local();
    sstring table_name = "extent_map_of_" + container_name;
    auto& cf = db.find_column_family(keyspace, table_name);
    const schema_ptr& s = cf.schema();

    auto pk = partition_key::from_exploded(*s, {to_bytes(extent_id)});         //row key
    auto ck = clustering_key::from_exploded(*s, {to_bytes(extent_group_id)});  //clustering key
    mutation m(pk, s);
    
    const column_definition& m_time_def         = *s->get_column_definition("mtime");
    m.set_clustered_cell(ck, m_time_def, make_atomic_cell(long_type->decompose(mtime)));

    m.partition().apply_insert(*s, ck, new_timestamp());
    std::vector<mutation> mutations;
    mutations.emplace_back(m);
 
    return service::get_local_storage_proxy().mutate(mutations, db::consistency_level::ANY).then([]() mutable {
        return make_ready_future<>();
    });
}

future<> write_volume_map_cf(sstring keyspace, sstring container_name, sstring volume_id, int64_t block_id, sstring extent_id, int32_t start_at_block, int64_t mtime) {
//return make_ready_future<>();

    database& db = service::get_local_storage_proxy().get_db().local();
    sstring table_name = "volume_map_of_" + container_name;
    auto& cf = db.find_column_family(keyspace, table_name);
    const schema_ptr& s = cf.schema();

    const column_definition& extent_id_def = *s->get_column_definition("extentid");
    const column_definition& start_at_block_def = *s->get_column_definition("offsetstartinblock");
    const column_definition& m_time_def         = *s->get_column_definition("mtime");

    auto pk = partition_key::from_exploded(*s, {to_bytes(volume_id)});         //row key
    auto ck = clustering_key::from_exploded(*s, {long_type->decompose(block_id)});  //clustering key
    mutation m(pk, s);
    
    m.set_clustered_cell(ck, extent_id_def, make_atomic_cell(to_bytes(extent_id)));
    m.set_clustered_cell(ck, start_at_block_def, make_atomic_cell(int32_type->decompose(start_at_block)));
    m.set_clustered_cell(ck, m_time_def, make_atomic_cell(long_type->decompose(mtime)));

    std::vector<mutation> mutations;
    mutations.emplace_back(m);

    return service::get_local_storage_proxy().mutate(mutations, db::consistency_level::ANY).then([]() mutable {
        return make_ready_future<>();
    });

}

future<> update_volume_vclock(sstring keyspace, sstring volume_id, int64_t vclock) {
    database& db = service::get_local_storage_proxy().get_db().local();
    sstring table_name = "volumes";
    auto& cf = db.find_column_family(keyspace, table_name);
    const schema_ptr& s = cf.schema();
    const column_definition& field_def = *s->get_column_definition(to_bytes("vclock"));

    auto pk = partition_key::from_exploded(*s, {to_bytes(volume_id)});       //row key
    auto ck = clustering_key::from_exploded(*s, {});                         //clustering key
    mutation m(pk, s);
    
    m.set_clustered_cell(ck, field_def, make_atomic_cell(long_type->decompose(vclock)));
    std::vector<mutation> mutations;
    mutations.emplace_back(m);
    //std::cout<< "update_volume_vclock volume_id = "<< volume_id <<"  Vclock = " << vclock<< std::endl;

    return service::get_local_storage_proxy().mutate(mutations, db::consistency_level::ANY).then([]() mutable {
        return make_ready_future<>();
    });

}




future<std::unique_ptr<reply> > commit_metadata_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
//rep->add_header(sstring("connection"), sstring("keep-alive"));
//return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
//std::cout <<"commit_metadata_handler::handle, start, cpu_id: " << engine().cpu_id() << std::endl;

    // params
    lw_shared_ptr<request_params> params = parse_write_params(*req);
    sstring keyspace        = value_cast<sstring>(params->find("keyspace")->second);
    sstring container_name  = value_cast<sstring>(params->find("container_name")->second);
    sstring volume_id       = value_cast<sstring>(params->find("volume_id")->second);
    sstring extent_group_id = value_cast<sstring>(params->find("extent_group_id")->second);
    sstring disk_ids_str    = value_cast<sstring>(params->find("disk_ids")->second);
    sstring extent_string   = value_cast<sstring>(params->find("extent_string")->second);
    sstring extent_id       = value_cast<sstring>(params->find("extent_id")->second);

    int64_t block_id        = value_cast<int64_t>(params->find("block_id")->second);
    int64_t vclock          = value_cast<int64_t>(params->find("vclock")->second);
    int64_t mtime           = value_cast<int64_t>(params->find("m_time")->second);
    int32_t start_at_block  = value_cast<int32_t>(params->find("start_at_block")->second);

    auto disk_ids = hive_tools::split_to_vector(disk_ids_str, ":");

    std::vector<future<>> futures;
    assert(disk_ids.size() > 0);
    for(auto disk_id : disk_ids){
        auto write_fut = write_group_cf(keyspace, container_name, extent_group_id, disk_id, extent_string, mtime);
        futures.push_back(std::move(write_fut));
    }

    return when_all(futures.begin(), futures.end()).then([](auto futs){
       try{
           for(auto& fut : futs){
              fut.get(); 
           }
       }catch(...){
           throw; 
       }
    }).then([keyspace, container_name, extent_id, extent_group_id, mtime]() mutable {
        return write_extent_cf(keyspace, container_name, extent_id, extent_group_id, mtime);
    }).then([keyspace, container_name, volume_id, block_id, extent_id, start_at_block, mtime]() mutable{
        return write_volume_map_cf(keyspace, container_name, volume_id, block_id, extent_id, start_at_block, mtime);
    }).then([keyspace, volume_id, vclock]() mutable{
        return update_volume_vclock(keyspace, volume_id, vclock);
    }).then([rep = std::move(rep)]() mutable{
        rep->add_header(sstring("connection"), sstring("keep-alive"));
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}
}//namespace hive
