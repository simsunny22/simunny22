#include "schema_builder.hh"
#include "db_driver_test_handler.hh"

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

lw_shared_ptr<request_params> db_driver_test_handler::parse_write_params(request& req){
    header_map& headers = req._headers;

    sstring row_key     = get_header_value("row_key", headers);
    sstring cluster_key = get_header_value("cluster_key", headers);
    sstring value       = get_header_value("value", headers);

    lw_shared_ptr<request_params> params = make_lw_shared<request_params>();
    params->insert(std::make_pair("row_key", row_key));
    params->insert(std::make_pair("cluster_key", cluster_key));
    params->insert(std::make_pair("value", value));

    return std::move(params);
}

future<> check(){
    distributed<database>& database_dist = service::get_local_storage_proxy().get_db();
    return database_dist.invoke_on_all([](database& db){
        sstring ks = "test_ks";
        sstring cf_name = "test_cf";
        auto& cf = db.find_column_family(ks, cf_name);       
        const schema_ptr& s = cf.schema();
        std::cout << "dltest ===>>> cpu_id: " << engine().cpu_id() << std::endl;
        std::cout << "dltest ===>>> ks: " << s->ks_name() << std::endl;
        std::cout << "dltest ===>>> cf: " << s->cf_name() << std::endl;
        return make_ready_future<>();
    }).then([](){
        return make_ready_future<>();
    });
}

future<std::unique_ptr<reply> > db_driver_test_handler::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    // params
    std::cout << "====================db_driver_test_handler==============" <<std::endl;
   // lw_shared_ptr<request_params> params = parse_write_params(*req);
   // sstring row_key     = value_cast<sstring>(params->find("row_key")->second);
   // sstring cluster_key = value_cast<sstring>(params->find("cluster_key")->second);
   // sstring value       = value_cast<sstring>(params->find("value")->second);
    check(); 
    sstring uuid_str = utils::UUID_gen::get_time_UUID().to_sstring(); 
    sstring row_key     = "row_key"+uuid_str;
    sstring row_key1    = "row_key1"+uuid_str;
    sstring cluster_key = "cluster_key"+uuid_str;
    sstring cluster_key1 = "cluster_key1"+uuid_str;
    sstring value       = "value-test"+uuid_str;
    database& db = service::get_local_storage_proxy().get_db().local();
    sstring ks = "test_ks";
    sstring cf_name = "test_cf";
    auto& cf = db.find_column_family(ks, cf_name);
    const schema_ptr& s = cf.schema();
    std::cout << "11111111111111111111111" << std::endl;
    const column_definition& value_def = *s->get_column_definition("value");
    std::cout << "22222222222222222222222" << std::endl;

    auto pk = partition_key::from_exploded(*s, {to_bytes(row_key), to_bytes(row_key1)});       //row key
    auto ck = clustering_key::from_exploded(*s, {to_bytes(cluster_key), to_bytes(cluster_key1)});  //clustering key
    std::cout << "33333333333333333333333" << std::endl;
    mutation m(pk, s);

    std::cout << "44444444444444444444444" << std::endl;
    m.set_clustered_cell(ck, value_def, make_atomic_cell(to_bytes(value)));
    std::cout << "55555555555555555555555" << std::endl;
    std::vector<mutation> mutations;
    mutations.emplace_back(m);
    std::cout << "66666666666666666666666" << std::endl;
    //service::get_local_storage_proxy().mutate(mutations, db::consistency_level::ANY); 
    std::cout << "77777777777777777777777" << std::endl;
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}




























//sstring db_driver_test_handler::build_return_json(int64_t vclock){
//    sstring str_vclock = to_sstring(vclock);
//    sstring build_str = "{\"vclock\":" + str_vclock + "}";   
//    //std::string err;
//    //auto json_tmp = hive::Json::parse(build_str, err);
//    //auto json_str = json_tmp.dump();
//    //return json_str;
//    return build_str;
//}
//
//make_mutate(){
//    schema_ptr s =generate_schema();
//    const colume_definition& 
//}
//schema_ptr generate_schema() {
//    sstring version_str = "hive-schema-version";
//    utils::UUID schema_version = utils::UUID_gen::get_name_UUID(
//             reinterpret_cast<const unsigned char *>(version_str.c_str()), version_str.length() );
//    schema_builder builder(make_lw_shared(schema({schema_version}, "hive", "global-schema",
//        // partition key
//        {{"extent_id", utf8_type}},
//        // clustering key
//        {{"vclock", long_type }}, 
//        // regular columns
//        {   
//            {"volume_id", utf8_type},
//            {"extent_group_id", utf8_type},
//            {"extent_offset_in_group", long_type},
//            {"data_offset_in_extent", long_type},
//            {"length", long_type},
//            {"data", bytes_type},
//            {"disk_ids", utf8_type},
//        },  
//        // static columns
//        {}, 
//        // regular column name type
//        utf8_type,
//        // comment
//        "hive journal schema"
//    )));
//    builder.with_version(schema_version);
//    return builder.build(schema_builder::compact_storage::no);
//}
//




}//namespace hive
