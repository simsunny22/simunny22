#include"hive/hive_config.hh"
#include"db/consistency_level_type.hh"
#include"schema_builder.hh"

//schema_ptr generate_global_schema();

uint64_t hive_config::extent_size = 1024*1024;          //1M
uint64_t  hive_config::hive_block_for = 2;
db::consistency_level hive_config::hive_consistency_level = db::consistency_level::TWO;
sstring hive_config::file_seperator = "/";
sstring hive_config::hive_keyspace = "hive";
uint64_t hive_config::extent_group_size = 4*1024*1024;  //4M
uint64_t hive_config::extent_header_size = 512;
uint64_t hive_config::extent_header_digest_size = 16;   
sstring hive_config::primary_journal_name = "primary_journal";
sstring hive_config::secondary_journal_name = "secondary_journal";
//schema_ptr hive_config::global_schema = generate_global_schema();

uint64_t hive_config::wash_monad_queue_periodic = 60*30; //in seconds

//tododl:yellow generate_schema() at here is not suitable
schema_ptr hive_config::generate_schema() {
    sstring version_str = "hive-schema-version";
    utils::UUID schema_version = utils::UUID_gen::get_name_UUID(
             reinterpret_cast<const unsigned char *>(version_str.c_str()), version_str.length() );
    schema_builder builder(make_lw_shared(schema({schema_version}, "hive", "global-schema",
        // partition key
        {{"extent_id", utf8_type}},
        // clustering key
        {{"vclock", long_type }},
        // regular columns
        {   
            {"volume_id", utf8_type},
            {"extent_group_id", utf8_type},
            {"extent_offset_in_group", long_type},
            {"data_offset_in_extent", long_type},
            {"length", long_type},
            {"data", bytes_type},
            {"disk_ids", utf8_type},
            {"intent_id", utf8_type},
            {"options", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "hive journal schema"
    )));
    builder.with_version(schema_version);
    return builder.build(schema_builder::compact_storage::no);
}

//schema_ptr generate_global_schema() {
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
