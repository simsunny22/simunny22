#pragma once
#include <seastar/core/sstring.hh>
#include <db/consistency_level.hh>
#include <hashing.hh>
#include "schema.hh"

class hive_config{
public:
    static uint64_t extent_size;
    static uint64_t hive_block_for;
    static db::consistency_level hive_consistency_level;
    static sstring file_seperator;
    static sstring hive_keyspace;
    static uint64_t extent_group_size;
    static uint64_t extent_header_size;
    static uint64_t extent_header_digest_size;
    static sstring primary_journal_name;
    static sstring secondary_journal_name;
    //static schema_ptr global_schema;

    static schema_ptr generate_schema();
    static uint64_t wash_monad_queue_periodic; //in seconds
};
