#pragma once
#include "schema_builder.hh"
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

#include "cql3/untyped_result_set.hh"

#include "cql3/query_processor.hh"
#include "exceptions/exceptions.hh"

#include "hive/metadata_service/entity/storage_map_entity.hh"
#include "hive/metadata_service/db_client/scylla_client.hh"

namespace hive {

static const uint64_t extent_group_hash_range = 100;

class storage_map_helper {
private:
    scylla_client _client;
    sstring       _keyspace;
public:
    storage_map_helper(){
        _keyspace = "guru_metadata";
    }
    storage_map_helper(scylla_client client){
        _client = client;
        _keyspace = "guru_metadata";
    }

    std::unordered_map<std::string, variant> make_storage_map_params(sstring disk_id, int64_t hash, sstring extent_group_id, sstring container_name);
    future<> create(sstring disk_id, sstring extent_group_id, sstring container_name);

    future<storage_map_entity> find(sstring disk_id, sstring extent_group_id);
    future<> remove(sstring disk_id, sstring extent_group_id);

private:
    storage_map_entity convert_to_storage_map_entity(std::vector<variant> vt);
};

} //namespace hive
