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

#include "hive/metadata_service/entity/volume_map_entity.hh"
#include "hive/metadata_service/db_client/scylla_client.hh"

namespace hive {

class volume_map_helper {
private:
    scylla_client _client;
    sstring       _keyspace;
public:
    volume_map_helper(){
        _keyspace = "guru_metadata";
    }
    volume_map_helper(scylla_client client){
        _client = client;
        _keyspace = "guru_metadata";
    }

    std::unordered_map<std::string, variant> make_volume_map_params(sstring container_name, sstring volume_id, uint64_t block_it, sstring extent_group_id);
    future<> create(sstring container_name, sstring volume_id, uint64_t block_id, sstring extent_group_id);
    future<volume_map_entity> find(sstring container_name, sstring volume_id, uint64_t block_id);
    future<std::vector<volume_map_entity>> find_by_volume_id(sstring container_name, sstring volume_id);
    future<volume_map_entity> update(sstring container_name, sstring volume_id, uint64_t block_id, sstring new_extent_group_id);
    future<> remove(sstring container_name, sstring volume_id, uint64_t block_id);
    future<> remove(sstring container_name, sstring volume_id);

private:
    volume_map_entity convert_to_volume_map_entity(std::vector<variant> vt);
};

} //namespace hive
