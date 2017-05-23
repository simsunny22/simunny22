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

#include <msgpack.hpp>

#include "hive/metadata_service/entity/extent_group_entity.hh"
#include "hive/metadata_service/db_client/scylla_client.hh"

namespace hive {

class extent_group_map_helper {
private:
    scylla_client _client;
    sstring       _keyspace;
public:
    extent_group_map_helper(){
        _keyspace = "guru_metadata";
    }
    extent_group_map_helper(scylla_client client){
        _client = client;
        _keyspace = "guru_metadata";
    }

    std::unordered_map<std::string, variant> make_extent_group_map_params(sstring container_name, 
        sstring owner_id,
        sstring extent_group_id,
        sstring disk_id);
    future<> create(sstring container_name, sstring owner_id, sstring extent_group_id, sstring disk_id);
    future<extent_group_entity> find(sstring container_name, sstring extent_group_id);
    future<extent_group_entity> find(sstring container_name, sstring extent_group_id, sstring disk_id);
    future<> remove(sstring container_name, sstring extent_group_id);
    future<> remove(sstring container_name, sstring extent_group_id, sstring disk_id);

    future<std::vector<sstring>> get_extent_group_disks(sstring container_name, sstring extent_group_id);
    future<bool> get_created(sstring container_name, sstring extent_group_id);

    future<> update_extents(sstring container_name, sstring extent_group_id, sstring disk_id, std::vector<extent_entity> extents);
    future<> update_version(sstring container_name, sstring extent_group_id, sstring disk_id, uint64_t version);
    future<> update_created(sstring container_name, sstring extent_group_id, sstring disk_id, bool created);

    future<> test();
    future<bool> test1();
    future<> test2();
private:
    extent_group_entity convert_to_extent_group_entity(std::vector<variant> vt);
};

} //namespace hive
