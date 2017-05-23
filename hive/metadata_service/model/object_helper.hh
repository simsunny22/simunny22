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

#include "hive/metadata_service/entity/object_entity.hh"
#include "hive/metadata_service/db_client/scylla_client.hh"

namespace hive {

class object_helper {
private:
    scylla_client _client;
    sstring       _keyspace;
public:
    object_helper(){
        _keyspace = "guru_metadata";
    }
    object_helper(scylla_client client){
        _client = client;
        _keyspace = "guru_metadata";
    }

    future<> update_object_last_extent_group(sstring object_id, sstring extent_group_id);
    future<> update_object_size(sstring object_id, uint64_t new_size);

};

} //namespace hive
