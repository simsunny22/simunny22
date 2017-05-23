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

#include "service/storage_proxy.hh"
#include "cql3/untyped_result_set.hh"

#include "cql3/query_processor.hh"
#include "hive/exceptions/exceptions.hh"


namespace hive {

struct variant {
public:
    std::string name = "null";
    std::string type = "null";
    std::string str_val = "null";
    int64_t     int64_val = 0;
    double      double_val = 0.0;
    bool        bool_val = true;

    friend std::ostream & operator << (std::ostream & os, variant &v) {
        std::cout <<"name: " << v.name
                  <<", type: " << v.type 
                  <<", str_val: " << v.str_val 
                  <<", int64_val: " << v.int64_val 
                  <<", double_val: "<< v.double_val 
                  <<", bool_val: "<< v.bool_val 
                  <<std::endl;
        return os;
    }
};

inline db::consistency_level wire_to_consistency(int64_t v)
{
     switch (v) {
         case 0: return db::consistency_level::ANY;
         case 1: return db::consistency_level::ONE;
         case 2: return db::consistency_level::TWO;
         case 3: return db::consistency_level::THREE;
         case 4: return db::consistency_level::QUORUM;
         case 5: return db::consistency_level::ALL;
         case 6: return db::consistency_level::LOCAL_QUORUM;
         case 7: return db::consistency_level::EACH_QUORUM;
         case 8: return db::consistency_level::SERIAL;
         case 9: return db::consistency_level::LOCAL_SERIAL;
         case 10: return db::consistency_level::LOCAL_ONE;
         case 11: return db::consistency_level::VEGA_LOG; //for hive vega_log
         default:     throw exceptions::protocol_exception(sprint("Unknown code %d for a consistency level", v)); 
     }
}

struct scylla_option {
public:
    sstring _contact_points    = "172.16.24.86";
    int64_t _port              = 9042;
    int64_t _request_timeout   = 5000;
    bool    _is_local          = true;
    int64_t _consistency_level = 1;
};

class scylla_client {

private:
    scylla_option    _scylla_option; 
    api::timestamp_type _last_timestamp_micros = 0;

public:
    scylla_client(){
        _scylla_option._consistency_level= 1;
        _scylla_option._is_local= true;
    }
    scylla_client(scylla_option option)
                       :_scylla_option(option){
    }

    scylla_client(int64_t consistency_level){
        _scylla_option._consistency_level= consistency_level;
        _scylla_option._is_local= true;
    }

    int64_t get_consistency_level() {
      return _scylla_option._consistency_level;
    }

    variant make_variant(const cql3::untyped_result_set::row& row_, sstring type, sstring name);
    bytes get_value_by_type(variant& v, const column_definition& def);

    future<> insert(sstring cqlstring, std::unordered_map<std::string, variant> insert_params);
    future<std::vector<std::vector<variant> > > query(sstring cqlstring, std::vector<bytes_opt> query_values);

    api::timestamp_type get_timestamp();

private:
    future<std::vector<std::vector<variant> > > query_by_http(sstring cqlstring, std::vector<bytes_opt> query_values);
    future<std::vector<std::vector<variant> > > query_internal(sstring cqlstring, std::vector<bytes_opt> query_values);

    future<> insert_internal(sstring cqlstring, std::unordered_map<std::string, variant> insert_params);
    future<> insert_by_http(sstring cqlstring, std::unordered_map<std::string, variant> insert_params);

};

} //namespace hive
