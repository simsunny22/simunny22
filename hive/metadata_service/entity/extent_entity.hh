#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

#include <stdlib.h>
#include <msgpack.hpp>

namespace hive {

class extent_entity {
public:
    std::string       _extent_id;
    uint64_t           _offset;
    uint64_t           _ref_count;
    std::string       _check_sum;

    MSGPACK_DEFINE(_extent_id, _offset, _ref_count, _check_sum);                                               

public:
    extent_entity(){
      _extent_id = "undefined";
    }
    extent_entity(sstring extent_id
                ,uint64_t offset
                ,uint64_t ref_count
                ,sstring check_sum)
                :_extent_id(extent_id)
                ,_offset(offset)
                ,_ref_count(ref_count)
                ,_check_sum(check_sum){
    }

    extent_entity(extent_entity& entity){
        _extent_id = entity._extent_id;
        _check_sum       = entity._check_sum;
        _ref_count = entity._ref_count;
        _offset    = entity._offset;
    }

    extent_entity(const extent_entity& entity){
        _extent_id = entity._extent_id;
        _check_sum       = entity._check_sum;
        _ref_count = entity._ref_count;
        _offset    = entity._offset;
    }

    extent_entity& operator=(extent_entity& entity) noexcept  {
        _extent_id = entity._extent_id;
        _check_sum       = entity._check_sum;
        _ref_count = entity._ref_count;
        _offset    = entity._offset;
        return *this;
    }
    
    extent_entity& operator=(const extent_entity& entity) noexcept  {
        _extent_id = entity._extent_id;
        _check_sum       = entity._check_sum;
        _ref_count = entity._ref_count;
        _offset    = entity._offset;
        return *this;
    }

    extent_entity(extent_entity&& entity) noexcept  {
        if(this != &entity){
            _extent_id = entity._extent_id;
            _check_sum       = entity._check_sum;
            _ref_count = entity._ref_count;
            _offset    = entity._offset;
        }
    }

    extent_entity& operator=(extent_entity&& entity) noexcept  {
        if(this != &entity){
            _extent_id = entity._extent_id;
            _check_sum       = entity._check_sum;
            _ref_count = entity._ref_count;
            _offset    = entity._offset;
        }
        return *this;
    }

    sstring get_entity_string(){ 
        hive::Json body_json = hive::Json::object {
          {"extent_id", _extent_id.c_str()},
          {"offset",    to_sstring(_offset).c_str()},
          {"ref_count", to_sstring(_ref_count).c_str()},
          {"check_sum", _check_sum.c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_extent_id(){return _extent_id;}
    sstring get_check_sum(){return _check_sum;}
    uint64_t get_ref_count(){return _ref_count;}
    uint64_t get_offset(){return _offset;}



  //  friend std::ostream & operator << (std::ostream & os, extent_entity &v) {                                                 
  //    std::cout <<"extent_id: " << v._extent_id
  //      <<", offset: " << v._offset
  //      <<", ref_count: " << v._ref_count
  //      <<", check_sum: " << v._check_sum
  //      <<std::endl;
  //    return os;
  //  }
};

} //namespace hive
