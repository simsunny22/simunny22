#pragma once
#include "log.hh"
#include "core/future.hh"

#include "hive/trail/trail_service.hh"


namespace hive{

enum class trail_type{
    UNKNOW = 0,
    ACCESS_TRAIL,
};

class abstract_trail_params{
public:
    abstract_trail_params(){}
    ~abstract_trail_params(){}

    virtual trail_type get_trail_type(){
        return trail_type::UNKNOW;
    }
};

class abstract_trail {
public:
    abstract_trail(){
    }

    ~abstract_trail(){
    }

    virtual future<> create_trail(){
        return make_ready_future<>();
    }

    virtual future<> collect(trail_data data){
        return make_ready_future<>();
    }

    virtual future<> read_trail(){
       return make_ready_future<>();
    }

    virtual future<> trace(shared_ptr<abstract_trail_params> params){
        return make_ready_future<>();
    }

};



} //namespace hive


