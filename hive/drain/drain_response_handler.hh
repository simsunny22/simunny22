#pragma once
#include "core/sstring.hh" 
#include "gms/inet_address.hh"
#include "hive/message_data_type.hh"
#include "hive/hive_tools.hh"
#include "hive/drain/drain_task_group.hh"
#include <set>
#include <map>


namespace hive {
using task_id_set = std::set<uint64_t>; 

class drain_response_handler {
private:
    uint64_t _generation;
    std::map<uint64_t, drain_task_group> _task_groups; 
    std::set<uint64_t> _targets; 
    std::set<uint64_t> _success; 
    promise<task_id_set> _ready; //return the success targets address

    size_t  _cl_acks = 0;
    bool    _cl_achieved = false;
private:
    virtual size_t total_block_for() {
        return _targets.size();
    }
   
public:
    drain_response_handler(int64_t generation = 0):_generation(generation){}

    virtual ~drain_response_handler() {
        if (!_cl_achieved) {
            auto error_info = sprint("handler destruct but has not enough response, targets:{}, success:{}"
                , _targets.size(), _success.size());
            _ready.set_exception(std::runtime_error(error_info));
        }
    };

    void add_task_groups(std::vector<drain_task_group> task_groups){
        for(auto entry:task_groups){
            if(entry.job_generation == _generation){
                uint64_t id = entry.group_id;
                _targets.insert(id); 
                _task_groups.insert(std::make_pair(id, entry));
            }
        }
    };
    std::map<uint64_t, drain_task_group>& get_task_groups(){
        return _task_groups;
    }  

    void trigger_exception(std::exception ex){
        if(!_cl_achieved){
            _cl_achieved = true;
            _ready.set_exception(ex);
        }
    }

    void trigger_stop(){
        if(!_cl_achieved){
            _cl_achieved = true;
            _ready.set_value(_success);
        }
    }

    void signal(size_t nr = 1) {
        _cl_acks += nr;
        if (!_cl_achieved && _cl_acks >= total_block_for()) {
            _cl_achieved = true;
            _ready.set_value(_success);
        }
    }

    // return true on last ack
    bool response(uint64_t id, uint64_t generation) {
        if(generation == _generation){
            auto itor = _targets.find(id);
            if(itor != _targets.end()) {
                _success.insert(id);
                signal();
            }
        }
        return _success.size() == _targets.size();
    }
    
    future<task_id_set> wait() {
        return _ready.get_future();
    }

    std::set<uint64_t> get_targets(){
        return _targets;
    }

    std::set<uint64_t> get_success(){
        return _success;
    }

    uint64_t generation() {
        return _generation;
    }

    bool achieved() {
        return _cl_achieved; 
    } 
   
};//class drain_response_handler




}//namespace hive

