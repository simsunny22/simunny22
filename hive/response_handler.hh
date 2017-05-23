#pragma once
#include "core/sstring.hh" 
#include "gms/inet_address.hh"
#include "hive/message_data_type.hh"
#include "hive/hive_tools.hh"


namespace hive {

using response_entry = std::unordered_set<gms::inet_address>; 

class common_response_handler {
private:
    uint64_t _response_id;
    //std::shared_ptr<abstract_md> _data;
    union_md _data;
    std::unordered_set<gms::inet_address> _targets; 
    std::unordered_set<gms::inet_address> _success; 
    promise<response_entry> _ready; //return the success targets address

    size_t  _cl_acks = 0;
    bool    _cl_achieved = false;
private:
    virtual size_t total_block_for() {
        return _targets.size();
    }
   
public:
    common_response_handler(
        uint64_t response_id, 
        //std::shared_ptr<abstract_md> data, 
        union_md data,
        std::unordered_set<gms::inet_address> targets )
        : _response_id(response_id)
        , _data(std::move(data))
        , _targets(std::move(targets))
    {}

    virtual ~common_response_handler() {
        if (!_cl_achieved) {
            auto error_info = sprint("handler destruct but has not enough response, targets:{}, success:{}"
                , hive_tools::format(_targets), hive_tools::format(_success));
            _ready.set_exception(std::runtime_error(error_info));
        }
    };

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
    bool response(gms::inet_address from) {
        auto itor = _targets.find(from);
        if(itor != _targets.end()) {
            _success.insert(from);
            signal();
        }
        return _success.size() == _targets.size();
    }
    
    future<response_entry> wait() {
        return _ready.get_future();
    }

    std::unordered_set<gms::inet_address> get_targets(){
        return _targets;
    }

    std::unordered_set<gms::inet_address> get_success(){
        return _success;
    }

    //std::shared_ptr<abstract_md> get_data() {
    //    return _data;
    //}

    union_md& get_data(){
        return _data;
    }

    
    uint64_t response_id() {
        return _response_id;
    }

    bool achieved() {
        return _cl_achieved; 
    } 

};//class common_response_handler




}//namespace hive

