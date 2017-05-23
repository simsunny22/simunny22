#pragma once
#include "core/sstring.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"
#include "context_entry.hh"

namespace hive {

enum monad_handle_type {
    UNKOWN_HANDLE_TYPE   = 0,
    CREATE_EXTENT_GROUP  = 1,
    WRITE_EXTENT_GROUP   = 2,
    READ_EXTENT_GROUP    = 3,
    DELETE_EXTENT_GROUP  = 4,
    MIGRATE_EXTENT_GROUP = 5,
};

class extent_group_context_entry :public context_entry{
private:
    sstring           _extent_group_id;
    sstring           _disk_ids;
    uint64_t          _vclock;
    //tododl:yellow meybe remove bellow code
    bool              _credible = true;
    monad_handle_type _handle_type = UNKOWN_HANDLE_TYPE;

public:
    extent_group_context_entry(){}
    extent_group_context_entry(sstring extent_group_id
                             , sstring disk_ids
                             , uint64_t vclock)
                                 :_extent_group_id(extent_group_id)
                                 , _disk_ids(disk_ids)
                                 ,_vclock(vclock){}

    extent_group_context_entry(extent_group_context_entry& entry){
        _extent_group_id = entry._extent_group_id;
        _disk_ids        = entry._disk_ids;
        _vclock          = entry._vclock;
        _credible        = entry._credible;
        _handle_type     = entry._handle_type;
    }

    extent_group_context_entry(const extent_group_context_entry& entry){
        _extent_group_id = entry._extent_group_id;
        _disk_ids        = entry._disk_ids;
        _vclock          = entry._vclock;
        _credible        = entry._credible;
        _handle_type     = entry._handle_type;
    }

    extent_group_context_entry& operator=(extent_group_context_entry& entry) noexcept  {
        _extent_group_id = entry._extent_group_id;
        _disk_ids        = entry._disk_ids;
        _vclock          = entry._vclock;
        _credible        = entry._credible;
        _handle_type     = entry._handle_type;
        return *this;
    }
    
    extent_group_context_entry& operator=(const extent_group_context_entry& entry) noexcept  {
        _extent_group_id = entry._extent_group_id;
        _disk_ids        = entry._disk_ids;
        _vclock          = entry._vclock;
        _credible        = entry._credible;
        _handle_type     = entry._handle_type;
        return *this;
    }

    extent_group_context_entry(extent_group_context_entry&& entry) noexcept  {
        if(this != &entry){
            _extent_group_id = entry._extent_group_id;
            _disk_ids        = entry._disk_ids;
            _vclock          = entry._vclock;
            _credible        = entry._credible;
            _handle_type     = entry._handle_type;
        }
    }

    extent_group_context_entry& operator=(extent_group_context_entry&& entry) noexcept  {
        if(this != &entry){
            _extent_group_id = entry._extent_group_id;
            _disk_ids        = entry._disk_ids;
            _vclock          = entry._vclock;
            _credible        = entry._credible;
            _handle_type     = entry._handle_type;
        }
        return *this;
    }



    sstring get_entry_string(){ 
        sstring handle_type_string = "";
        if (_handle_type == UNKOWN_HANDLE_TYPE) {
            handle_type_string = "UNKOWN_HANDLE_TYPE";
        }else if(_handle_type == CREATE_EXTENT_GROUP){
            handle_type_string = "CREATE_EXTENT_GROUP";
        }else if(_handle_type == WRITE_EXTENT_GROUP){
            handle_type_string = "WRITE_EXTENT_GROUP";
        }else if(_handle_type == READ_EXTENT_GROUP){
            handle_type_string = "READ_EXTENT_GROUP";
        }else if(_handle_type == DELETE_EXTENT_GROUP){
            handle_type_string = "DELETE_EXTENT_GROUP";
        }else if(_handle_type == MIGRATE_EXTENT_GROUP){
            handle_type_string = "MIGRATE_EXTENT_GROUP";
        }
        
        hive::Json body_json = hive::Json::object {
          {"extent_group_id", _extent_group_id.c_str()},
          {"disk_ids", _disk_ids.c_str()},
          {"vclock", to_sstring(_vclock).c_str()},
          {"credible", to_sstring(_credible).c_str()},
          {"handle_type", handle_type_string.c_str()},
        };
        auto body = body_json.dump();
        return body;
    }
   

    sstring get_extent_group_id(){return _extent_group_id;}
    sstring get_disk_ids(){return _disk_ids;}
    int64_t get_vclock(){return _vclock;}

    void    update_credible(bool credible){_credible = credible;}
    bool    is_credible(){return _credible;}

    void update_handle_type(monad_handle_type handle_type){_handle_type = handle_type;}
    monad_handle_type get_handle_type(){return _handle_type;}

    bool check_disk_id_exist(sstring disk_id){
        auto disks = hive_tools::split_to_unordered_set(_disk_ids, ":"); 
        return disks.count(disk_id) > 0;
    }
};

} //namespace hive

