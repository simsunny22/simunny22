#pragma once
#include "core/sstring.hh"
#include "hive/http/json11.hh"
#include "context_entry.hh"

namespace hive {

class disk_context_entry : public context_entry {
private:
    sstring _id;
    sstring _ip;
    sstring _mount_path;
    sstring _type;
public:
    disk_context_entry(){}
    disk_context_entry(sstring id, sstring ip, sstring mount_path, sstring type)
        :_id(id), _ip(ip), _mount_path(mount_path), _type(type){}

    disk_context_entry(disk_context_entry& entry) {
        _id = entry._id;
        _ip = entry._ip;
        _mount_path = entry._mount_path;
        _type = entry._type;
    }

    disk_context_entry(const disk_context_entry& entry) {
        _id = entry._id;
        _ip = entry._ip;
        _mount_path = entry._mount_path;
        _type = entry._type;
    }

    disk_context_entry& operator=(disk_context_entry& entry) noexcept  {
        _id = entry._id;
        _ip = entry._ip;
        _mount_path = entry._mount_path;
        _type = entry._type;
        return *this;
    }


    disk_context_entry& operator=(const disk_context_entry& entry) noexcept  {
        _id = entry._id;
        _ip = entry._ip;
        _mount_path = entry._mount_path;
        _type = entry._type;
        return *this;
    }


    disk_context_entry(disk_context_entry&& entry) noexcept  {
        if(this != &entry){
            _id = entry._id;
            _ip = entry._ip;
            _mount_path = entry._mount_path;
            _type = entry._type;
        }
    }

    disk_context_entry& operator=(disk_context_entry&& entry) noexcept  {
        if(this != &entry){
            _id = entry._id;
            _ip = entry._ip;
            _mount_path = entry._mount_path;
            _type = entry._type;
        }
        return *this;
    }
    
    

    sstring get_entry_string(){
        hive::Json body_json = hive::Json::object {
          {"id", _id.c_str()},
          {"ip", _ip.c_str()},
          {"mount_path", _mount_path.c_str()},
          {"type", _type.c_str()},
        };
        auto body = body_json.dump();
        return body;
    }

    sstring get_id(){return _id;}
    sstring get_ip(){return _ip;}
    sstring get_mount_path(){return _mount_path;}
    sstring get_type(){return _type;}
};

} //namespace hive

