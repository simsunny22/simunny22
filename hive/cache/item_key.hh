#ifndef __HIVE_EXTENT_ITEM_KEY__
#define __HIVE_EXTENT_ITEM_KEY__

#include <string>
#include <iostream>
#include "core/temporary_buffer.hh"
#include "core/future.hh"


namespace hive {

static constexpr uint64_t default_key_length = 64; 

class item_key {
private:
    std::string _key;
    size_t _hash;
public:
    item_key(){}
    item_key(std::string key)
        : _key(key)
        , _hash(std::hash<std::string>()(key))
    {
        assert(_key.size() <= default_key_length);    
    }

    item_key(const item_key& other) {
        *this = other;
    }

    item_key(item_key&& other)
        : _key(std::move(other._key))
        , _hash(other._hash) {
        other._hash = 0;
    }

    size_t hash() const {
        return _hash;
    }

    const std::string key() const {
        return _key;
    }

    bool operator==(const item_key& other) const {
        return other._hash == _hash && other._key == _key;
    }

    item_key& operator=(item_key&& other) {
        _key = std::move(other._key);
        _hash = other._hash;
        other._hash = 0;
        return *this;
    }

    item_key& operator=(const item_key& other) {
        _key = other._key;
        _hash = other._hash;
        return *this;
    }

}; //class item_key

using version_type = uint64_t;
struct item_insertion_data {
    item_key key;
    version_type version;
    temporary_buffer<char> data;
  
    item_insertion_data() {}
    item_insertion_data(std::string k, version_type v, temporary_buffer<char>&& d)
        : key(k)
        , version(v)
        , data(std::move(d)){}

    item_insertion_data(const item_insertion_data& other) {
        *this = other;
    }

    item_insertion_data(item_insertion_data&& other)
        : key(std::move(other.key))
        , version(other.version)
        , data(std::move(other.data)){}

    item_insertion_data& operator=(const item_insertion_data& insertion) {
        key = insertion.key;
        version = insertion.version;

        temporary_buffer<char> tmp(insertion.data.size());
        std::copy_n(insertion.data.get(), insertion.data.size(), tmp.get_write());
        data = std::move(tmp);
        return *this;
    }

}; //struct item_insertion_data

} //namespace hive 

namespace std {

template<>
struct hash<hive::item_key> {
    size_t operator()(const hive::item_key& key) {
        return key.hash();
    }
};

} //namespace std

#endif
