#ifndef __HIVE_SSD_ITEM_DATA__
#define __HIVE_SSD_ITEM_DATA__

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include <sstream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include "utils/data_output.hh"


namespace hive {

class ssd_item_data {
public:
    uint64_t _offset;
    uint64_t _size;
    uint64_t _version;
    temporary_buffer<char> _key; 
    temporary_buffer<char> _data;
public:
    ssd_item_data(uint64_t offset, uint64_t size, uint64_t version, sstring key, temporary_buffer<char> data);
    ssd_item_data()
        : _key(64)
        , _data(1024 * 64){}

    ssd_item_data(ssd_item_data&& other) noexcept{
        _offset  = other._offset;
        _size    = other._size;
        _version = other._version;
        _key     = std::move(other._key);
        _data    = std::move(other._data );
    }
       /// Moves a object.
    ssd_item_data& operator=(ssd_item_data&& x) noexcept{
        if( this != &x ){
          _offset  = x._offset;
          _size    = x._size;
          _version = x._version;
          _key     = std::move(x._key);
          _data    = std::move(x._data );
        }
        return *this;
    }
    
    uint64_t offset() {
        return _offset; 
    }
    
    uint64_t size() {
        return _size;
    }
 
    uint64_t version() {
        return _version;
    }
    
    temporary_buffer<char> serialize(size_t slab_size);
    void deserialize(temporary_buffer<char>&& buf, size_t slab_size);

}; //class ssd_item_data

uint64_t get_ssd_item_data_size(size_t slab_size);

future<> write_file(sstring path, uint64_t offset, ssd_item_data& item_data, size_t slab_size);
future<ssd_item_data> read_file(sstring path, uint64_t offset, uint32_t size, size_t slab_size);


} //namespace hive 

#endif
