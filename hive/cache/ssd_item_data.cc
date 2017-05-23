#include "ssd_item_data.hh"
#include "seastar/core/simple-stream.hh"

#include "hive/file_store.hh"
#include "log.hh"

using namespace ser;

namespace hive { 
static logging::logger logger("ssd_item_data");

static uint64_t data_size = 1024UL * 64UL;
static uint64_t key_size  = 64UL;

///////////////
// for global
//////////////
uint64_t get_ssd_item_data_size(size_t slab_size) {
    constexpr size_t field_alignment = alignof(void*);
    uint64_t size = sizeof(ssd_item_data) 
                  + align_up(key_size, field_alignment) 
                  + align_up(slab_size, field_alignment);
    return align_up(size, 512UL);
}

/////////////////////////////
// for class ssd_item_data 
/////////////////////////////
ssd_item_data::ssd_item_data(uint64_t offset
                           , uint64_t size
                           , uint64_t version
                           , sstring key
                           , temporary_buffer<char> data) 
                               : _offset(offset)
                               , _size(size)
                               , _version(version)
{
    _key = temporary_buffer<char>(key_size);
    std::copy_n(key.c_str(), key.size(), _key.get_write());
    _data = std::move(data);
}

temporary_buffer<char> ssd_item_data::serialize(size_t slab_size) {
    uint64_t size = get_ssd_item_data_size(slab_size); 
    temporary_buffer<char> buffer = temporary_buffer<char>(size);
    
    size_t pos = 0;
    memcpy(buffer.get_write(), &_offset, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(buffer.get_write() + pos, &_size, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(buffer.get_write() + pos, &_version, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    //tododl:yellow use memcpy also ok?
    std::copy_n(_key.begin(), key_size, buffer.get_write() + pos);
    pos += key_size; 
    std::copy_n(_data.begin(), data_size, buffer.get_write() + pos);
    
    return std::move(buffer);
}

void ssd_item_data::deserialize(temporary_buffer<char>&& buf, size_t slab_size) {
    assert(buf.size() == get_ssd_item_data_size(slab_size));
    _offset  = 0UL;
    _size    = 0UL;
    _version = 0UL;
    
    size_t pos = 0;
    memcpy(&_offset, buf.begin(), sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&_size, buf.begin() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&_version, buf.begin() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
   
    //tododl:yellow use memcpy also ok?
    std::copy_n(buf.begin() + pos, key_size, _key.get_write());
    pos += key_size; 
    std::copy_n(buf.begin() + pos, data_size, _data.get_write());
}

future<ssd_item_data> read_file(sstring path, uint64_t offset, uint32_t size, size_t slab_size) {
    return hive::read_file(path, offset, size).then([size, slab_size] (auto&& fdata) {
        ssd_item_data datum;
        datum.deserialize(std::move(fdata), slab_size);
        return make_ready_future<ssd_item_data>(std::move(datum)); 
    });
}

future<> write_file(sstring path, uint64_t offset, ssd_item_data& item_data, size_t slab_size) {
    temporary_buffer<char> buffer = item_data.serialize(slab_size);
    uint64_t size = get_ssd_item_data_size(slab_size);
    bytes data(reinterpret_cast<const signed char*>(buffer.begin()), buffer.size());

    return do_with(std::move(data), [path, offset, size](auto& data){
        return hive::write_file(path, offset, size, data);
    });
}

} //namespace hive
