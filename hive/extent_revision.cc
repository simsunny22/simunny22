#include "hive/extent_revision.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

static logging::logger logger("primary_journal");
namespace hive {

extent_revision::extent_revision(){} 
extent_revision::~extent_revision(){}

void extent_revision::set_data(temporary_buffer<char> data_buf){
    using byte = bytes_view::value_type;
    bytes_view data_view = bytes_view(reinterpret_cast<const byte*>(data_buf.get()), data_buf.size());
    data = to_bytes(data_view);
}

hive::extent_revision extent_revision::from_raw_result(const hive_result& result) {
    extent_revision revision;
    revision.data = value_cast<bytes>(bytes_type->deserialize(result.buf().view()));
    return std::move(revision);
}

uint64_t extent_revision::serialize_size(){
    uint64_t  size = 0;
    auto uint64_size = sizeof(uint64_t);

    //owner_id
    size = size + uint64_size + owner_id.size();
    //extent_group_id
    size = size + uint64_size + extent_group_id.size();
    //extent_id
    size = size + uint64_size + extent_id.size();
    //disk_ids
    size = size + uint64_size + disk_ids.size();
    //options
    size = size + uint64_size + options.size();
    //intent_id
    size = size + uint64_size + intent_id.size();
    //extent_offset_in_group
    size = size + uint64_size;
    //data_offset_in_extent
    size = size + uint64_size;
    //vclock
    size = size + uint64_size;
    //lenght
    size = size + uint64_size;

    //return size;
    //bytes
    return size + uint64_size + data.size();

}
// ==================================================
// serialize
// ==================================================
uint64_t extent_revision::serialize_sstring(char* buf, uint64_t pos, sstring& value, sstring description){
    auto uint64_size = sizeof(uint64_t);
    uint64_t length = value.size();

    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;

    std::memcpy(buf+pos, value.begin(), length);
    pos += length;
    //logger.error("extent_revision::serialize_sstring {}:{}, length:{}, pos:{}", description, value, length, pos);
    return pos;
}

uint64_t extent_revision::serialize_uint64(char* buf, uint64_t pos, uint64_t& value, sstring description){
    auto uint64_size = sizeof(uint64_t);

    std::memcpy(buf+pos, &value, uint64_size);
    pos += uint64_size;

    //logger.error("extent_revision::serialize_uint64 {}:{}, pos:{}", description, value, pos);
    return pos;
}

void extent_revision::serialize(std::unique_ptr<char[], free_deleter>& bufptr){
    auto buf = bufptr.get();
    auto uint64_size = sizeof(uint64_t);
    uint64_t pos = 0;

    
    //logger.error("extent_revision::serialize start ......");

    //volume_id
    pos = serialize_sstring(buf, pos, owner_id, "owner_id");

    //extent_group_id
    pos = serialize_sstring(buf, pos, extent_group_id, "extent_group_id");

    //extent_id
    pos = serialize_sstring(buf, pos, extent_id, "extent_id");

    //disk_ids
    pos = serialize_sstring(buf, pos, disk_ids, "disk_ids");

    //options
    pos = serialize_sstring(buf, pos, options, "options");

    //intent_id
    pos = serialize_sstring(buf, pos, intent_id, "inent_id");

    //extent_offset_in_group
    pos = serialize_uint64(buf, pos, extent_offset_in_group, "extent_offset_in_group");

    //data_offset_in_extent
    pos = serialize_uint64(buf, pos, data_offset_in_extent, "data_offset_in_extent");

    //vclock
    pos = serialize_uint64(buf, pos, vclock, "vclock");

    //length
    pos = serialize_uint64(buf, pos, length, "length");

    //bytes
    uint64_t length = data.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, data.begin(), data.size());
    pos += data.size();
}
// ==================================================
// deserialize
// ==================================================
sstring extent_revision::deserialize_sstring(char* buf, uint64_t pos, sstring description){
    auto uint64_size = sizeof(uint64_t);
    uint64_t length;

    std::memcpy(&length, buf+pos, uint64_size);
    pos += uint64_size;

    sstring dest_str(sstring::initialized_later(), length);
    std::memcpy(dest_str.begin(), buf+pos, length);
    pos += length;
   
    //logger.error("extent_revision::deserialize_sstring {}:{}, length:{}, pos:{}", description, dest_str, length, pos);
    return dest_str;
}

uint64_t extent_revision::deserialize_uint64(char* buf, uint64_t pos, sstring description){
     auto uint64_size = sizeof(uint64_t);

     uint64_t value;
     std::memcpy(&value, buf+pos, uint64_size);
     //logger.error("extent_revision::deserialize_uint64 {}:{},  pos:{}", description, value,  pos + uint64_size);

     return value;
}

void extent_revision::deserialize(std::unique_ptr<char[], free_deleter>&  bufptr, uint64_t pos){
     //logger.error("extent_revision::deserialize start ......");
     auto buf = bufptr.get();
     auto uint64_size = sizeof(uint64_t);
     //uint64_t pos = 0;

     //volume_id
     owner_id = deserialize_sstring(buf, pos, "volume_id");
     pos = pos + uint64_size + owner_id.size();

     //extent_group_id
     extent_group_id = deserialize_sstring(buf, pos, "extent_group_id");
     pos = pos + uint64_size + extent_group_id.size();

     //extent_id
     extent_id = deserialize_sstring(buf, pos, "extent_id");
     pos = pos + uint64_size + extent_id.size();

     //disk_ids
     disk_ids = deserialize_sstring(buf, pos, "disk_ids");
     pos = pos + uint64_size + disk_ids.size();

     //options
     options = deserialize_sstring(buf, pos, "options");
     pos = pos + uint64_size + options.size();

     //intent_id
     intent_id = deserialize_sstring(buf, pos, "intent_id");
     pos = pos + uint64_size + intent_id.size();

     //extent_offset_in_group
     extent_offset_in_group = deserialize_uint64(buf, pos, "extent_offset_in_group");
     pos = pos + uint64_size;

     //data_offset_in_extent
     data_offset_in_extent = deserialize_uint64(buf, pos, "data_offset_in_extent");
     pos = pos + uint64_size;

     //vclock
     vclock = deserialize_uint64(buf, pos, "vclock");
     pos = pos + uint64_size;

     //length
     length = deserialize_uint64(buf, pos, "length");
     pos = pos + uint64_size;

     //bytes
     uint64_t b_length;
     std::memcpy(&b_length, buf+pos, uint64_size);
     pos += uint64_size;
     bytes buffer(b_length, 0);
     std::memcpy(buffer.begin(), buf+pos, b_length);
     data = std::move(buffer);

}

std::ostream& operator<<(std::ostream& out, const extent_revision& revision) {
    return out << "{extent_revision:["
               << "owner_id:" << revision.owner_id
               << ", extent_group_id:" << revision.extent_group_id
               << ", extent_id:" << revision.extent_group_id
               << ", extent_offset_in_group:" << revision.extent_offset_in_group
               << ", data_offset_in_extent:" << revision.data_offset_in_extent
               << ", length:" << revision.length
               << ", vclock:" << revision.vclock
               << ", disk_ids:" << revision.disk_ids
               << ", options:" << revision.options
               << "}]";
}

} //namespace hive
