#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "bytes.hh"
#include "types.hh"
#include "log.hh"

namespace hive {

logging::logger log_ed("extent_datum");

extent_datum::extent_datum():extent_id("")
                  ,extent_group_id("")
                  ,extent_offset_in_group(0)
                  ,candidate_disk_ids("")
                  ,hit_disk_ids("")
                  ,version(0)
                  ,md5(""){} 
extent_datum::~extent_datum(){}

void extent_datum::set_data(temporary_buffer<char> buf){
    using byte = bytes_view::value_type;
    bytes_view data_view = bytes_view(reinterpret_cast<const byte*>(buf.get()), buf.size());
    data = to_bytes(data_view);
}

hive::extent_datum extent_datum::from_raw_result(const hive_result& result) {
    extent_datum datum;
    datum.data = value_cast<bytes>(bytes_type->deserialize(result.buf().view()));
    return std::move(datum);
}

std::ostream& operator<<(std::ostream& out, const extent_datum& datum) {
    return out << "{extent_datum:["
               << "extent_id:" << datum.extent_group_id
               << ", extent_group_id:" << datum.extent_group_id
               << ", extent_offset_in_group:" << datum.extent_offset_in_group
               << ", candidate_disk_ids:" << datum.candidate_disk_ids
               << ", hit_disk_ids:" << datum.hit_disk_ids
               << ", data:" <<datum.data 
               << "]}";
}

void extent_datum::serialize(std::unique_ptr<char[], free_deleter>& bufptr){
    //assert((hive_config::extent_header_size+hive_config::extent_size)==bufptr.get());
log_ed.debug("dltest===>>> serialize, extent_id:{}", extent_id);
log_ed.debug("dltest===>>> serialize, extent_offset_in_group:{}", extent_offset_in_group);
log_ed.debug("dltest===>>> serialize, candidate_disk_ids:{}", candidate_disk_ids);
log_ed.debug("dltest===>>> serialize, hit_disk_ids:{}", hit_disk_ids);
log_ed.debug("dltest===>>> serialize, version:{}", version);
log_ed.debug("dltest===>>> serialize, md5:{}", md5);

    auto buf = bufptr.get();
    auto uint64_size = sizeof(uint64_t);
    uint64_t pos = hive_config::extent_header_digest_size;  

    // extent_id sstring
    int64_t length = extent_id.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, extent_id.begin(), extent_id.size());
    pos += extent_id.size();
    
    // extent_group_id sstring 
    length = extent_group_id.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, extent_group_id.begin(), extent_group_id.size());
    pos += extent_group_id.size();
    
    //extent_offset_in_group int64_t
    length = uint64_size;
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, &extent_offset_in_group, uint64_size);
    pos += uint64_size;

    //candidate_disk_ids sstring
    length = candidate_disk_ids.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, candidate_disk_ids.begin(), candidate_disk_ids.size());
    pos += candidate_disk_ids.size();
   
    //hit_disk_ids sstring
    length = hit_disk_ids.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, hit_disk_ids.begin(), hit_disk_ids.size());
    pos += hit_disk_ids.size();
   
    //version int64_t
    length = uint64_size;
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, &version, uint64_size);
    pos += uint64_size;

    //TODO compute md5 by datas
    //md5 sstring 
    length = md5.size();
    std::memcpy(buf+pos, &length, uint64_size);
    pos += uint64_size;
    std::memcpy(buf+pos, md5.begin(), md5.size());
    pos += md5.size();

    //header digest
    auto header_digest_size = hive_config::extent_header_digest_size;
    auto header_data_size = hive_config::extent_header_size - header_digest_size;
    bytes header_digest = hive_tools::calculate_digest(reinterpret_cast<const unsigned char*>(buf+header_digest_size)
                                                     , header_data_size);
    
    assert(header_digest.size() == (size_t)header_digest_size);
    std::memcpy(buf, header_digest.begin(), header_digest.size());

    //data bytes 
    std::memcpy(buf+hive_config::extent_header_size, data.begin(), data.size());
}

void extent_datum::init_empty_datum() {
    extent_id = "";
    extent_group_id = "";
    extent_offset_in_group = 0;
    candidate_disk_ids = "";
    hit_disk_ids = "";
    version = 0;
    md5 = "";
    bytes empty_buffer(bytes::initialized_later(), hive_config::extent_size);
    std::memset(empty_buffer.begin(), 0, hive_config::extent_size);
    data = std::move(empty_buffer);
}

bool extent_datum::is_extent_datum_exist(bytes& extent_header_digest) {
    bytes empty_buffer(bytes::initialized_later(), extent_header_digest.size());
    std::memset(empty_buffer.begin(), 0, extent_header_digest.size());
    auto cmp_result = std::memcmp(empty_buffer.begin(), extent_header_digest.begin(), extent_header_digest.size());
    return cmp_result != 0 ? true : false;
}

void extent_datum::deserialize(std::unique_ptr<char[], free_deleter> bufptr){
    auto buf = bufptr.get(); 
    auto uint64_size = sizeof(uint64_t);
    uint64_t pos = 0;
   
    //header digest
    auto header_digest_size = hive_config::extent_header_digest_size;
    bytes read_header_digest(bytes::initialized_later(), header_digest_size);
    std::memset(read_header_digest.begin(), 0, header_digest_size);
    std::memcpy(read_header_digest.begin(), buf+pos, header_digest_size); 
    pos += header_digest_size;
    
    if(!is_extent_datum_exist(read_header_digest)) {
        init_empty_datum();
        return;
    }

    //check header digest
    auto header_data_size = hive_config::extent_header_size - header_digest_size; 
    bytes header_data(bytes::initialized_later(), header_data_size); 
    std::memset(header_data.begin(), 0, header_data_size);
    std::memcpy(header_data.begin(), buf+pos, header_data_size);
    bytes calculate_header_digest = hive_tools::calculate_digest(reinterpret_cast<const unsigned char*>(header_data.begin())
                                                               , header_data.size()); 

    assert(calculate_header_digest.size() == (size_t)header_digest_size);
    if(std::memcmp(read_header_digest.begin(), calculate_header_digest.begin(), header_digest_size) != 0) {
        throw std::runtime_error("extent_datum deserialize, read_header_digest not equal with calculate_header_digest");    
    }
    
    //extent_id sstring
    uint64_t length = 0;
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&extent_id, buf+pos, length);
    pos += length;

    //extent_group_id sstring
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&extent_group_id, buf+pos, length);
    pos += length;
    
    //extent_offset_in_group int64_t
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&extent_offset_in_group, buf+pos, length);
    pos += length;

    //candidate_disk_ids sstring 
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&candidate_disk_ids, buf+pos, length);
    pos += length;

    //hit_disk_ids sstring 
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&hit_disk_ids, buf+pos, length);
    pos += length;

    //version int64_t 
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&version, buf+pos, length);
    pos += length;

    //md5 sstring 
    std::memcpy(&length, buf+pos, uint64_size); 
    pos += uint64_size;
    std::memcpy(&md5, buf+pos, length);
    pos += length;

    //data bytes
    bytes data_buf(hive_config::extent_size, 0);  
    std::memcpy(data_buf.begin(), buf+hive_config::extent_header_size, hive_config::extent_size);
    data = std::move(data_buf);
}


} //namespace hive
