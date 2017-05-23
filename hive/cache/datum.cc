#include "hive/cache/datum.hh"
#include <ostream>
#include <seastar/core/sstring.hh>

namespace hive {

std::ostream& operator<<(std::ostream& os, const datum& differ) {
    os << "datum";
    
    return os;
}

temporary_buffer<char> datum::serialize() {
    uint64_t data_len = data?data->length():0;
    uint64_t total_len = sizeof(uint64_t) + sizeof(datum_origin) + sizeof(uint64_t) + data_len;
std::cout<<"total_len "<<total_len<<std::endl;
    temporary_buffer<char> buffer = temporary_buffer<char>(total_len);
    memset(buffer.get_write(), 0, total_len);

    size_t pos = 0;
    //seq 
    memcpy(buffer.get_write()+pos, &seq, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    //origin
    memcpy(buffer.get_write()+pos, &origin, sizeof(datum_origin));
    pos += sizeof(datum_origin);
    //data length
    memcpy(buffer.get_write()+pos, &data_len, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    //data
    if(data){
        memcpy(buffer.get_write()+pos, data->begin(), data_len);
    }

    return std::move(buffer);
}

void datum::deserialize(const char* buf, size_t size) {
    size_t pos = 0;
    //seq 
    memcpy(&seq, buf + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    //origin
    memcpy(&origin, buf + pos, sizeof(datum_origin));
    pos += sizeof(datum_origin);

    //data
    uint64_t data_len = 0;
    memcpy(&data_len, buf + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    if(data_len != 0){
        bytes data_buf(data_len, 0);
        std::memcpy(data_buf.begin(), buf + pos, data_len);;
        data = make_lw_shared<bytes>(std::move(data_buf));
    }else{
        data = nullptr;
    }
}

}//namespace hive
