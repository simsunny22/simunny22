#ifndef FILE_STORE_HH_
#define FILE_STORE_HH_ 

#include "dht/i_partitioner.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/temporary_buffer.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <chrono>
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "keys.hh"
#include "mutation.hh"
#include <list>
#include <seastar/core/rwlock.hh>

#include "hive/hive_config.hh"
#include "hive/journal/revision_data.hh"


namespace hive {


class malformed_file_store_exception : public std::exception {
    sstring _msg;
public:
    malformed_file_store_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};


future<> create_file(sstring path, size_t truncate_size=0, bool create_new = false);
future<> do_create_file(sstring path, size_t truncate_size);
future<> delete_file(sstring path);
future<> copy_file(sstring from_file, sstring to_file);


future<bytes> build_align_data(sstring path
                             , size_t read_offset
                             , size_t read_length
                             , size_t replay_offset
                             , bytes& old_data);
future<> write_file_align(sstring path, size_t offset, size_t size, bytes& data);
future<> write_file(sstring path, size_t offset, size_t size, bytes& data);

future<> write_file_ex(sstring path, size_t offset, size_t size, std::unique_ptr<char[], free_deleter> bufptr); 

//for batch_write_file
future<std::unique_ptr<char[], free_deleter>> 
read_align_data_for_batch_write(file fd, size_t align_offset, size_t align_size);

future<> do_write_for_batch(
    file fd
    , std::unique_ptr<char[], free_deleter> bufptr
    , size_t align_offset
    , size_t align_size);

future<> write_file_batch(sstring file_path, std::vector<revision_data> revision_datas);



future<temporary_buffer<char>> read_file(sstring path, size_t offset, size_t size); 
future<bytes>                  read_file_ex(sstring path, size_t offset, size_t size);
future<bytes>                  read_file_ex_align(sstring path, size_t offset, size_t size);



} //namespace hive

#endif /* FILE_STORE_HH_*/
