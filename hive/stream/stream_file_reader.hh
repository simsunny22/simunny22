#ifndef __HIVE_FILE_READER__
#define __HIVE_FILE_READER__

#include "core/fstream.hh"
#include "core/file.hh"
#include "core/future.hh"
#include "core/temporary_buffer.hh"

#include "hive/stream/hive_file_data_source.hh"

namespace hive {

class stream_file_reader {
    hive_file_data_source _fd; 
public:
    stream_file_reader(file f_, uint64_t offset_, uint64_t length_, file_input_stream_options options)
        : _fd(std::move(hive_file_data_source(std::move(f_), offset_, length_, options))) {
    }

    future<temporary_buffer<char>> operator()() {
        return _fd.get();
    }
};

} //namespace hive

#endif //__HIVE_FILE_READER__
