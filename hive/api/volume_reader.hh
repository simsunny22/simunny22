#pragma once
#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>
#include "log.hh"
#include "types.hh"
#include "hive/hive_request.hh"

namespace hive{

using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class volume_data_source final : public data_source_impl {
private:
    lw_shared_ptr<hive_read_command>  _read_cmd;
    uint64_t _need_length = 0;
    future<> trace_access_trail(sstring disk_ids, sstring extent_group_id, sstring node_id, uint64_t size, sstring options);
public:
    explicit volume_data_source(lw_shared_ptr<hive_read_command> read_cmd)
        : _read_cmd(std::move(read_cmd)){
       _need_length = _read_cmd->length; 
    }

    virtual future<temporary_buffer<char>> get() override;
};

struct volume_reader {
public:
    std::unique_ptr<input_stream<char>> _is = nullptr;
    std::unique_ptr<reply>   _rep = nullptr;

public:
    volume_reader(lw_shared_ptr<hive_read_command> read_cmd, std::unique_ptr<reply> rep);

    using unconsumed_remainder = std::experimental::optional<temporary_buffer<char>>;
    future<unconsumed_remainder> operator()(temporary_buffer<char> data) {
        if (data.empty()) {
            _rep->done();
            return make_ready_future<unconsumed_remainder>(std::move(data));
        } else {
            _rep->_content.append(data.get(), data.size());
            return make_ready_future<unconsumed_remainder>();
        }
    }

};

}//namespace hive
