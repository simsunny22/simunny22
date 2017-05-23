#include "volume_reader.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/lexical_cast.hpp>

#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"

#include "log.hh"
#include "hive/trail/trail_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/hive_service.hh"

namespace hive{

static logging::logger logger("volume_reader");

static std::unique_ptr<input_stream<char>> 
make_volume_input_stream(lw_shared_ptr<hive_read_command> read_cmd) {
    return std::make_unique<input_stream<char>>(
        data_source(std::make_unique<volume_data_source>(std::move(read_cmd))));
}

volume_reader::volume_reader(lw_shared_ptr<hive_read_command> read_cmd, std::unique_ptr<reply> rep) 
    :_rep(std::move(rep)){
    _is = make_volume_input_stream(std::move(read_cmd));
}

future<temporary_buffer<char>> volume_data_source::get() {
    if(_need_length <= 0) {
        return make_ready_future<temporary_buffer<char>>();
    }
    logger.debug("[{}] start, _need_length:{}, _read_cmd:{}", __func__, _need_length, *_read_cmd);

    auto shard = hive::get_local_stream_service().shard_of(_read_cmd->extent_group_id);
    return hive::get_stream_service().invoke_on(shard, 
            [this] (auto& shard_stream_service){
        auto volume_id = this->_read_cmd->owner_id;
        return shard_stream_service.find_or_create_stream(volume_id).then(
                [this](auto volume_stream){
            return volume_stream->read_volume(this->_read_cmd);
        });
    }).then_wrapped([this](auto f){
        try{
            auto result = f.get0(); 
            extent_datum datum = extent_datum::from_raw_result(*result);
            auto data_begin = datum.data.begin();
            auto data_size  = datum.data.size();
            temporary_buffer<char> buf(data_size);
            std::copy_n(data_begin, data_size, buf.get_write()); //tododl:yellow can not use copy??
            _need_length -= data_size;
            logger.debug("[get] done, read_data_size:{}, read_cmd:{}", data_size, *_read_cmd);

            auto disk_ids = datum.candidate_disk_ids;
            auto extent_group_id = _read_cmd->extent_group_id;
            auto size =  _read_cmd->length;
            auto options = _read_cmd->options;
            auto volume_id = _read_cmd->owner_id;

            auto& context_service = hive::get_local_context_service();
            return context_service.get_or_pull_volume_context(volume_id).then([
                    this, disk_ids, extent_group_id, size, options, buf = std::move(buf)](auto volume_context)mutable{
                sstring node_id =  volume_context.get_driver_node().get_id();
                if("empty" == options){
                    options = volume_context.get_container_name();
                }
                this->trace_access_trail(disk_ids, extent_group_id, node_id, size, options);
                return make_ready_future<temporary_buffer<char>>(std::move(buf));
            });  
        }catch(...){
            std::ostringstream out;
            out << "[get] error, exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<temporary_buffer<char>>(std::runtime_error(error_info));
        }
    });
}


future<> 
volume_data_source::trace_access_trail(sstring disk_ids, sstring extent_group_id, sstring node_id, uint64_t size, sstring options){
    auto& trail_service = hive::get_local_trail_service();
    auto trail_params = make_shared<access_trail_params>(disk_ids, extent_group_id, node_id, size, options, access_trail_type::READ);
    auto access_trail = trail_service.get_trail(vega_trail_type::ACCESS_TRAIL);
    access_trail->trace(trail_params);
    return make_ready_future<>();
}

}//namespace hive
