#include "hive/stream_writer.hh"
#include "hive/stream_reader.hh"
#include "hive/stream_service.hh"
#include "hive/volume_service.hh"
#include "hive/extent_datum.hh"
#include "hive/extent_store.hh"
#include "hive/hive_config.hh"
#include "hive/hive_tools.hh"
#include "hive/hive_request.hh"
#include "hive/journal/journal_service.hh"
#include "hive/commitlog/commitlog_replayer.hh"
#include "hive/context/context_service.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/hive_plan.hh"
#include "hive/trail/trail_service.hh"

#include "unimplemented.hh"
#include "core/future-util.hh"
#include "exceptions/exceptions.hh"
#include "locator/snitch_base.hh"
#include "log.hh"
#include "to_string.hh"
#include <seastar/core/thread.hh>
#include <sstream>
#include <algorithm>
#include "unimplemented.hh"
#include <sys/time.h>
#include "core/distributed.hh"
#include "core/seastar.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/do_with.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/enum.hh>
#include <seastar/net/tls.hh>
#include <boost/range/algorithm/find.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "atomic_cell.hh"
#include "bytes.hh"
#include <chrono>
#include <functional>
#include <cstdint>

using namespace std::chrono_literals;
namespace hive{
static logging::logger logger("stream_writer");

static inline bool is_me(gms::inet_address from){
    return from == utils::fb_utilities::get_broadcast_address();
}
    
stream_writer::stream_writer(sstring volume_id, hive_write_command write_cmd)
    :_volume_id(volume_id)
    ,_write_cmd(write_cmd)
{
}

stream_writer::~stream_writer(){
}

/////////////////////
//for rwrite_volume
////////////////////
future<> stream_writer::commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks){
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id, vclocks] (auto& shard_volume_service){
        return shard_volume_service.commit_write_plan(volume_id, vclocks);
	});
}

future<std::vector<volume_revision>> stream_writer::build_volume_revisions(
    hive_write_command _write_cmd
    ,std::vector<rw_split_item> split_items){

    std::vector<volume_revision> revisions;
    for(auto item : split_items) {
      volume_revision revision;
      revision.owner_id = _write_cmd.owner_id;
      revision.offset_in_volume = item.extent_offset_in_volume + item.data_offset_in_extent;
      revision.extent_id = item.extent_id;
      revision.offset_in_extent = item.data_offset_in_extent;
      revision.length = item.length;

      //TODO bytes_view?
      bytes data(item.length, 0);
      std::memcpy(data.begin(), _write_cmd.data.begin()+item.data_offset, item.length);
      revision.data = std::move(data);
      revisions.push_back(std::move(revision));
    }
    return make_ready_future<std::vector<volume_revision>>(std::move(revisions));
}

future<uint64_t> stream_writer::get_vclock(sstring volume_id, uint64_t count){
    auto& volume_service = hive::get_local_volume_service();
    return volume_service.get_vclock(volume_id, count);
}

future<uint64_t> 
stream_writer::attach_vclock_to_volume_revision(sstring volume_id, std::vector<volume_revision>& revisions){ 
    auto count = revisions.size();
    return get_vclock(volume_id, count).then([volume_id, &revisions](auto vclock)mutable{
        for(auto& revision : revisions){
            revision.vclock = vclock++; 
        }
        return make_ready_future<uint64_t>(--vclock); //max vclock in revisions
    });
}

future<> stream_writer::do_rwrite_volume(sstring volume_id, volume_revision revision){
    auto extent_id = revision.extent_id;
    auto shard = get_local_journal_service().shard_of(extent_id);
    return hive::get_journal_service().invoke_on(shard, [revision=std::move(revision)]
            (auto& journal_service)mutable{
        auto primary_journal = journal_service.get_primary_journal(revision.owner_id);
        return primary_journal->write_journal(std::move(revision));
    });
}

future<> stream_writer::execute(){
    return seastar::async([this]()mutable{
        sstring volume_id = _write_cmd.owner_id;
        uint64_t offset = _write_cmd.offset;
        uint64_t length = _write_cmd.length;
        io_planner   planner;
        auto items = planner.split_data(volume_id, offset, length);

        //1. cut data to volume_revisions
        auto revisions = build_volume_revisions(std::move(_write_cmd), items).get0();

        //2. attach vclock
        //auto max_vclock = attach_vclock_to_volume_revision(volume_id, revisions).get0();
        attach_vclock_to_volume_revision(volume_id, revisions).get0();

        //3. write to journal 
        for(auto& revision : revisions){
            do_rwrite_volume(volume_id, std::move(revision)).get(); 
        }

        //4. commit write_plan
        //std::vector<int64_t> vclocks = {max_vclock};
        //commit_write_plan(volume_id, vclocks).get();
    }).then_wrapped([this](auto f)mutable{
        try {
            f.get(); 
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[rwrite_volume] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

}//namespace hive



