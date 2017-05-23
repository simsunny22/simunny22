#include "hive/object/object_stream.hh"
#include "hive/object/object_service.hh"
#include "hive/hive_service.hh"
#include "hive/extent_datum.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/context/context_service.hh"
#include "hive/exceptions/exceptions.hh"



using namespace std::chrono_literals;
namespace hive{
static logging::logger logger("object_stream");
                
object_stream::object_stream(){}
object_stream::~object_stream(){}

static future<std::unordered_set<gms::inet_address>> get_targets(sstring disk_ids_str){
    auto disk_ids = hive_tools::split_to_set(disk_ids_str, ":");

    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_ids).then([](auto disk_contexts){
        std::unordered_set<gms::inet_address> targets;
        for(auto& context : disk_contexts) {
            sstring ip = context.get_ip(); 
            targets.insert(gms::inet_address(ip));
        }   
        return make_ready_future<std::unordered_set<gms::inet_address>>(std::move(targets));
    }); 
}

static bool is_all_disks_success(std::unordered_set<gms::inet_address>& targets
                         , std::unordered_set<gms::inet_address>& success){

    for(auto& addr : targets){   
        auto itor = success.find(addr);
        if(itor == success.end()){
            return false;
        }
    }
    return true;
}

/////////////////////
// for write_object
/////////////////////
future<write_plan> object_stream::get_write_plan(
    sstring object_id
    , uint64_t offset
    , uint64_t length) {
    
    auto& object_service = hive::get_local_object_service();
    return object_service.get_object_driver().then([object_id, offset, length](auto driver_ptr){
        return driver_ptr->get_write_plan(object_id, offset, length);
    });
}

future<> object_stream::execute_create_actions(sstring object_id, std::vector<create_action> create_actions){
    //TODO:red not implement;
    return seastar::async([this, object_id, create_actions=std::move(create_actions)]()mutable{
        //1. create extent group
        for(auto& create_action: create_actions){
            do_create_extent_group(object_id, create_action).get();
        }
        //2. commit
        commit_for_create_extent_group(object_id, create_actions).get();
    });
}

future<> object_stream::do_create_extent_group(sstring volume_id, create_action action){
    sstring extent_group_id = action.extent_group_id;
    sstring disk_ids_str =  action.disk_ids;
    
    return get_targets(disk_ids_str).then([this, extent_group_id, disk_ids_str]
            (auto targets)mutable{
        auto disk_ids = hive_tools::split_to_vector(disk_ids_str, ":");
        smd_create_extent_group smd_create(extent_group_id, disk_ids);
    
        auto& proxy = hive::get_local_extent_store_proxy();
        return proxy.create_extent_group(std::move(smd_create), std::move(targets)).then(
                [this, extent_group_id, targets](auto success)mutable{

            if(is_all_disks_success(targets, success)){
                return make_ready_future<>();
            }else {
                auto error_info = sprint("create extent_group file failed,extent_group_id:{}", extent_group_id);
                throw hive::create_file_exception(error_info);
            }   
        }); 
    }); 
}

future<> object_stream::commit_for_create_extent_group(sstring object_id, std::vector<create_action> create_actions) {
    auto& object_service = get_local_object_service();
    return object_service.get_object_driver().then([object_id=std::move(object_id), create_actions = std::move(create_actions)](auto driver_ptr){
        return driver_ptr->commit_create_group(object_id, create_actions);
    });

}

future<std::vector<extent_revision>> object_stream::build_extent_revisions(
    hive_write_command write_cmd
    , std::vector<write_action> actions){

    std::vector<extent_revision> revisions;
    for(auto action : actions) {
      extent_revision revision;
      revision.owner_id = write_cmd.owner_id;
      revision.extent_group_id = action.extent_group_id;
      revision.extent_id = action.extent_id;
      revision.extent_offset_in_group = action.extent_offset_in_group;
      revision.data_offset_in_extent = action.data_offset_in_extent;
      revision.length = action.length;
      revision.disk_ids = action.disk_ids;
      bytes data(action.length, 0);
      std::memcpy(data.begin(), write_cmd.data.begin() + action.data_offset, action.length);
      revision.data = std::move(data);
      revisions.push_back(std::move(revision));
    }
    return make_ready_future<std::vector<extent_revision>>(std::move(revisions));

}


//return: inet_address <-> disk_id
future<std::map<gms::inet_address, sstring>> 
object_stream::get_extent_group_targets(sstring disk_ids) {
    auto& context_service = hive::get_local_context_service();
    auto disk_ids_set = hive_tools::split_to_set(disk_ids, ":");

    return context_service.get_or_pull_disk_context(disk_ids_set).then(
            [](auto disk_contexts){
        std::map<gms::inet_address, sstring> targets;
        for(auto& disk_context : disk_contexts){
            auto disk_id = disk_context.get_id();
            auto disk_ip = disk_context.get_ip();
            targets.insert(std::make_pair(gms::inet_address(disk_ip), disk_id)); 
        }
        return make_ready_future<std::map<gms::inet_address, sstring>>(targets);
    });
}

std::vector<sstring> object_stream::get_disk_ids(std::map<gms::inet_address, sstring>& targets){
    std::vector<sstring> disk_ids;
    for(auto& target : targets){
        disk_ids.push_back(target.second); 
    }
    return std::move(disk_ids);
}

std::unordered_set<gms::inet_address> 
object_stream::get_disk_addrs(std::map<gms::inet_address, sstring>& targets){
    std::unordered_set<gms::inet_address> disk_addrs;
    for(auto& target : targets){
        disk_addrs.insert(target.first); 
    }
    return std::move(disk_addrs);
}

future<std::experimental::optional<hint_entry>> 
object_stream::do_write_extent_group_end(
    sstring object_id
    , sstring extent_group_id
    , std::map<gms::inet_address, sstring>& targets
    , std::unordered_set<gms::inet_address>& success ){

    std::set<sstring> success_disk_ids;
    std::set<sstring> failed_disk_ids;
    for(auto& target : targets){
        auto addr = target.first;
        auto itor = success.find(addr);
        if(itor != success.end()){
            success_disk_ids.insert(target.second);
        }else{
            failed_disk_ids.insert(target.second);
        }
    }

    auto min_success = hive::get_local_hive_service().get_hive_config()->min_success_when_record_hint();
    auto max_hint_count = hive::get_local_hive_service().get_hive_config()->max_hint_count();
    if( 0 == failed_disk_ids.size() ) {
        //success
        return make_ready_future<std::experimental::optional<hint_entry>>(std::experimental::nullopt);
    }else if(success_disk_ids.size() >= min_success && failed_disk_ids.size() <= max_hint_count) {
        //success but need write hint
        hint_entry hint(object_id, extent_group_id, failed_disk_ids);  
        return make_ready_future<std::experimental::optional<hint_entry>>(hint);
    }else {
        //error
        auto disk_addrs = get_disk_addrs(targets);
        std::ostringstream out;
        out << "do_write_extent_group_end failed";
        out << ", object_id:" << object_id;
        out << ", extent_group_id:" << extent_group_id;
        out << ", targets:" << hive_tools::format(disk_addrs);
        out << ", success:" << hive_tools::format(success);
        auto error_info = out.str();
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}


future<std::experimental::optional<hint_entry>> 
object_stream::do_write_object(extent_revision revision){
    auto disk_ids_str = revision.disk_ids;
    return this->get_extent_group_targets(disk_ids_str).then([this, revision=std::move(revision)]
            (auto targets)mutable{
        auto disk_ids_vt = this->get_disk_ids(targets);
        auto disk_addrs = this->get_disk_addrs(targets);
        auto object_id = revision.owner_id;
        auto extent_group_id = revision.extent_group_id;
        std::vector<extent_revision> revisions;
        revisions.push_back(std::move(revision));
        smd_rwrite_extent_group smd_rwrite(extent_group_id, disk_ids_vt, std::move(revisions));

        auto& extent_store_proxy = hive::get_local_extent_store_proxy();
        return extent_store_proxy.rwrite_extent_group(std::move(smd_rwrite), disk_addrs).then(
                [this, targets, object_id, extent_group_id](auto success)mutable{
           return this->do_write_extent_group_end(object_id, extent_group_id, targets, success);   
        });
    });
}

future<> object_stream::commit_for_write_object(std::vector<hint_entry> hints) {
    auto& object_service = get_local_object_service();
    return object_service.get_object_driver().then([hints = std::move(hints)](auto driver_ptr){
        return driver_ptr->commit_for_write_object(std::move(hints));
    });
}

future<uint64_t> object_stream::commit_write_object(sstring object_id, uint64_t offset, uint64_t length) {
    auto& object_service = get_local_object_service();
    return object_service.get_object_driver().then([object_id, offset, length](auto driver_ptr){
        return driver_ptr->commit_write_object(object_id, offset, length);
    });
}


future<> object_stream::remove_extent_group_context(hint_entry& hint){
    //if drain has hint we had better to remove the extent_group_context,
    //so ,the next drain will pull new context, maybe avoid write hint again.
    auto extent_group_id = hint.extent_group_id;
    auto& context_service = hive::get_local_context_service();
    return context_service.remove_on_every_shard(extent_group_id);
}

future<> object_stream::execute_write_actions(std::vector<extent_revision> revisions) {
    return seastar::async([this, revisions=std::move(revisions)]()mutable{
        //1. write revisions
        std::vector<hint_entry> hints;
        for(auto& revision : revisions){
            //1.1 do write 
            auto hint_opt = do_write_object(std::move(revision)).get0(); 
            if(hint_opt){
                remove_extent_group_context(hint_opt.value()).get(); 
                hints.push_back(hint_opt.value());
            }
        }
   
        //2. commit metadata
        commit_for_write_object(std::move(hints)).get();
    });
}

future<uint64_t> object_stream::write_object(hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    
    return seastar::async([this, write_cmd=std::move(write_cmd)]()mutable{
        sstring object_id = write_cmd.owner_id;
        uint64_t offset = write_cmd.offset;
        uint64_t length = write_cmd.length;

        //1.1 get write_plan 
        auto plan = get_write_plan(object_id, offset, length).get0();

        //1.2 create_extent_group (optional)
        execute_create_actions(object_id, plan.create_actions).get();

        //2. cut plan to extent_revision set 
        auto&& revisions = build_extent_revisions(std::move(write_cmd), plan.write_actions).get0();

        //3. do write 
        execute_write_actions(std::move(revisions)).get();

	//4. commit object size
	auto size = commit_write_object(object_id, offset, length).get0();
	return size;
    
    }).then_wrapped([this](auto f)mutable{
        try {
            auto size = f.get0(); 
            return make_ready_future<uint64_t>(size);
        }catch(...){
            std::ostringstream out;
            out << "[write_object] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<uint64_t>(std::runtime_error(error_info));
        }
    });
}

/////////////////////
// for read_object
/////////////////////
future<read_plan> object_stream::get_read_plan(
    sstring object_id
    , uint64_t offset
    , uint64_t length) {

    auto& object_service = hive::get_local_object_service();
    return object_service.get_object_driver().then([object_id, offset, length](auto driver_ptr){
        return driver_ptr->get_read_plan(object_id, offset, length);
    });
}

future<std::vector<hive_read_subcommand>>
object_stream::build_read_subcommand(sstring volume_id, read_plan plan){
    std::vector<hive_read_subcommand> sub_cmds;
    for(auto action : plan.read_actions) {
      hive_read_subcommand read_cmd(
          plan.owner_id,
          action.extent_group_id,
          action.extent_id,
          action.extent_offset_in_group,
          action.data_offset_in_extent,
          action.length,
          action.disk_ids,
          action.md5,
          "");

      sub_cmds.push_back(std::move(read_cmd));
    }   

    return make_ready_future<std::vector<hive_read_subcommand>>(std::move(sub_cmds));
}


future<extent_datum> 
object_stream::load_extent_store_without_cache(hive_read_subcommand read_subcmd){
    logger.debug("[{}] start, read_subcmd:{}", __func__, read_subcmd);
    auto& extent_store_proxy = hive::get_local_extent_store_proxy();
    auto read_fut = extent_store_proxy.read_extent_group(read_subcmd, true);
    return std::move(read_fut).then([this, read_subcmd](auto&& result){
        extent_datum datum(
            read_subcmd.extent_id
            , read_subcmd.extent_group_id
            , read_subcmd.extent_offset_in_group
            , read_subcmd.disk_ids         
            , result->hit_disk_id
            , read_subcmd.md5         
            , std::move(result->data)
        );

        logger.debug("[load_extent_store_without_cache] done, read_subcmd:{}", read_subcmd);
        return make_ready_future<extent_datum>(std::move(datum));
    });
}

future<extent_datum> 
object_stream::load_extent_store_data(hive_read_subcommand read_subcmd){
    if(read_subcmd.extent_group_id == "undefined") {
        bytes empty_buffer(bytes::initialized_later(), read_subcmd.length); 
	      std::memset(empty_buffer.begin(), 0, read_subcmd.length);
        hive::extent_datum datum(
              read_subcmd.extent_id
            , read_subcmd.extent_group_id
            , read_subcmd.extent_offset_in_group
            , ""
            , ""
            , ""
            , std::move(empty_buffer)
        );

        return make_ready_future<extent_datum>(std::move(datum));
    }

    return load_extent_store_without_cache(read_subcmd);    
}
future<std::tuple<uint64_t, bytes>> 
object_stream::do_read_object(uint64_t order_id, hive_read_subcommand read_subcmd){
    return load_extent_store_data(read_subcmd).then([order_id](extent_datum&& datum){
        std::tuple<uint64_t, bytes> data = std::make_tuple(order_id, std::move(datum.data));
        return make_ready_future<std::tuple<uint64_t, bytes>>(std::move(data)); 
    });
}

future<bytes> object_stream::execute_subcommads(
    uint64_t total_length
    , std::vector<hive_read_subcommand> read_subcmds){
   
    std::vector<future<std::tuple<uint64_t, bytes>>> futures;
    uint64_t order_id = 0;
    for(auto read_subcmd : read_subcmds){
        auto fut = do_read_object(order_id++, read_subcmd); 
        futures.push_back(std::move(fut));
    }

    return when_all(futures.begin(), futures.end()).then([total_length](auto futs){
        std::vector<std::tuple<uint64_t, bytes>> datas;
        for(auto& fut : futs) {
            auto&& data = fut.get0();  
            datas.push_back(std::move(data));
        }

        //splice data in order
        std::sort(datas.begin(), datas.end(),[&](auto& x, auto& y){
            auto x_order_id = std::get<0>(x);
            auto y_order_id = std::get<0>(y);
            return x_order_id < y_order_id;
        });

        bytes buf(bytes::initialized_later(), total_length);
        uint64_t offset = 0;
        for(auto& data : datas) {
            bytes& content = std::get<1>(data);
            std::memcpy(buf.begin()+offset, content.begin(), content.length());
            offset += content.length();
        }
        assert(offset == total_length);
        return make_ready_future<bytes>(std::move(buf));
    });
}

future<bytes> object_stream::read_object(hive_read_command read_cmd){
    return seastar::async([this, read_cmd=std::move(read_cmd)]()mutable{
        auto object_id = read_cmd.owner_id;
        auto offset = read_cmd.offset;
        auto length = read_cmd.length;

        //1. get read_plan
        auto&& plan = get_read_plan(object_id, offset, length).get0();

        //2. cut read_plan to multiple hive_read_subcommand
        auto&& sub_cmds = build_read_subcommand(object_id, plan).get0();

        //3. execute all hive_read_subcommand
        auto&& data = execute_subcommads(length, std::move(sub_cmds)).get0();
        return std::move(data);
    }).then_wrapped([this](auto fut) mutable{
        try{
            auto&& data = fut.get0();
            return make_ready_future<bytes>(std::move(data));
        }catch (...) {
            std::ostringstream out;
            out << "[read_object] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str()); 
            throw std::runtime_error(error_info);
        }
    });
}

}//namespace hive
