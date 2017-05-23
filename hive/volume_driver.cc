
#include "hive/volume_driver.hh"
#include "hive/stream_service.hh"
#include "hive/context/context_service.hh"
#include "hive/context/volume_context_entry.hh"
#include "hive/guru/guru_kit.hh"
#include "hive/hive_service.hh"
#include "hive/metadata_service/metadata_service.hh"
#include "hive/metadata_service/entity/extent_map_entity.hh"

namespace hive{

static logging::logger logger("volume_driver");

uint64_t g_allocate_latency_0 = 0;
uint64_t g_allocate_latency_1 = 0;
uint64_t g_allocate_latency_2 = 0;
uint64_t g_allocate_latency_3 = 0;
uint64_t g_allocate_times_0 = 0;
uint64_t g_allocate_times_1 = 0;
uint64_t g_allocate_times_2 = 0;
uint64_t g_allocate_times_3 = 0;

volume_driver::volume_driver(sstring volume_id)
    :_volume_id(volume_id)
{
    _write_count = 0;
}

volume_driver::~volume_driver(){
}

future<> volume_driver::init() {
    guru_kit guru;
    auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
    return guru.get_volume_entity(metadata_url, _volume_id).then([this](auto volume_entity){
        this->_volume_entity = volume_entity;
        this->_vclock = volume_entity._vclock;
        return this->start_volume_stream();
    }).then([this](){
        return this->load_volume_metadata();
    });
}

future<> volume_driver::start_volume_stream(){
    logger.debug("[{}] start, volume_id:{}", __func__,  _volume_id);
    return init_volume_context().then([this](auto volume_context)mutable{
        _vclock = volume_context.get_vclock();
        return hive::get_stream_service().invoke_on_all([this] (stream_service& stream_service){
            return stream_service.start_volume_stream(this->_volume_id);
        });
    }).then_wrapped([this](auto f){
        try{
            f.get(); 
            logger.debug("[start_volume_stream] done, volume_id:{}", _volume_id);
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[start_volume_stream] error, volume_id:" << _volume_id;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

future<> volume_driver::rebuild_volume_stream(){
    return init_volume_context().then([this](auto volume_context)mutable{
        _vclock = volume_context.get_vclock();
        return hive::get_stream_service().invoke_on_all([this] (stream_service& stream_service){
            return stream_service.rebuild_volume_stream(this->_volume_id, this->_vclock);
        });
    }).then_wrapped([this](auto f){
        try{
            f.get();
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[volume_driver rebuild_volume_stream] error";
            out << ", volume_id:" << _volume_id; 
            out << ", excption:"  << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });   
}

future<> volume_driver::stop_volume_stream(){
    return hive::get_stream_service().invoke_on_all([this] (stream_service& stream_service){
        return stream_service.stop_volume_stream(this->_volume_id);
    });
}

future<hive::volume_context_entry> volume_driver::init_volume_context(){
    std::set<sstring> volume_ids;
    volume_ids.insert(_volume_id);
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_volume_context(_volume_id);
}


sstring volume_driver::get_extent_group_id_from_cache(sstring extent_id) {
    //1. get from cache
    return _cache[extent_id];
}

future<sstring> volume_driver::get_extent_group_id(sstring container_name, sstring volume_id, sstring extent_id) {
    //1. get from cache
    auto it = _cache.find(extent_id);
    if (it != _cache.end()){
        sstring id = _cache[extent_id];
        return make_ready_future<sstring>(id);
    }else{
        auto& metadata_service = hive::get_local_metadata_service();
        //2. get from db
        sstring id = metadata_service.get_extent_group_id(container_name, volume_id, extent_id).get0();
        //3. set cache
        _cache[extent_id] = id;
        return make_ready_future<sstring>(id);
    }
}



//future<write_plan> volume_driver::get_write_plan(sstring volume_id, uint64_t offset, uint64_t length){
//    logger.debug("[get_write_plan] start, volume_id:{}", volume_id);
//return with_lock(_driver_lock, [this, volume_id, offset, length](){
//    return seastar::async([this, volume_id, offset, length]()mutable{
//        write_plan plan;
//
//    logger.debug("[get_write_plan] start, volume_id:{} write_count:{}", volume_id, _write_count);
//
//        _write_count++;
//        plan.owner_id = volume_id;
//        sstring container_name = _volume_entity._container_name;
//        sstring container_uuid = _volume_entity._container_uuid;
//        auto& metadata_service = hive::get_local_metadata_service();
//        auto items  = _planner.split_data(volume_id, offset, length);
//        std::vector<create_action> c_actions;
//        std::vector<write_action>  w_actions;
//
//        for(auto& item: items) {
//           // 1、find extent_group_id for extent_id
//           sstring extent_group_id = get_extent_group_id(container_name, volume_id, item.extent_id).get0();
//           sstring last_group_id = _volume_entity._last_extent_group_id;
//
//           write_action w_action;
//           w_action.extent_id = item.extent_id;
//           w_action.data_offset_in_extent = item.data_offset_in_extent;
//           w_action.data_offset = item.data_offset;
//           w_action.length = item.length;
//
//           create_action c_action;
//          //need new extent group
//          if (extent_group_id == "undefined") {
//              //1. check volume last group full
//              auto current_pos = metadata_service.get_extents_num(container_name, last_group_id).get0();
//    logger.debug("[get_write_plan] las_extent_group , last_extent_group_id:{} current_pos:{} extent_id:{}", last_group_id, current_pos, item.extent_id);
//              if (last_group_id == "" || current_pos == (hive::extent_size*hive::extent_num_in_group)) {
//                  auto uuid = utils::make_random_uuid();
//                  auto random_string = to_sstring(hive_tools::random(104857600));
//                  sstring uuid_str = uuid.to_sstring()+random_string;
//                  w_action.extent_group_id = uuid_str;
//
//                  guru_kit guru;
//                  auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
//                  auto disks = guru.allocate_write_disks(metadata_url, container_uuid).get0();
//                  w_action.disk_ids = hive_tools::join(disks, ":");
//                  w_action.extent_offset_in_group = 0;
//
//                  c_action.extent_group_id = w_action.extent_group_id;
//                  c_action.disk_ids = w_action.disk_ids;
//                  c_actions.push_back(c_action);
//
//                  // create extent group to db
//                  extent_group_entity entity(c_action.extent_group_id, c_action.disk_ids, volume_id, 0);
//    logger.debug("[get_write_plan] create_extent_group , volume_id:{} c_action.extent_group_id:{} c_action.disk_ids:{}", volume_id, c_action.extent_group_id, c_action.disk_ids);
//                  metadata_service.create_extent_group(container_name, entity).get0();
//
//
//                  // add extent to group
//                  extent_entity extent;
//                  extent._extent_id = w_action.extent_id;
//                  extent._offset    = w_action.extent_offset_in_group;
//                  extent._ref_count = 1;
//    logger.debug("[get_write_plan] add_extent_to_group extent_group_id:{}, volume_id:{} w_action.disk_ids:{}", w_action.extent_group_id, volume_id, w_action.disk_ids);
//                  metadata_service.add_extent_to_group(container_name, w_action.extent_group_id, w_action.disk_ids, extent).get0();
//
//                  // add volume map
//                  metadata_service.create_volume_map(container_name, volume_id, w_action.extent_id, w_action.extent_group_id).get0();
//
//                  // set cache
//                  _cache[w_action.extent_id] = w_action.extent_group_id;
//
//                  //update volume last group
//                  _volume_entity._last_extent_group_id = w_action.extent_group_id;
//                  //TODO: update to db
//                  metadata_service.update_volume_last_extent_group(volume_id, w_action.extent_group_id);
//              }else{
//                  //get last extent group extents
//                  w_action.extent_group_id = last_group_id;
//                  w_action.extent_offset_in_group = current_pos;
//                  auto disks = metadata_service.get_extent_group_disks(container_name, w_action.extent_group_id).get0();
//
//                  w_action.disk_ids = hive_tools::join(disks, ":");
//
//                  // add extent to group
//                  extent_entity extent;
//                  extent._extent_id = w_action.extent_id;
//                  extent._offset    = w_action.extent_offset_in_group;
//                  extent._ref_count = 1;
//    logger.debug("[get_write_plan] add_extent_to_group 2 extent_group_id:{}, volume_id:{} w_action.disk_ids:{}, extent_id:{}", w_action.extent_group_id, volume_id, w_action.disk_ids, w_action.extent_id);
//                  metadata_service.add_extent_to_group(container_name, w_action.extent_group_id, w_action.disk_ids, extent).get0();
//
//                  metadata_service.create_volume_map(container_name, volume_id, w_action.extent_id, w_action.extent_group_id).get0();
//
//                  // set cache
//                  _cache[w_action.extent_id] = w_action.extent_group_id;
//              }
//          }else{
//              w_action.extent_group_id = extent_group_id;
//              w_action.extent_offset_in_group = metadata_service.get_extent_offset_in_group(container_name, extent_group_id, w_action.extent_id).get0();
//              auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
//              w_action.disk_ids = hive_tools::join(disks, ":");
//    logger.debug("[get_write_plan] find old extent_group_id extent_group_id:{}, volume_id:{} w_action.disk_ids:{} extent_id:{}", w_action.extent_group_id, volume_id, w_action.disk_ids, w_action.extent_id);
//          }
//
//          w_actions.push_back(w_action);
//        }
//
//        plan.write_actions  = w_actions;
//        plan.create_actions = c_actions;
//
//        return std::move(plan);
//    }).then_wrapped([](auto f)mutable{
//        try {
//            auto&& plan = f.get0();
//            return make_ready_future<write_plan>(std::move(plan));
//        }catch (...) {
//            throw std::runtime_error("test"); 
//        }
//    });
//});
//}

future<> volume_driver::commit_write_plan(sstring volume_id, std::vector<uint64_t> vclocks){
    uint64_t max_vclock = 0;
    for(auto i : vclocks) {
      if(i>max_vclock){
        max_vclock = i;
      }
    }
    
    auto& metadata_service = hive::get_local_metadata_service();
    return metadata_service.update_volume_vclock(volume_id, max_vclock);
}

future<> volume_driver::commit_create_group(std::vector<create_action> create_actions){
    std::vector<future<>> futures;
    for(auto& action: create_actions) {
      auto& metadata_service = hive::get_local_metadata_service();
      sstring container_name = _volume_entity._container_name;
      sstring extent_group_id = action.extent_group_id;
      sstring disk_ids = action.disk_ids;
      auto f = metadata_service.update_extent_group_created(container_name, extent_group_id, disk_ids, true);
      futures.push_back(std::move(f));
    }

    return when_all(futures.begin(), futures.end()).discard_result();
}

read_action volume_driver::make_read_plan_test(rw_split_item item){
    read_action action;

    auto config = hive::get_local_hive_service().get_hive_config();
    auto disk_ids = config->ctx_extent_group_target_disk_ids();

    auto strs = hive_tools::split_to_vector(item.extent_id, "#");
    sstring extent_num_str = strs[1];
    uint64_t extent_num =  hive_tools::str_to_uint64(extent_num_str);

    uint64_t extent_group_num = extent_num / 4 +1;
    uint64_t suffix_num = extent_group_num % 10;
    sstring  extent_group_suffix = "_" + to_sstring(suffix_num) + to_sstring(suffix_num);
    sstring extent_group_id = _volume_id + "_extent_group_" + to_sstring(extent_group_num) + extent_group_suffix;

    uint64_t extent_offset_in_group = extent_num % 4;

    action.extent_group_id = extent_group_id;
    action.extent_offset_in_group = extent_offset_in_group * 1024 * 1024;
    action.disk_ids = disk_ids;
    action.md5 = "";

    action.extent_id = item.extent_id;
    action.length    = item.length;
    action.data_offset_in_extent = item.data_offset_in_extent;
    return std::move(action);
}

future<write_plan> volume_driver::get_write_plan(sstring volume_id, uint64_t offset, uint64_t length, lw_shared_ptr<bytes> buffer){
    auto write_items = _planner.split_data(volume_id, offset, length);
    std::vector<read_action> actions;
    for(auto& item : write_items){
        write_action action;

        action.volume_id = volume_id;
        action.data_offset_in_volume = item.extent_offset_in_volume + item.data_offset_in_extent;

        action.extent_id = item.extent_id;
        action.data_offset_in_extent = item.data_offset_in_extent;

        action.buffer = buffer;
        action.offset_in_buffer = item.data_offset;
        action.length = item.length;

        actions.push_back(action);
    }
    write_plan plan;
    plan.owner_id = volume_id;
    plan.write_actions = actions;
    return std::move(plan);


}
future<read_plan> volume_driver::get_read_plan(sstring volume_id, uint64_t offset, uint64_t length){
    logger.debug("[{}] start volume_id:{}, offset:{} length:{}",__func__, volume_id, offset, length);
    return seastar::async([this, volume_id, offset, length]() mutable {
        auto read_items = _planner.split_data(volume_id, offset, length);
        std::vector<read_action> actions;
        for(auto& item: read_items) {
          // 1. new action
          read_action action;

           //wztest
          auto config = hive::get_local_hive_service().get_hive_config();
          if(config->auto_test_hive()){
               action = make_read_plan_test(item);
               actions.push_back(action);
               continue;
          }
          
          // 2. add params for read_journal
          action.extent_id = item.extent_id;
          action.length    = item.length;
          action.data_offset_in_extent = item.data_offset_in_extent;

          // 3. add params for read extent_group
          auto& metadata_service = hive::get_local_metadata_service();
          sstring container_name = _volume_entity._container_name;
          sstring extent_group_id = get_extent_group_id(container_name, volume_id, item.extent_id).get0();
          bool created = metadata_service.get_created(container_name, extent_group_id).get0();
          if (!created) { 
              //extent_group not created yet
              action.extent_group_id = "undefined";
              action.extent_offset_in_group = 0;
              action.disk_ids = "";
              action.md5 = "";
          }else{
              action.extent_group_id = extent_group_id;
              action.extent_offset_in_group = metadata_service.get_extent_offset_in_group(container_name, extent_group_id, action.extent_id).get0();
              auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
              action.disk_ids = hive_tools::join(disks, ":");
              action.md5 = "";
          }

          // 4. add action
          actions.push_back(action);
        }

        read_plan plan;
        plan.owner_id = volume_id;
        plan.read_actions = actions;
        return std::move(plan);
    }).then_wrapped([](auto f)mutable{
        try {
            auto&& plan = f.get0();
            return make_ready_future<read_plan>(std::move(plan));
        }catch (...) {
            throw std::runtime_error("get_read_plan error"); 
        }
    });
}
#if 0
//dltest1
static void print_allocate_latency(utils::latency_counter lc){
    auto latency = lc.stop().latency_in_nano();
    auto shard = engine().cpu_id();
    uint64_t loop = 1000;
    uint64_t average_latency;
    
    if( 0 == shard){
        g_allocate_latency_0 += latency;
        ++g_allocate_times_0;
        average_latency =  g_allocate_latency_0/g_allocate_times_0;
        if(g_allocate_times_0 % loop == 0)
            logger.error("allocate_extent_group, latency:{}, shard:{}", average_latency, shard);
    }else if( 1 == shard){
        g_allocate_latency_1 += latency;
        ++g_allocate_times_1;
        average_latency = g_allocate_latency_1/g_allocate_times_1;
        if(g_allocate_times_1 % loop == 0)
            logger.error("allocate_extent_group, latency:{}, shard:{}", average_latency, shard);
    }else if( 2 == shard && g_allocate_times_2 % loop == 0){
        g_allocate_latency_2 += latency;
        ++g_allocate_times_2;
        average_latency = g_allocate_latency_2/g_allocate_times_2;
        if(g_allocate_times_2 % loop == 0)
            logger.error("allocate_extent_group, latency:{}, shard:{}", average_latency, shard);
    }else if( 3 == shard && g_allocate_times_3 % loop == 0){
        g_allocate_latency_3 += latency;
        ++g_allocate_times_3;
        average_latency = g_allocate_latency_3/g_allocate_times_3;
        if(g_allocate_times_3 % loop == 0)
            logger.error("allocate_extent_group, latency:{}, shard:{}", average_latency, shard);
    }else {
       //do nothing 
    }
}
#endif

allocate_item volume_driver::allocate_from_exist_extent_group(sstring container_name, sstring volume_id, sstring extent_group_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();

    item.volume_id = volume_id;
    item.extent_id = extent_id;
    item.extent_group_id = extent_group_id;
    item.extent_offset_in_group = metadata_service.get_extent_offset_in_group(container_name, extent_group_id, extent_id).get0();
    auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
    item.need_create = !metadata_service.get_created(container_name, item.extent_group_id).get0();
    item.version  = metadata_service.get_version(container_name, item.extent_group_id).get0();
    item.disks = disks;//hive_tools::join(disks, ":");

    return std::move(item);
}

future<drain_plan> volume_driver::make_drain_plan_for_test(sstring disk_ids, std::vector<volume_revision_view> volume_revision_views ){
    std::unordered_map<sstring, drain_extent_group_task> extent_group_tasks;
    sstring volume_id = _volume_id;
    sstring container_name = _volume_entity._container_name;


    for(auto& revision: volume_revision_views){
        auto volume_id  = revision.volume_id;
        auto extent_id =   revision.extent_id;

        auto strs = hive_tools::split_to_vector(extent_id, "#");
        sstring extent_num_str = strs[1];
        uint64_t extent_num =  hive_tools::str_to_uint64(extent_num_str);

        uint64_t extent_group_num = extent_num / 4 +1;
        uint64_t suffix_num = extent_group_num % 10;
        sstring  extent_group_suffix = "_" + to_sstring(suffix_num) + to_sstring(suffix_num);
        sstring extent_group_id = volume_id + "_extent_group_" + to_sstring(extent_group_num) + extent_group_suffix;

        uint64_t extent_offset_in_group = extent_num % 4 * 1024*1024;


        // 1.1 build extent_group_revision from volume_revision
        extent_group_revision group_revision;
        group_revision.vclock = revision.vclock; 
        group_revision.offset_in_journal_segment = revision.offset_in_journal_segment;
        group_revision.offset_in_extent_group = extent_offset_in_group + revision.offset_in_extent;
        group_revision.length = revision.length;

        drain_extent_group_task task;
        task.volume_id = revision.volume_id;
        task.container_name = container_name;
        task.journal_segment_id= revision.journal_segment_id;
        task.extent_group_id = extent_group_id;
        task.version= 0;
        task.replica_disk_ids = disk_ids;
        task.need_create_extent_group = false;
        
        auto it = extent_group_tasks.find(extent_group_id);

        if(it != extent_group_tasks.end()){
            task.extents = it->second.extents;
            task.extent_group_revisions = it->second.extent_group_revisions;
        }

        task.extents.push_back(revision.extent_id);
        task.extent_group_revisions.push_back(group_revision);
        extent_group_tasks.insert(std::make_pair(extent_group_id, task));
    }
    auto drain_tasks = make_drain_tasks(volume_id, extent_group_tasks);

    logger.debug("[{}] make_drain_plan done1 drain_tasks.size:{} extent_group_tasks.size:{}", __func__,  drain_tasks.size(), extent_group_tasks.size());
    drain_plan plan("drain", std::move(drain_tasks));
    //return std::move(plan);
    return make_ready_future<drain_plan>(std::move(plan));

}
allocate_item volume_driver::allocate_from_new_extent_group(sstring container_name, sstring container_uuid, sstring volume_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();

    //new extent group
    auto uuid = utils::make_random_uuid();
    sstring extent_group_id = uuid.to_sstring();//+random_string;

    guru_kit guru;
    auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
    auto disks = guru.allocate_write_disks(metadata_url, container_uuid).get0();
    
    // create extent group to db
    auto disks_string  = hive_tools::join(disks, ":");
    extent_group_entity entity(extent_group_id, disks_string, volume_id, 0);
    metadata_service.create_extent_group(container_name, entity).get0();

    item.extent_group_id = extent_group_id;
    item.volume_id = volume_id;
    item.extent_id = extent_id;
    item.disks = disks;
    item.extent_offset_in_group = 0;
    item.version = 0;

    // set create flag;
    item.need_create = true;
    
    // add extent to group
    extent_entity extent;
    extent._extent_id = extent_id;
    extent._offset    = item.extent_offset_in_group;
    extent._ref_count = 1;

    metadata_service.add_extent_to_group(container_name, extent_group_id, disks_string, extent).get0();

    // add volume map
    metadata_service.create_volume_map(container_name, volume_id, extent_id, extent_group_id).get0();

    // set cache
    _cache[extent_id] = extent_group_id;

    //update volume last group
    _volume_entity._last_extent_group_id = extent_group_id;
    //update to db
    metadata_service.update_volume_last_extent_group(volume_id, extent_group_id).get();

    // insert storage info to storage_map
    metadata_service.create_storage_map(disks, extent_group_id, container_name).get();
   
    return std::move(item);
}

allocate_item volume_driver::allocate_from_last_extent_group(sstring container_name, sstring volume_id, sstring extent_group_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();
    auto extents_num = metadata_service.get_extents_num(container_name, extent_group_id).get0();

    //get last extent group extents
    auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
    item.extent_group_id = extent_group_id;
    item.extent_offset_in_group = extents_num*hive::extent_size;
    item.need_create = !metadata_service.get_created(container_name, item.extent_group_id).get0();
    item.version  = metadata_service.get_version(container_name, item.extent_group_id).get0();
    item.disks = disks;//hive_tools::join(disks, ":");

    auto disks_string  = hive_tools::join(disks, ":");
    // add extent to group
    extent_entity extent;
    extent._extent_id = extent_id;
    extent._offset    = item.extent_offset_in_group;
    extent._ref_count = 1;

    metadata_service.add_extent_to_group(container_name, extent_group_id, disks_string, extent).get0();

    metadata_service.create_volume_map(container_name, volume_id, extent_id, extent_group_id).get0();

    // set cache
    _cache[extent_id] = extent_group_id;
    return std::move(item);
}

future<allocate_item> volume_driver::allocate_extent_group(sstring volume_id, sstring extent_id){
utils::latency_counter lc_allocate;
lc_allocate.start();

return with_lock(_driver_lock, [this, volume_id, extent_id](){
    return seastar::async([this, volume_id, extent_id]()mutable{
        allocate_item item;
        sstring container_name = _volume_entity._container_name;
        sstring container_uuid = _volume_entity._container_uuid;
        auto& metadata_service = hive::get_local_metadata_service();
        // 1. find extent_group_id for extent_id
        sstring extent_group_id = get_extent_group_id(container_name, volume_id, extent_id).get0();
        sstring last_group_id = _volume_entity._last_extent_group_id;

        //need new extent group
        if (extent_group_id == "undefined") {
            //1. check volume last group full
            auto extents_num = metadata_service.get_extents_num(container_name, last_group_id).get0();
            if (last_group_id == "" || extents_num == hive::extent_num_in_group) {
                item = allocate_from_new_extent_group(container_name, container_uuid, volume_id, extent_id);
            }else{
                item = allocate_from_last_extent_group(container_name, volume_id, last_group_id, extent_id);
           }
        }else{
            item = allocate_from_exist_extent_group(container_name, volume_id, extent_group_id, extent_id);
        }

        logger.debug("[{}] done item.extent_group_id:{} item.disks:{}, item.version:{}", __func__, item.extent_group_id, item.disks, item.version);
        return std::move(item);
    }).then_wrapped([](auto f)mutable{
        try {
            auto&& item = f.get0();
            return make_ready_future<allocate_item>(std::move(item));
        }catch (...) {
            throw std::runtime_error("allocate_extent_group error"); 
        }
    });
}).finally([lc_allocate]()mutable{
  //  print_allocate_latency(lc_allocate);
});
}


//test drain plan start
future<> volume_driver::test_drain_plan(){
    sstring volume_id = "6e8facedbf6afe43ed8bae5b9b039b8f";
    std::vector<volume_revision_view> revisions;
    for (int i=0; i<10;i++){
        int rand = hive_tools::random(50000);
        sstring extent_id = "vol1#" + to_sstring(rand);
        volume_revision_view revision;
        revision.volume_id = volume_id;
        revision.extent_id = extent_id;
        revision.journal_segment_id = "sgement123";
        revision.offset_in_extent = 23;
        revision.offset_in_volume = i * 1024 * 1024;
        revision.offset_in_journal_segment = 45;
        revision.length = 56;
        revision.vclock = 78;
  
  logger.debug("test_drain_plan revision:{}", revision);
        revisions.push_back(revision);
    }

    return make_drain_plan(revisions).then([](auto r){
        return make_ready_future<>(); 
    });
}
//test drain plan end 

future<> volume_driver::load_volume_metadata(){
  sstring container_name = _volume_entity._container_name;
  sstring volume_id = _volume_id;

  auto& metadata_service = hive::get_local_metadata_service();
  return metadata_service.get_volume_metadata(container_name, volume_id).then([this, container_name, volume_id](auto volume_entities)mutable{
      for(auto volume_entity : volume_entities){
          sstring extent_id = volume_id + "#" + to_sstring(volume_entity._block_id);
          this->_cache[extent_id] = volume_entity._extent_group_id;
          logger.debug("[{}] volume_id:{},extent_id:{},extent_group_id:{}", __func__, volume_id, extent_id, volume_entity._extent_group_id);
      }
      return make_ready_future<>();
  }); 
}

// memo:
// 1. seperate functional and processing codes
// 2. data structure naming
// 3. side effect controlling
future<drain_plan> volume_driver::make_drain_plan(std::vector<volume_revision_view> volume_revision_views){
utils::latency_counter lc_make_plan;
lc_make_plan.start();
    logger.debug("[{}] start volume_id:{}, revisions.size:{}", __func__,  _volume_id, volume_revision_views.size());

    auto config = hive::get_local_hive_service().get_hive_config();
    if(config->auto_test_hive()){
       logger.debug("wztest ........... make_drain_plan _for test");
       auto disk_ids = config->ctx_extent_group_target_disk_ids();
       return make_drain_plan_for_test(disk_ids, std::move(volume_revision_views));
    }


    return seastar::async([this, volume_revision_views, lc_make_plan]() mutable{
        std::unordered_map<sstring, drain_extent_group_task> extent_group_tasks;
        sstring volume_id = _volume_id;
        sstring container_name = _volume_entity._container_name;
        std::unordered_map<sstring, allocate_item> items_map;

utils::latency_counter lc1;
lc1.start();
      	//1. iterate volume_revisions, and build extent_group_tasks accordingly 
        for(auto& revision : volume_revision_views){
            auto extent_id = revision.extent_id;
            allocate_item item;
            auto itor = items_map.find(extent_id);
            if(itor != items_map.end()){
                item = itor->second; 
            }else{
                item = allocate_extent_group(volume_id, extent_id).get0();
                items_map.insert(std::make_pair(extent_id, item));
            }

            auto disks_string = hive_tools::join(item.disks, ":");
            // 1.1 build extent_group_revision from volume_revision
            extent_group_revision group_revision;
            group_revision.vclock = revision.vclock;
            group_revision.offset_in_journal_segment = revision.offset_in_journal_segment;
            group_revision.offset_in_extent_group = item.extent_offset_in_group+ revision.offset_in_extent;
            group_revision.length = revision.length;

            drain_extent_group_task task;
            task.volume_id = volume_id;
	    task.container_name = container_name;
	    task.journal_segment_id= revision.journal_segment_id;
	    task.extent_group_id = item.extent_group_id;
	    task.version= item.version;
	    task.replica_disk_ids = disks_string;

            // 1.2 add extent_group_revision to a extent_group_task, and the task should be created if not found.
	    auto it = extent_group_tasks.find(item.extent_group_id);
	    if (it != extent_group_tasks.end()){
                task.extents = it->second.extents;
		task.extent_group_revisions = it->second.extent_group_revisions;
	    }

	    if (item.need_create == true) {
	        task.need_create_extent_group = item.need_create;
	    }

	    task.extents.push_back(extent_id);
	    task.extent_group_revisions.push_back(group_revision);

	    extent_group_tasks[item.extent_group_id] = task;
        }

logger.debug("[make_drain_plan] volume_id:{} latency:{} ns", _volume_id, lc1.stop().latency_in_nano());
        auto drain_tasks = make_drain_tasks(volume_id, extent_group_tasks);

        logger.debug("[make_drain_plan] done drain_tasks.size:{} extent_group_tasks.size:{}", drain_tasks.size(), extent_group_tasks.size());

        drain_plan plan("drain", std::move(drain_tasks));
        return std::move(plan);
    }).then_wrapped([this, lc_make_plan](auto fut)mutable{
        try {
            auto plan = fut.get();
            logger.debug("[make_drain_plan] volume_id:{} done, latency:{} ns", _volume_id, lc_make_plan.stop().latency_in_nano());
            return make_ready_future<drain_plan>(std::move(plan));
        } catch (...) {
            std::ostringstream out;
            out << "[volume_driver] make_drain_plan failed";
            out << " , volume_id " << _volume_id;
            out << " , error_info:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<drain_plan>(std::runtime_error(error_info));
        }
    });
}

std::vector<drain_extent_group_task> volume_driver::make_drain_tasks(sstring volume_id, std::unordered_map<sstring, drain_extent_group_task> extent_group_tasks) {
    // get journal nodes
    auto& context_service = hive::get_local_context_service();
    std::vector<sstring> journal_nodes;
    auto volume_context_entry = context_service.get_volume_context(volume_id);
    auto second_journal_nodes = volume_context_entry.get_journal_nodes();
    for(auto journal_node: second_journal_nodes){
        journal_nodes.push_back(journal_node.get_ip());
    }

    auto driver_node_ip = volume_context_entry.get_driver_node().get_ip();
    journal_nodes.push_back(driver_node_ip);

    // make drain tasks
    std::vector<drain_extent_group_task> drain_tasks;
    for(auto& it : extent_group_tasks){
        sstring extent_group_id = it.first;
        auto task = it.second;
        auto disks_string = task.replica_disk_ids;
        auto disks = hive_tools::split_to_vector(disks_string, ":");
        for(auto& disk_id : disks){
            auto disk_context = context_service.get_or_pull_disk_context(disk_id).get0();
            auto node_ip = disk_context.get_ip();
            auto peer_task= task;
            peer_task.disk_id = disk_id;
            peer_task.node_ip = node_ip;
            peer_task.journal_nodes = journal_nodes;
            drain_tasks.push_back(peer_task);
        }
    }
    return std::move(drain_tasks);
}

//future<std::vector<allocate_item>> volume_driver::allocate_extent_groups(std::vector<volume_revision_view> revision_views)){
//    sstring volume_id = revision_views[0].volume_id;
//    logger.debug("[{}] volume_id:{}",  __func__, volume_id);
//    std::vector<future<allocate_item>> futs;
//    for(auto revision_view : revision_views){
//        auto fut = allocate_extent_group(volume_id, revision_view.extent_id);
//        futs.push_back(std::move(fut));
//    }
//
//    return when_all(futs.begin(), futs.end()).then([](auto futs){
//        std::vector<allocate_item> items;
//        for(auto& fut: futs){
//            auto item = fut.get0();
//            items.push_back(std::move(item));
//        }
//
//    });
//}


future<drain_plan> volume_driver::rescue_drain_plan(std::vector<drain_extent_group_task> tasks){
    return seastar::async([this, tasks](){
        auto& context_service = hive::get_local_context_service();
        std::vector<drain_extent_group_task> new_tasks;
        auto& metadata_service = hive::get_local_metadata_service();
        //1. reallocate disk for failed extent group
        for(auto& task : tasks) {
            sstring extent_group_id = task.extent_group_id;
            sstring disk_id = task.disk_id;
            sstring replica_disk_ids = task.replica_disk_ids;
            auto disks = hive_tools::split_to_vector(replica_disk_ids, ":");
            //1.1 reallocate disk for extent group id
            guru_kit guru;
            auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
            auto new_disk_id  = guru.allocate_disk_for_extent_group(metadata_url, task.volume_id, extent_group_id, disks, disk_id).get0();

            auto new_disks_   = hive_tools::filter(disks, disk_id); 
            new_disks_.push_back(new_disk_id);

            auto new_disks = hive_tools::deduplicate(new_disks_);
            //1.2 make new task
            drain_extent_group_task new_task = task;
            new_task.disk_id = new_disk_id;
            auto disk_context = context_service.get_or_pull_disk_context(new_disk_id).get0();
            auto new_node_ip = disk_context.get_ip();
            new_task.node_ip = new_node_ip;

            //1.3 create new replica for extent group
            metadata_service.create_extent_group_new_replica(task.container_name, task.volume_id, extent_group_id, disk_id, new_disk_id).get0();

            new_task.replica_disk_ids = hive_tools::join(new_disks, ":");
            new_tasks.push_back(new_task);
        }

        //2. make drain plan
        drain_plan plan("drain", new_tasks);
        return std::move(plan);
    }).then_wrapped([this](auto fut){
        try {
            auto plan = fut.get();
            logger.debug("[{}] reallocate_drain_plan done!", _volume_id);
            return make_ready_future<drain_plan>(std::move(plan));
        } catch (...) {
            std::ostringstream out;
            out << "[volume_driver] reallocate_drain_plan failed";
            out << " , volume_id " << _volume_id;
            out << " , error_info:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<drain_plan>(std::runtime_error(error_info));
        }
    });
}


}//end namespace hive
