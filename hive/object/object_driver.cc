#include "hive/object/object_driver.hh"
#include "hive/context/context_service.hh"
#include "hive/hive_service.hh"
#include "log.hh"
#include "hive/guru/guru_kit.hh"
#include "hive/metadata_service/metadata_service.hh"

namespace hive{

static logging::logger logger("object_driver");

object_driver::object_driver(){

}

object_driver::~object_driver(){
}

future<object_info> object_driver::get_object_info(sstring object_id) {
    auto it = _object_cache.find(object_id); 
    if (it != _object_cache.end()) {
	auto info = _object_cache[object_id];
        return make_ready_future<object_info>(info);
    }else{
        guru_kit guru;
        auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
        return guru.get_object_entity(metadata_url, object_id).then([this, object_id](auto object_entity){
            //set cache
	    object_info info;
	    info.container_name = object_entity._container_name;
	    info.container_uuid = object_entity._container_uuid;
	    info.size           = object_entity._size;
	    info.last_extent_group_id  = "";
            _object_cache[object_id] = info;
            return make_ready_future<object_info>(info);
        });
    }
}

allocate_item object_driver::allocate_from_new_extent_group(sstring container_name, sstring container_uuid, sstring object_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();

    //new extent group
    auto uuid = utils::make_random_uuid();
    sstring extent_group_id = uuid.to_sstring();

    guru_kit guru;
    auto metadata_url = hive::get_local_hive_service().get_hive_config()->pithos_server_url();
    auto disks = guru.allocate_write_disks(metadata_url, container_uuid).get0();
    
    // create extent group to db
    auto disks_string  = hive_tools::join(disks, ":");
    extent_group_entity entity(extent_group_id, disks_string, object_id, 0);
    metadata_service.create_extent_group(container_name, entity).get0();

    item.extent_group_id = extent_group_id;
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

    // add object map
    metadata_service.create_volume_map(container_name, object_id, extent_id, extent_group_id).get0();

    // insert storage info to storage_map
    metadata_service.create_storage_map(disks, extent_group_id, container_name).get();

    return std::move(item);
}

allocate_item object_driver::allocate_from_last_extent_group(sstring container_name, sstring object_id, sstring extent_group_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();
    auto extents_num = metadata_service.get_extents_num(container_name, extent_group_id).get0();

    //get last extent group extents
    auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
    item.extent_group_id = extent_group_id;
    item.extent_offset_in_group = extents_num*hive::extent_size;
    item.need_create = !metadata_service.get_created(container_name, item.extent_group_id).get0();
    item.version  = metadata_service.get_version(container_name, item.extent_group_id).get0();
    item.disks = disks;

    auto disks_string  = hive_tools::join(disks, ":");
    // add extent to group
    extent_entity extent;
    extent._extent_id = extent_id;
    extent._offset    = item.extent_offset_in_group;
    extent._ref_count = 1;

    metadata_service.add_extent_to_group(container_name, extent_group_id, disks_string, extent).get0();

    metadata_service.create_volume_map(container_name, object_id, extent_id, extent_group_id).get0();

    return std::move(item);
}

allocate_item object_driver::allocate_from_exist_extent_group(sstring container_name, sstring object_id, sstring extent_group_id, sstring extent_id){
    allocate_item item;
    auto& metadata_service = hive::get_local_metadata_service();

    item.extent_id = extent_id;
    item.extent_group_id = extent_group_id;
    item.extent_offset_in_group = metadata_service.get_extent_offset_in_group(container_name, extent_group_id, extent_id).get0();
    auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
    item.need_create = !metadata_service.get_created(container_name, item.extent_group_id).get0();
    item.version  = metadata_service.get_version(container_name, item.extent_group_id).get0();
    item.disks = disks;

    return std::move(item);
}


future<write_plan> object_driver::get_write_plan(sstring object_id, uint64_t offset, uint64_t length){

return with_lock(_driver_lock, [this, object_id, offset, length](){
    return seastar::async([this, object_id, offset, length]()mutable{
	auto object_info = get_object_info(object_id).get0();
        sstring container_name = object_info.container_name;
        sstring container_uuid = object_info.container_uuid;

        auto items  = _planner.split_data(object_id, offset, length);
        std::vector<create_action> c_actions;
        std::vector<write_action>  w_actions;

        auto& metadata_service = hive::get_local_metadata_service();

        for(auto& s_item: items) {
            // 1. find extent_group_id for extent_id
	    allocate_item a_item;
            sstring extent_group_id = metadata_service.get_extent_group_id(container_name, object_id, s_item.extent_id).get0();
            sstring last_group_id   = object_info.last_extent_group_id;

            //need new extent group
            if (extent_group_id == "undefined") {
                //1. check object last group full
                auto extents_num = metadata_service.get_extents_num(container_name, last_group_id).get0();
                if (last_group_id == "" || extents_num == hive::extent_num_in_group) {
                    a_item = allocate_from_new_extent_group(container_name, container_uuid, object_id, s_item.extent_id);
                    //set last extent_group to cache
                    object_info.last_extent_group_id = a_item.extent_group_id;
                    _object_cache[object_id] = object_info;

                    create_action c_action;
                    c_action.extent_group_id = a_item.extent_group_id;
                    c_action.disk_ids = hive_tools::join(a_item.disks, ":");
                    c_actions.push_back(c_action);
                }else{
                    a_item = allocate_from_last_extent_group(container_name, object_id, last_group_id, s_item.extent_id);
               }
            }else{
                a_item = allocate_from_exist_extent_group(container_name, object_id, extent_group_id, s_item.extent_id);
            }

            write_action w_action;
            w_action.extent_id = s_item.extent_id;
            w_action.data_offset_in_extent = s_item.data_offset_in_extent;
            w_action.data_offset = s_item.data_offset;
            w_action.length = s_item.length;

            w_action.extent_group_id = a_item.extent_group_id;
            w_action.extent_offset_in_group = a_item.extent_offset_in_group;
            w_action.disk_ids = hive_tools::join(a_item.disks, ":");

            w_actions.push_back(w_action);
	}

        write_plan plan;
        plan.owner_id = object_id;
        plan.write_actions  = w_actions;
        plan.create_actions = c_actions;
        return std::move(plan);
    }).then_wrapped([](auto f)mutable{
        try {
            auto&& plan = f.get0();
            return make_ready_future<write_plan>(std::move(plan));
        }catch (...) {
            throw std::runtime_error("allocate_extent_group error"); 
        }
    });
});

}

future<read_plan> object_driver::get_read_plan(sstring object_id, uint64_t offset, uint64_t length){
    logger.debug("[{}] start object_id:{}, offset:{} length:{}",__func__, object_id, offset, length);
    return seastar::async([this, object_id, offset, length]()mutable{

	auto object_info = get_object_info(object_id).get0();
        sstring container_name = object_info.container_name;
        auto& metadata_service = hive::get_local_metadata_service();
        auto read_items = _planner.split_data(object_id, offset, length);
        std::vector<read_action> r_actions;
        for(auto& item: read_items) {
          // 1、find extent_group_id for extent_id
          sstring extent_group_id = metadata_service.get_extent_group_id(container_name, object_id, item.extent_id).get0();
          read_action r_action;

          r_action.extent_id = item.extent_id;
          r_action.length    = item.length;
          r_action.data_offset_in_extent = item.data_offset_in_extent;

          //need new extent group
          if (extent_group_id == "undefined") {
              //1. check object last group full
              r_action.extent_group_id = "undefined";
              r_action.extent_offset_in_group = 0;
              r_action.disk_ids = "";
              r_action.md5 = "";
          }else{
              r_action.extent_group_id = extent_group_id;
              r_action.extent_offset_in_group = metadata_service.get_extent_offset_in_group(container_name, extent_group_id, r_action.extent_id).get0();
              auto disks = metadata_service.get_extent_group_disks(container_name, extent_group_id).get0();
              r_action.disk_ids = hive_tools::join(disks, ":");
              r_action.md5 = "";
          }

          r_actions.push_back(r_action);
        }

        read_plan plan;
        plan.owner_id = object_id;
        plan.read_actions = r_actions;
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

future<> object_driver::commit_create_group(sstring object_id, std::vector<create_action> create_actions){
    return get_object_info(object_id).then([this, object_id, create_actions](auto object_info){
        sstring container_name = object_info.container_name;
        std::vector<future<>> futures;
        for(auto& action: create_actions) {
          auto& metadata_service = hive::get_local_metadata_service();
          sstring extent_group_id = action.extent_group_id;
          sstring disk_ids = action.disk_ids;
          auto f = metadata_service.update_extent_group_created(container_name, extent_group_id, disk_ids, true);
          futures.push_back(std::move(f));
        }

        return when_all(futures.begin(), futures.end()).discard_result();
    });
}

future<> object_driver::commit_for_write_object(std::vector<hint_entry> hints){
    return make_ready_future<>();
}

future<uint64_t> object_driver::commit_write_object(sstring object_id, uint64_t offset, uint64_t length){
    return get_object_info(object_id).then([this, object_id, offset, length](auto object_info){
        uint64_t old_size = object_info.size;
        if ((offset+length) > old_size){
            object_info.size = offset+length;
	    _object_cache[object_id] = object_info;
	    // need update db?
            auto& metadata_service = hive::get_local_metadata_service();
	    metadata_service.update_object_size(object_id, object_info.size);
            return make_ready_future<uint64_t>(object_info.size);
	}else{
            return make_ready_future<uint64_t>(old_size);
	}
    });
}



}//end namespace hive




