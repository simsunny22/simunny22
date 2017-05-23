#include "drain_minion.hh"
#include "hive/journal/journal_proxy.hh"
#include "hive/store/extent_store_proxy.hh"
#include "hive/message_data_type.hh"
#include "hive/hive_tools.hh"
#include "hive/context/context_service.hh"
#include "hive/extent_store.hh"
#include "hive/store/extent_store_proxy.hh"

#include "bytes.hh"
#include "types.hh"
#include "log.hh"


namespace hive {

static logging::logger logger("drain_minion");

static inline bool is_me(gms::inet_address from) {
    return from == utils::fb_utilities::get_broadcast_address();
}

drain_minion::drain_minion(){} 
drain_minion::~drain_minion(){}

std::ostream& operator<<(std::ostream& out, const drain_minion& minion) {
    return out << "{drain_minion:["
               << "]}";
}

void drain_minion::set_drain_task_status(drain_task_group& task_group, drain_task_status status){
    for(auto& task : task_group.tasks){
        if( drain_task_status::COMMIT_CREATE_EXTENT_GROUP_ERROR == status 
         && task.need_create_extent_group
         && task.status < drain_task_status::ERROR) {
            task.status = status; 
        }else {
            if(task.status < drain_task_status::ERROR) {
                task.status = status; 
            }
        }
    }
}

//--- for create or replicate extent group 
future<> drain_minion::create_extent_group(drain_extent_group_task& task){
    auto extent_group_id = task.extent_group_id;
    auto disk_id = task.disk_id;
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then(
            [extent_group_id](auto disk_context)mutable{
        auto disk_mount_path = disk_context.get_mount_path();
        auto shard = hive::get_local_extent_store().shard_of(extent_group_id); 
        return hive::get_extent_store().invoke_on(shard, [extent_group_id, disk_mount_path]
                (auto& shard_extent_store)mutable{
            return shard_extent_store.create_extent_group_ex(extent_group_id, disk_mount_path);
        });
    }).then_wrapped([&task](auto f){
        try{
            f.get();
        }catch(...){
            task.status = drain_task_status::CREATE_EXTENT_GROUP_ERROR; 
            auto error_info = sprint("create_extent_group error, extent_group_id:{}, exception:{}"
                , task.extent_group_id, std::current_exception());
            logger.error(error_info.c_str());
        }
        return make_ready_future<>();
    });
}

future<gms::inet_address> 
drain_minion::select_get_extent_group_src_node(sstring disk_ids_str){
    logger.debug("[{}] start, disk_ids_str:{}", __func__, disk_ids_str);

    auto disk_ids = hive_tools::split_to_vector(disk_ids_str, ":");
    assert(disk_ids.size()>0);
    auto rand_index = std::rand() % disk_ids.size();
    sstring src_disk_id = disk_ids.at(rand_index);

    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(src_disk_id).then(
            [](auto disk_context){
        auto disk_ip = disk_context.get_ip();
        return make_ready_future<gms::inet_address>(gms::inet_address(disk_ip));
    });
}

future<sstring> drain_minion::get_disk_mount_path(sstring disk_id){
    logger.debug("[{}] start, disk_id:{}", __func__, disk_id);
    auto& context_service = hive::get_local_context_service();
    return context_service.get_or_pull_disk_context(disk_id).then(
            [](auto disk_context){
        auto mount_path = disk_context.get_mount_path();
        return make_ready_future<sstring>(mount_path);
    });
}

future<> drain_minion::replicate_extent_group(drain_extent_group_task& task){
    return seastar::async([this, &task]()mutable{
        auto volume_id = task.volume_id;
        auto extent_group_id = task.extent_group_id;
        auto replica_disk_ids = task.replica_disk_ids;
        //1. get extent group data
        gms::inet_address src_node_ip = select_get_extent_group_src_node(replica_disk_ids).get0();
        smd_get_extent_group request_data(volume_id, extent_group_id);
        auto& extent_store_proxy = hive::get_local_extent_store_proxy();
        auto&& rmd_data = extent_store_proxy.get_extent_group(std::move(request_data), src_node_ip).get0();

        //2. create extent group with data
        auto dst_disk_id = task.disk_id; 
        auto disk_mount_path = get_disk_mount_path(dst_disk_id).get0();
        auto shard = hive::get_local_extent_store().shard_of(extent_group_id); 
        hive::get_extent_store().invoke_on(shard, [extent_group_id, disk_mount_path 
               , data=std::move(rmd_data.data)](auto& shard_extent_store)mutable{
           return shard_extent_store.create_extent_group_with_data(
               extent_group_id
               , disk_mount_path
               , std::move(data)
           );
        }).get();
    }).then_wrapped([&task](auto f)mutable{
        try {
            f.get(); 
        } catch (...){
            task.status = drain_task_status::REPLICATE_EXTENT_GROUP_ERROR; 
            std::ostringstream out;
            out << "[replicate_extent_group] error"
                << ", task:" << task
                << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
        return make_ready_future<>();
    });
}

future<> drain_minion::prepare_for_task(drain_extent_group_task& task){
    if(task.need_create_extent_group){
        return create_extent_group(task);
    }else if(task.need_replicate_extent_group){
        return replicate_extent_group(task); 
    }else {
        return make_ready_future<>();
    }
}

future<> drain_minion::prepare_for_task_group(drain_task_group& task_group){
    std::vector<future<>> futures;
    auto& tasks = task_group.tasks;    
    for(auto& task : tasks){
        auto fut = prepare_for_task(task); 
        futures.push_back(std::move(fut));
    }

    return when_all(futures.begin(), futures.end()).discard_result();
}

future<> drain_minion::commit_for_create(drain_task_group& task_group){
    std::vector<commit_create_params> commit_params;
    for(auto& task : task_group.tasks){
        if(task.need_create_extent_group && task.status < drain_task_status::ERROR){
            commit_create_params params(
                task.container_name  
                , task.volume_id
                , task.extent_group_id
                , task.disk_id
                ,true
            ); 
            commit_params.push_back(std::move(params));
        }
    }

    auto& metadata_service = hive::get_local_metadata_service();
    return metadata_service.commit_create_groups(std::move(commit_params)).then_wrapped(
            [this, &task_group](auto f)mutable{
        try {
            f.get(); 
        }catch(...){
            //set commit create extent group error
            this->set_drain_task_status(task_group, drain_task_status::COMMIT_CREATE_EXTENT_GROUP_ERROR);
            std::ostringstream out;
            out << "[commit_for_create] error"
                << ", task_group:" << task_group
                << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
        return make_ready_future<>();
    });
}

//--- for get journal data
future<gms::inet_address> 
drain_minion::select_get_journal_data_src_node(std::vector<sstring> journal_nodes){
    logger.debug("[{}] start", __func__);
    if(journal_nodes.size() > 0){
        auto rand_index = std::rand() % journal_nodes.size();
        sstring node_ip = journal_nodes.at(rand_index);
        auto node_address = gms::inet_address(node_ip);
        return make_ready_future<gms::inet_address>(node_address);
    }else{
        sstring error_info = "[select_get_journal_data_src_node] error, journal_nodes is empty"; 
        logger.error(error_info.c_str());
        throw std::runtime_error(error_info);
    }
}

future<std::map<sstring, std::vector<revision_data>>>
drain_minion::get_journal_data(drain_task_group& task_group) {
    logger.debug("[{}] start, task_group:{}", __func__, task_group);
    auto journal_nodes = task_group.journal_nodes;
    return select_get_journal_data_src_node(journal_nodes).then(
            [this, &task_group](auto src_node_ip)mutable{

        auto volume_id  = task_group.volume_id;
        auto segment_id = task_group.segment_id;
        auto shard      = task_group.shard;
        std::map<sstring, std::vector<extent_group_revision>> revisions;
        for(auto& task : task_group.tasks){
            if(task.status >= drain_task_status::ERROR){
                //skip because had error before
            }else{
                auto extent_group_id = task.extent_group_id;
                auto extent_group_revisions = task.extent_group_revisions;
                revisions.insert(std::make_pair(extent_group_id, extent_group_revisions));
            }
        }

        smd_get_journal_data request_data(volume_id, segment_id, shard, std::move(revisions)); 
        auto& journal_proxy = hive::get_local_journal_proxy();
        return journal_proxy.get_journal_data(std::move(request_data), src_node_ip).then([
                ](auto&& rmd_data){
             return std::move(rmd_data.revision_datas);
        });
    }).then_wrapped([this, &task_group](auto f){
        try {
            auto&& revision_datas = f.get0(); 
            return std::move(revision_datas);
        } catch (...){
            this->set_drain_task_status(task_group, drain_task_status::GET_JOURNAL_DATA_ERROR);
            std::ostringstream out;
            out << "[get_journal_data] error"
                << ", task_group:" << task_group
                << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            std::map<sstring, std::vector<revision_data>> empty_data;
            return std::move(empty_data);
        }
    });
}

//--- for write extent group 
future<> drain_minion::write_extent_group(
    drain_extent_group_task& task
    , std::vector<revision_data> revision_datas){
    logger.debug("[{}] start, task:{}, revision_datas.size:{}", __func__, task, revision_datas.size());
    auto disk_id = task.disk_id;
    return get_disk_mount_path(disk_id).then([&task, revision_datas=std::move(revision_datas)]
            (auto disk_mount_path)mutable{
        auto extent_group_id = task.extent_group_id;
        auto shard = hive::get_local_extent_store().shard_of(extent_group_id);
        return hive::get_extent_store().invoke_on(shard, [extent_group_id, disk_mount_path
                , revision_datas=std::move(revision_datas)](auto& shard_extent_store)mutable{
            return shard_extent_store.rwrite_extent_group(
                extent_group_id
                , disk_mount_path
                , std::move(revision_datas)
            ); 
        });
    }).then_wrapped([&task](auto f){
        try{
            f.get(); 
        } catch (...) {
            task.status = drain_task_status::WRITE_EXTENT_GROUP_ERROR;
            std::ostringstream out;
            out << "[write_extent_group] error"
                << ", task:" << task
                << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
        logger.debug("[write_extent_group] done, task:{}", task);
        return make_ready_future<>();
    });
}

future<> drain_minion::write_extent_groups(
    drain_task_group& task_group
    , std::map<sstring, std::vector<revision_data>> revision_datas){
    logger.debug("[{}] start, task_group:{}", __func__, task_group);

    std::vector<future<>> futures;
    for(auto& task : task_group.tasks){
        if(task.status != drain_task_status::INIT){
            //skip: because had error before 
        }else {
            auto extent_group_id = task.extent_group_id;
            auto itor = revision_datas.find(extent_group_id);
            if(itor != revision_datas.end()){
                auto fut = write_extent_group(task, std::move(itor->second)); 
                futures.push_back(std::move(fut));
            }else{
                task.status = drain_task_status::GET_JOURNAL_DATA_ERROR;    
                logger.error("[write_extent_groups] error, can not found journal data, extent_group_id:{}", extent_group_id);
            }
        }
    }

    logger.debug("[{}] do write, futures.size:{}", __func__, futures.size());
    return when_all(futures.begin(), futures.end()).discard_result();
}

future<> drain_minion::commit_for_write(drain_task_group& task_group){
    std::vector<commit_write_params> commit_params;
    for(auto& task : task_group.tasks){
        if(task.status < drain_task_status::ERROR){
            commit_write_params params(
                task.container_name  
                , task.volume_id
                , task.extent_group_id
                , task.disk_id
                , task.version+1
            ); 
            commit_params.push_back(std::move(params));
        }
    }

    auto& metadata_service = hive::get_local_metadata_service();
    return metadata_service.commit_write_groups(std::move(commit_params)).then_wrapped(
            [this, &task_group](auto f)mutable{
        try {
            f.get(); 
            //set success status 
            this->set_drain_task_status(task_group, drain_task_status::SUCCESS);
        }catch(...){
            //set commit write extent group error status
            this->set_drain_task_status(task_group, drain_task_status::COMMIT_WRITE_EXTENT_GROUP_ERROR);
            std::ostringstream out;
            out << "[commit_for_write] error"
                << ", task_group:" << task_group
                << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
        }
        return make_ready_future<>();
    });
}

future<drain_task_group> drain_minion::perform_drain(drain_task_group task_group){
    logger.debug("[{}] start, task:{}", __func__, task_group);
utils::latency_counter lc_total;
lc_total.start();
auto task_group_id = task_group.group_id;

    return seastar::async([this, task_group=std::move(task_group)]()mutable{
        //1. do prepare, create or replicate extent_group
        prepare_for_task_group(task_group).get();
        commit_for_create(task_group).get();

        //2. do get segment data 
        auto&& journal_datas = get_journal_data(task_group).get0();

        //3. do write
        write_extent_groups(task_group, std::move(journal_datas)).get();
        commit_for_write(task_group).get();
        return std::move(task_group);
    }).finally([lc_total, task_group_id]()mutable{
logger.debug("dltest1, perform_drain, task_group_id:{}, latency in ns:{}", task_group_id, lc_total.stop().latency_in_nano());
    });
}

} //namespace hive
