#include "test_function_handler.hh"
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

#include "hive/hive_service.hh"
#include "hive/object/object_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_request.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"
#include "hive/metadata_service/metadata_service.hh"
#include "hive/volume_service.hh"
#include "hive/volume_driver.hh"
#include "hive/metadata_service/model/extent_group_map_helper.hh"

namespace hive{

static logging::logger logger("test_function_handler");


void test1(){
    logger.debug("[{}]  start",__func__);
    auto& metadata_service = hive::get_local_metadata_service();
    metadata_service.update_extent_group_created("vega", "8b010625-f569-493e-92d2-b547e90fab4f17644657","performance_unit#1",true);
    sleep(5s);
    metadata_service.get_created("vega", "8b010625-f569-493e-92d2-b547e90fab4f17644657");
    logger.debug("[{}]  end",__func__);
}

void test2(){
    logger.debug("[{}]  start",__func__);
    auto& metadata_service = hive::get_local_metadata_service();
    metadata_service.get_created("vega", "8b010625-f569-493e-92d2-b547e90fab4f17644657");
    logger.debug("[{}]  end",__func__);
}

future<> test3(){
    logger.debug("[{}]  start",__func__);
    // 1. bind volume_driver
    sstring volume_id = "6e8facedbf6afe43ed8bae5b9b039b8f";
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){
		    std::cout << "test3 1"<<std::endl;
        return shard_volume_service.bind_volume_driver(volume_id).then([volume_id, &shard_volume_service](auto driver_info) mutable{
		    std::cout << "test3 2"<<std::endl;
            return shard_volume_service.get_volume_driver(volume_id);
        }).then([](auto driver_ptr)mutable{ 
            std::cout << "test3 3"<<std::endl;
            return driver_ptr->test_drain_plan();
        });
    });
}

void test4(){
    logger.debug("[{}]  start",__func__);
    extent_group_map_helper helper;
    helper.test1();
    logger.debug("[{}]  end",__func__);
}

future<> test5(){
    logger.debug("[{}]  start",__func__);
    extent_group_map_helper helper;
    return helper.test1().then([](auto success){
        if(success){
            extent_group_map_helper helper1;
	    return helper1.test2();
	}else{
	    logger.debug("set cache failed when test cache.");
	    return make_ready_future<>();
	}
        
        logger.debug("[{}]  end",__func__);
    });
}

void test6(){
    logger.debug("[{}]  start",__func__);
    metadata_cache_item item;
    item.extent_group_id = "group01";
    item.version = 1234;
    item.extents_num = 4;
    item.created = true;
    item.disks = "disk1:disk2:disk3";
    
    for(uint64_t i=0; i<item.extents_num; i++){
        extent_item e_item;
	e_item.extent_id = "vol#" + to_sstring(i);
	e_item.offset    = i*1024*1024;
	item.extents.push_back(e_item);
    }

    temporary_buffer<char> buffer = item.serialize();
    metadata_cache_item item1;

    std::cout <<"serialize size:" << buffer.size()<<std::endl;
    item1.deserialize(std::move(buffer), buffer.size());
    std::cout <<"get metadata item info "
              <<"extent_group_id:"<<item1.extent_group_id
	      <<",version:" <<item1.version
	      <<",extents_num:"<<item1.extents_num
	      <<",created:"<<item1.created
	      <<",disks:" <<item1.disks <<std::endl;
    for (auto extent :item1.extents){
        std::cout<<",extent_id:"<<extent.extent_id
		 <<",extent_offset_in_group:"<<extent.offset<<std::endl;
    }
    logger.debug("[{}]  end",__func__);
    
}

future<> test7(){
    logger.debug("[{}]  start",__func__);
    sstring extent_group_id = "3d6fd3f9-e7f4-47db-b35e-4cd8ea816fee";
    auto& metadata_service = hive::get_local_metadata_service();
    std::vector<commit_create_params> create_params;
    commit_create_params create_param(
        "vega"
        , "default_volume_id"
        , extent_group_id
        , "performance_unit#1"
        , true
    );
    create_params.push_back(create_param);

    logger.debug("[{}]   commit_create_groups start",__func__);
    return metadata_service.commit_create_groups(create_params).then([extent_group_id](){
        auto& metadata_service = hive::get_local_metadata_service();
        logger.debug("[{}]   commit_create_groups end",__func__);
        std::vector<commit_write_params> write_params;
        commit_write_params write_param(
            "vega"
            , "default_volume_id"
            , extent_group_id
            , "performance_unit#1"
            , 12345
        );
        write_params.push_back(write_param);
        logger.debug("[{}]   commit_write_groups start",__func__);
        return metadata_service.commit_write_groups(write_params).then([](){
            logger.debug("[{}]  end",__func__);
        });
    });
}

future<> test8(){
    logger.debug("[{}]  start",__func__);
    // 1. bind volume_driver
    sstring volume_id = "6e8facedbf6afe43ed8bae5b9b039b8f";
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){
		    std::cout << "test8 1"<<std::endl;
        return shard_volume_service.bind_volume_driver(volume_id).then([volume_id, &shard_volume_service](auto driver_info) mutable{
		    std::cout << "test8 2"<<std::endl;
            return shard_volume_service.get_volume_driver(volume_id);
        }).then([](auto driver_ptr)mutable{ 
            std::cout << "test8 3"<<std::endl;
            return driver_ptr->load_volume_metadata().then([driver_ptr](){
                sstring extent_id = "6e8facedbf6afe43ed8bae5b9b039b8f#0";
	        logger.debug("[{}] get block_id 0 extent_group_id:{}",__func__, driver_ptr->get_extent_group_id_from_cache(extent_id));
                logger.debug("[{}]  end",__func__);
                return make_ready_future<>();
            });
        });
    });
}

future<std::unique_ptr<reply>> test_function_handler::handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    logger.debug("[{}] start",__func__);

//        test6();
//        sstring response  = "test_function_success";
//        rep->set_status(reply::status_type::ok, response);
//        rep->done();
//        logger.debug("[{}] done.", __func__); 
//        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    

    return test3().then([rep=std::move(rep)]()mutable{
        sstring response  = "test_function_success";
        rep->set_status(reply::status_type::ok, response);
        rep->done();
        logger.debug("[{}] done.", __func__); 
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });

}//handle


}//namespace hive


