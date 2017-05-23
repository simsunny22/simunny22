#include "rwrite_volume_handler_test.hh"

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
#include "hive/volume_service.hh"
#include "hive/stream_service.hh"
#include "hive/hive_result.hh"
#include "hive/extent_datum.hh"
#include "hive/hive_config.hh"
#include "hive/token_service.hh"
#include "hive/hive_tools.hh"
#include "hive/http/json11.hh"

#include "hive/trail/trail_service.hh"
#include "hive/trail/access_trail.hh"
#include "hive/hive_service.hh"
#include "hive/exceptions/exceptions.hh"
#include "hive/extent_store.hh"

namespace hive{

extern hive::hive_status hive_service_status;

static logging::logger logger("rwrite_volume_handler_test");

uint64_t g_offset_0 = 0;
uint64_t g_offset_1 = 0;
uint64_t g_offset_2 = 0;
uint64_t g_offset_3 = 0;

static uint64_t get_test_offset(uint64_t shard, uint64_t size) {
    if(shard == 0){
        g_offset_0 += size;
        return g_offset_0; 
    }else if (shard == 1){
        g_offset_1 += size;
        return g_offset_1; 
    }else if (shard == 2){
        g_offset_2 += size;
        return g_offset_2; 
    }else if(shard == 3) {
        g_offset_3 += size;
        return g_offset_3; 
    }else{
       assert(false); 
       return 0;
    }
}


sstring rwrite_volume_handler_test::get_header_value(header_map& headers, sstring header_key, sstring default_value){
    header_map::const_iterator itor = headers.find(header_key);
    if( itor != headers.end() ){
        auto header_value = itor->second;
        if(!header_value.empty()){
            return header_value;    
        }
    }
 
    if(!default_value.empty()){
        return default_value; 
    }

    std::ostringstream out;
    out << "[get_header_value] error, can not find header value or the value is empty";
    out << ", header_key:" << header_key;
    auto error_info = out.str();
    logger.error(error_info.c_str());
    throw std::runtime_error(error_info);
}

hive_write_command rwrite_volume_handler_test::build_write_command(request& req){
    auto config = hive::get_local_hive_service().get_hive_config();
    int volume_count = config->ctx_volume_count(); 
    sstring owner_id = "volume_" + to_sstring(hive_tools::random(volume_count));
    int extent_group_count = config->ctx_extent_group_count_per_volume();
    uint64_t volume_size = extent_group_count * 4*1024*1024; 
    uint64_t offset = (hive_tools::random(volume_size-4096)/512)*512; 

    temporary_buffer<char> req_body = req.move_body();
    assert(req_body.size() == (size_t)req.get_content_length());
    bytes data = bytes(reinterpret_cast<const signed char *>(req_body.get()), req.get_content_length());
    uint64_t length = data.size(); 
    hive_write_command write_cmd(
        owner_id
        , offset 
        , length 
        , std::move(data)
    );    
    
    return std::move(write_cmd);
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test::redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip){
    //tododl:yellow this url build is unsuitable    
    sstring redirect_url = "http://" + redirect_ip + ":9041/hive_service/rwrite_volume";
    rep->set_status(reply::status_type::ok);
    rep->_headers["X-Vega-Location"] = redirect_url;
    rep->_headers["X-Vega-Status-Code"] = "307";
    rep->done();
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test::rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                             , hive_write_command write_cmd){
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);
    auto volume_id = write_cmd.owner_id;
    auto offset = write_cmd.offset;
    auto length = write_cmd.length;

    auto& stream_service = hive::get_local_stream_service();
    return stream_service.find_stream(volume_id).then(
        [this, write_cmd=std::move(write_cmd)](auto stream)mutable{
        logger.debug("[rwrite_volume] start, write_cmd:{}", write_cmd);
        return stream->rwrite_volume(std::move(write_cmd)); 
    }).then_wrapped([this, volume_id, offset, length, rep=std::move(rep)](auto f) mutable{ 
        try{
            f.get();
            sstring response  = this->build_return_json();
            rep->set_status(reply::status_type::ok, response);
            rep->done("json");
            logger.debug("[rwrite_by_volume_stream] done, volume_id:{}, offset:{}, length:{}"
                , volume_id, offset, length);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
        }catch(...){
             //tododl:yellow we should return accurate error code
             std::ostringstream out;
             out << "[rwrite_by_volume_stream] error";
             out << ", volume_id:" << volume_id;
             out << ", offset:" << offset;
             out << ", length:" << length;
             out << ", exception:" << std::current_exception();
             auto error_info = out.str();
             logger.error(error_info.c_str());
             throw std::runtime_error(error_info);
        }
    });
}

sstring rwrite_volume_handler_test::build_return_json(){
    hive::Json json_obj = hive::Json::object {
        {"success", true}
    };
    auto str = json_obj.dump();
    return str;
}

future<std::unique_ptr<reply> > rwrite_volume_handler_test::handle(
    const sstring& path
    , std::unique_ptr<request> req
    , std::unique_ptr<reply> rep) {

    if(NORMAL != hive_service_status ){
        rep->set_status(reply::status_type::service_unavailable);
        rep->done();
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }

    auto write_cmd = build_write_command(*req);
    logger.debug("[{}] start, write_cmd:{}", __func__, write_cmd);

    // 1. bind volume_driver
    sstring volume_id = write_cmd.owner_id;
    auto shard = hive::get_local_volume_service().shard_of(volume_id);
    return hive::get_volume_service().invoke_on(shard, [volume_id] (auto& shard_volume_service){
        return shard_volume_service.bind_volume_driver(volume_id);
    }).then([this, rep=std::move(rep), write_cmd=std::move(write_cmd)]
            (auto driver_ex)mutable{ 
        if(driver_ex.need_redirect){
            // 1.1 redirect write
            logger.debug("[handle] redirect to driver node, write_cmd:{}, redirect_ip:{}"
                , write_cmd, driver_ex.redirect_ip);
            return this->redirect_rwrite(std::move(rep), driver_ex.redirect_ip);
        }else{
            // 1.2 write locally 
            // TODO:write_cmd.set_vclock(driver_ex.vclock);
            return this->rwrite_by_volume_stream(std::move(rep), std::move(write_cmd)); 
           
            //dltest1
            //return this->test(std::move(rep), std::move(write_cmd)); 
        }
    });
}//handle

static future<> do_write_test(uint64_t shard, hive_write_command write_cmd){
    sstring dir_path = "/home/vega/data/hive-storage/disks/drive-scsi0-0-1-0/dltest/";
    sstring file_path = dir_path + to_sstring(shard);

    return open_file_dma(file_path, open_flags::wo).then([write_cmd=std::move(write_cmd)](auto fd)mutable{

        //sstring volume_id = write_cmd.owner_id;
        //uint64_t offset = write_cmd.offset;
        uint64_t length = write_cmd.length;
        bytes&& data = std::move(write_cmd.data);

        uint64_t align_size = length;
        //uint64_t align_offset = (std::rand() % 512)*8192;
        uint64_t align_offset = get_test_offset(engine().cpu_id(), align_size);


        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        auto buf = bufptr.get();
        std::copy_n(data.begin(), align_size, buf);
        return fd.dma_write(align_offset, buf, align_size).then(
                [fd, align_size, bufptr=std::move(bufptr)](auto write_size)mutable{
            if(write_size != align_size){
                std::ostringstream out;
                out << "[write_file] error";
                out << ", need write size:" << align_size << ", not equal actually write size:" << write_size;
                auto error_info = out.str();
                throw std::runtime_error(error_info);
            }
            return fd.flush();
        }).finally([fd]()mutable{
            fd.close().finally([fd] {});
        });
    });
}

future<std::unique_ptr<reply>> 
rwrite_volume_handler_test::test(std::unique_ptr<reply> rep
                               , hive_write_command write_cmd){
    int shard = rand()% smp::count;
    return smp::submit_to(shard, [this, shard, write_cmd=std::move(write_cmd)]()mutable{
        auto& extent_store = hive::get_local_extent_store();
        return extent_store.write_journal_test.wait(1).then([&extent_store, shard, write_cmd=std::move(write_cmd)]()mutable{
             return do_write_test(shard, std::move(write_cmd)).finally([&extent_store](){
                 extent_store.write_journal_test.signal(1);
             }); 
        });
    }).then([this, rep=std::move(rep)]()mutable{
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}





}//namespace hive
