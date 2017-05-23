#include "journal_segment.hh"
#include "log.hh"

#include "hive/hive_tools.hh"
#include "hive/context/context_service.hh"
#include "hive/message_data_type.hh"
#include "hive/journal/journal_proxy.hh"
#include "hive/hive_service.hh"
#include "hive/journal/journal_service.hh"
#include "hive/journal/primary_journal.hh"
#include "hive/journal/secondary_journal.hh"


namespace hive{
static logging::logger logger("journal_segment");

static std::unique_ptr<char[], free_deleter> bytes_to_buffer(bytes& data, uint64_t align_size){
    auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
    std::memset(bufptr.get(), 0, align_size);
    auto buf = bufptr.get();
    std::copy_n(data.begin(), align_size, buf);
    return std::move(bufptr);
};

static bytes buffer_to_bytes(std::unique_ptr<char[], free_deleter>& bufptr, uint64_t align_size){
    auto buf = bufptr.get();
    bytes data(align_size, 0);
    std::memcpy(data.begin(), buf, align_size);
    return std::move(data);
}


static  future<> create_file(
        sstring volume_id,
        sstring segment_id,
        sstring path,
        uint64_t max_size){
    logger.debug("segment_commitlog::create_commitlog_file start ....volume_id:{}, segment_id:{}, path:{}"
        ,volume_id, segment_id, path);
    file_open_options opt;
    opt.extent_allocation_size_hint = max_size;
    sstring file_name = path + "/" + segment_id;

    return open_checked_file_dma(general_disk_error_handler, file_name, open_flags::rw|open_flags::create, opt).then(
            [volume_id, segment_id, max_size](auto fd){
        return fd.truncate(max_size).finally([fd]()mutable{
            return fd.close().finally([fd]{});
        });
    });
}
// =======================================================================
// journal segment
// =======================================================================
//public

//static
future<lw_shared_ptr<journal_segment>> journal_segment::make_primary_segment(
        sstring volume_id){
    logger.debug("journal_segment::make_journal_segment start, volume_id:{}", volume_id);
    sstring segment_id = hive_tools::generate_journal_segment_id(volume_id);
    //sstring path = "/home/vega/data/hive/secondary_commitlog";                    

    auto& hive_service = get_local_hive_service();
    auto hive_config = hive_service.get_hive_config();
    auto path = hive_config->primary_commitlog_directory();
    auto size = hive_config->commitlog_max_size_mb();
    logger.debug("journal_segment::make_journal_segment start, path:{}, max_size:{}", path, size);
    
    return do_make_primary_segment(volume_id, segment_id, path, size*1024*1024);
}

future<lw_shared_ptr<journal_segment>> journal_segment::make_secondary_segment(
        sstring volume_id, sstring segment_id, sstring path, uint64_t size){
    uint64_t max_size = size*1024*1024;
    return create_file(volume_id, segment_id, path, max_size).then([
            volume_id, segment_id, path, max_size](){
        return segment_commitlog::make_secondary_segment_commitlog(volume_id, segment_id,
                path, max_size).then([volume_id, segment_id](auto commitlog_ptr){
            return segment_memtable::make_segment_memtable(volume_id).then([volume_id, segment_id
                    , commitlog_ptr = std::move(commitlog_ptr)](auto memtable_ptr){
                return make_lw_shared<journal_segment>(volume_id, segment_id, std::move(commitlog_ptr)
                    , std::move(memtable_ptr));
            });
        });       
    });

}

future<lw_shared_ptr<journal_segment>>
journal_segment::do_make_primary_segment(
        sstring volume_id,
        sstring segment_id,
        sstring path,
        uint64_t max_size){
    logger.debug("journal_segment::init_journal_segment start, volume_id:{}, segment_id:{}", volume_id, segment_id);
    auto& context_service = get_local_context_service();
    return context_service.get_or_pull_volume_context(volume_id).then([volume_id, segment_id, 
            path, max_size](auto volume_context){
        std::vector<future<>> futs;
        //auto f_local = segment_commitlog::create_commitlog(volume_id, segment_id, path, max_size);
        auto f_local = create_file(volume_id, segment_id, path, max_size);

        futs.push_back(std::move(f_local));

        auto& journal_nodes = volume_context.get_journal_nodes();
        std::unordered_set<gms::inet_address> targets;
        for(auto& journal_node : journal_nodes){
            targets.insert(gms::inet_address(journal_node.get_ip()));
        }

        if(!targets.empty()){
            unsigned cpu_id = engine().cpu_id();
            auto& proxy = get_local_journal_proxy();
            smd_init_segment smd(cpu_id, volume_id, segment_id, max_size);
            auto f_remote = proxy.init_segment(smd, targets); 
            futs.push_back(std::move(f_remote));
        }

        return when_all(futs.begin(), futs.end()).then([volume_id, segment_id, max_size, path](auto futs){
            try{
                for(auto& fut: futs){
                    fut.get();
                }
                return make_ready_future<>();
            }catch(...){
                std::ostringstream out;
                out << "[create_commitlog] error";
                out << ", exception:" << std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());

                auto file_name = path + "/" + segment_id;
                commit_io_check(::unlink, file_name.c_str());
                return make_exception_future<>(std::runtime_error(error_info));
            }
        });
    }).then([volume_id, segment_id, max_size, path](){
        return segment_commitlog::make_primary_segment_commitlog(volume_id, segment_id,
                path, max_size).then([volume_id, segment_id](auto commitlog_ptr){
            return segment_memtable::make_segment_memtable(volume_id).then([volume_id, segment_id
                    , commitlog_ptr = std::move(commitlog_ptr)](auto memtable_ptr){
                return make_lw_shared<journal_segment>(volume_id, segment_id, std::move(commitlog_ptr)
                    , std::move(memtable_ptr));
            });
        });
    });
}

//construct and destrcut
journal_segment::journal_segment(
    sstring volume_id
    , sstring journal_segment_id
    , lw_shared_ptr<segment_commitlog> commitlog_ptr
    , lw_shared_ptr<segment_memtable> memtable_ptr)
        : _volume_id(volume_id)
        , _seg_id(journal_segment_id)
        , _seg_commitlog(std::move(commitlog_ptr))
        , _seg_memtable(std::move(memtable_ptr)){
    logger.debug("journal_segment::construct start ... volume_id:{}, segment_id:{}", _volume_id, _seg_id);
}


journal_segment::journal_segment(journal_segment&& segment) noexcept
    : _volume_id(segment._volume_id)
    , _seg_id(segment._seg_id)
    , _seg_commitlog(std::move(segment._seg_commitlog))
    , _seg_memtable(std::move(segment._seg_memtable))
    , _occupancy(segment._occupancy)
    , _mutex(std::move(_mutex)){}


journal_segment::~journal_segment(){
    logger.debug("journal_segment destructor start .... volume_id:{} journal_segment_id:{}",_volume_id,  _seg_id);
}


//write segment
//future<> journal_segment::write_primary_segment(extent_revision revision){
future<> journal_segment::write_primary_segment(volume_revision revision){
//TODO:yellow too long
    logger.debug("journal_segment::write_data start .... volume_id:{} ", revision.owner_id);
    //return with_monad([this, revision = std::move(revision)]()mutable{
        auto     segment_id        = _seg_id;
        auto     volume_id         = revision.owner_id;
        uint64_t offset_in_volume  = revision.offset_in_volume; 
        auto     extent_id         = revision.extent_id;
        uint64_t offset_in_extent  = revision.offset_in_extent;
        auto     vclock            = revision.vclock;
        auto     length            = revision.length;

        auto size = revision.serialize_size();
        auto align_size = align_up(size, 512UL);

        auto offset = get_commitlog_offset();
        auto align_offset = align_up(offset, 512UL);

        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
	std::memset(bufptr.get(), 0, align_size);
        revision.serialize(bufptr);
        auto&& data = buffer_to_bytes(bufptr, align_size);

        auto& context_service = get_local_context_service();
        return context_service.get_or_pull_volume_context(volume_id).then([
                this, segment_id, align_offset, volume_id, offset_in_volume,
                extent_id, offset_in_extent, length, align_size, vclock, 
                bufptr = std::move(bufptr), data = std::move(data)](auto volume_context)mutable{
            logger.debug("journal_segment::write_data  .... volume_id:{}, extent_id:{}, offset:{}, size:{} "
                , volume_id, extent_id, align_offset, align_size);
            std::vector<future<>> futs;
            auto f_local = _seg_commitlog->write_primary_commitlog(std::move(bufptr), extent_id, align_size);
            futs.push_back(std::move(f_local));

            auto& journal_nodes = volume_context.get_journal_nodes();
            std::unordered_set<gms::inet_address> targets;
            for(auto& journal_node : journal_nodes){
                targets.insert(gms::inet_address(journal_node.get_ip()));
            }

            if(!targets.empty()){
                auto& proxy = get_local_journal_proxy();
                unsigned cpu_id = engine().cpu_id();
                smd_sync_segment smd(cpu_id, segment_id, align_offset, volume_id, offset_in_volume,
                    extent_id, offset_in_extent, length, align_size, vclock, std::move(data));
                auto f_remote = proxy.sync_segment(smd, targets); 
                futs.push_back(std::move(f_remote));
            }

            return when_all(futs.begin(), futs.end()).then([this, align_offset](auto futs){
                try{
                    for(auto& fut: futs){
                        fut.get();
                    }
                    return make_ready_future<>();
                }catch(...){
                    std::ostringstream out;
                    out << "[sync_commitlog] error";
                    out << ", exception:" << std::current_exception();
                    auto error_info = out.str();
                    logger.error(error_info.c_str());
                    this->rollback_commitlog_offset(align_offset);
                    return make_exception_future<>(std::runtime_error(error_info));
                }
            });
        }).then([this, segment_id, align_offset, volume_id, offset_in_volume, 
                extent_id, offset_in_extent, length, align_size, vclock](){
            revision_index index(
                segment_id, 
                align_offset, 
                volume_id,
                offset_in_volume, 
                extent_id, 
                offset_in_extent, 
		length,
                align_size, 
                vclock);
            _occupancy += align_size;
            return _seg_memtable->insert_index(extent_id, index);
        });
    //});

}


future<>  journal_segment::sync_secondary_segment(
        sstring segment_id,
        uint64_t offset_in_segment,
        uint64_t offset_in_volume, 
        sstring extent_id,
        uint64_t offset_in_extent,
        uint64_t data_length,
        uint64_t serialize_length,
        uint64_t vclock, 
        bytes data){
    return _seg_commitlog->write_secondary_commitlog(std::move(data), offset_in_segment, serialize_length).then([ 
            this, segment_id, offset_in_segment, offset_in_volume, extent_id, offset_in_extent, data_length, serialize_length, vclock](){
        _seg_memtable->rollback_for_sync(offset_in_segment);

        uint64_t offset_in_volume = 0; //wztest for compile
        uint64_t offset_in_extent = 0; //wztest for compile
        revision_index index(
            segment_id,
            offset_in_segment, 
            _volume_id, 
            offset_in_volume, 
            extent_id, 
            offset_in_extent,
            data_length,
	    serialize_length,
            vclock);    
        _occupancy += serialize_length;
        return _seg_memtable->insert_index(extent_id, index);
    });
}

//read segment
future<std::vector<volume_revision>> journal_segment::read_segment(hive_read_subcommand read_cmd){
    sstring extent_id = read_cmd.extent_id;
    return _seg_memtable->get_indexs(extent_id).then([this, extent_id, read_cmd](auto indexs){
    logger.debug(" segment_memtable::read_segment indexs.size:{} extent_id:{}", indexs.size(), extent_id);
        uint64_t begin = read_cmd.data_offset_in_extent;
        uint64_t end = begin + read_cmd.length;
        std::vector<future<volume_revision>> futs;
        for(auto index : indexs){
            uint64_t target_begin = index.offset_in_extent; 
            uint64_t target_end = index.offset_in_extent + index.data_length; 
            if((target_begin <= begin && target_end >= begin) || (target_begin >=begin && target_begin<=end)){
                auto fut = _seg_commitlog->read_commitlog(index);
                futs.push_back(std::move(fut));
            }
        }

        return when_all(futs.begin(), futs.end()).then([](auto futs){
            std::vector<volume_revision> revisions;
            for(auto& fut: futs){
                auto revision = fut.get0();
                revisions.push_back(std::move(revision));
            }

    logger.debug(" segment_memtable::read_segment testpengjiang done ");
            std::sort(revisions.begin(), revisions.end(),
                [&](volume_revision& x, volume_revision& y){return x.vclock<y.vclock;});
            return make_ready_future<std::vector<volume_revision>>(std::move(revisions));
            
        });
    });
}

//future<std::vector<extent_revision>> 
//journal_segment::get_data_by_extent_group_id(sstring extent_group_id){
//    return _seg_memtable->get_indexs_by_extent_group_id(extent_group_id).then([this](auto indexs){
//        std::vector<future<extent_revision>> futs;
//        for(auto index : indexs){
//            auto fut = _seg_commitlog->read_commitlog(index);
//            futs.push_back(std::move(fut));
//        }
//
//        return when_all(futs.begin(), futs.end()).then([](auto futs){
//            std::vector<extent_revision> revisions;
//            for(auto& fut: futs){
//                auto revision = fut.get0();
//                revisions.push_back(std::move(revision));
//            }
//
//            std::sort(revisions.begin(), revisions.end(),
//                [&](extent_revision& x, extent_revision& y){return x.vclock<y.vclock;});
//            return make_ready_future<std::vector<extent_revision>>(std::move(revisions));
//        });
//    });
//}

//TODO:red name should be discard()
future<> journal_segment::discard_primary_segment(){
   logger.debug("journal_segment::discard_commitlog discard commitlog start ... volume_id:{}, segment_id:{}"
       , _volume_id, _seg_id);
   auto& context_service = get_local_context_service();
   return context_service.get_or_pull_volume_context(_volume_id).then([this](auto volume_context){
       std::vector<future<>> futs;       
       auto f_local = _seg_commitlog->discard_commitlog();
       auto& journal_nodes = volume_context.get_journal_nodes();
       std::unordered_set<gms::inet_address> targets;
       for(auto& journal_node : journal_nodes){
           targets.insert(gms::inet_address(journal_node.get_ip()));
       }
       
       if(!targets.empty()){
           auto& proxy = get_local_journal_proxy();
           unsigned cpu_id = engine().cpu_id();
           smd_discard_segment smd(cpu_id, _volume_id, _seg_id);
           auto f_remote = proxy.discard_segment(smd, targets); 
           futs.push_back(std::move(f_remote));
       }

       return when_all(futs.begin(), futs.end()).then([](auto futs){
           try{
               for(auto& fut: futs){
                   fut.get();
               }
               return make_ready_future<>();
           }catch(...){
               std::ostringstream out;
               out << "[sync_commitlog] error";
               out << ", exception:" << std::current_exception();
               auto error_info = out.str();
               logger.error(error_info.c_str());
               return make_exception_future<>(std::runtime_error(error_info));
           }
       });
   });
}

future<> journal_segment::discard_secondary_segment(){
    return _seg_commitlog->discard_commitlog();
}

bool journal_segment::should_flush(){
    return _seg_commitlog->should_flush();
}

uint64_t journal_segment::get_occupancy() {
    return _occupancy;
}

bool journal_segment::empty() {
    return _seg_memtable->empty();
}

void journal_segment::rollback_for_rebuild(uint64_t vclock){
    _seg_memtable->rollback_for_rebuild(vclock);
}

future<std::vector<volume_revision_view>>
journal_segment::scan(){
    return _seg_memtable->get_order_indexs().then([this](auto indexs){
        std::vector<volume_revision_view> views;
        for(auto& index: indexs){
            volume_revision_view view;
            view.volume_id                 = index.volume_id;
            view.offset_in_volume          = index.offset_in_volume;
            view.extent_id                 = index.extent_id;
            view.offset_in_extent          = index.offset_in_extent;
            view.journal_segment_id        = index.segment_id;
            view.offset_in_journal_segment = index.offset_in_segment; 
            view.length                    = index.data_length;
            view.vclock                    = index.vclock;

            views.push_back(std::move(view));
        }

        return make_ready_future<std::vector<volume_revision_view>>(std::move(views));
    });
}

future<std::map<sstring, std::vector<revision_data>>>
journal_segment::get_revision_data(std::map<sstring, std::vector<extent_group_revision>> revision_map){
    return _seg_commitlog->read_commitlog_for_drain().then([this, 
            revision_map = std::move(revision_map)](auto bufptr) mutable{
        std::map<sstring , std::vector<revision_data>> data_map;
        for(auto it = revision_map.begin(); it != revision_map.end(); it++){
            sstring extent_group_id = it->first;
            auto&  revisions = it->second;
            std::vector<revision_data>&& revision_datas = this->do_get_revision_data(revisions, bufptr);
            auto data_itor = data_map.find(extent_group_id);
            if(data_itor == data_map.end()){
                data_map.insert(std::pair<sstring, std::vector<revision_data>>(
                    extent_group_id, 
                    std::move(revision_datas))
              );
            }else{
                for(auto& revision_data : revision_datas){
                    data_itor->second.push_back(std::move(revision_data));
                }
            }
        }
        return make_ready_future<std::map<sstring , std::vector<revision_data>>>(std::move(data_map));
    });
}

std::vector<revision_data>
journal_segment::do_get_revision_data(
  std::vector<extent_group_revision>& revisions,
  std::unique_ptr<char[], free_deleter>&  bufptr){
    std::vector<revision_data> datas;
    for(auto& revision: revisions){
        auto offset_in_segment = revision.offset_in_journal_segment;
        auto offset_in_group   = revision.offset_in_extent_group;
        auto length            = revision.length;
        auto vclock            = revision.vclock;

        volume_revision volume_revision;
        volume_revision.deserialize(bufptr, offset_in_segment);
        assert(length == volume_revision.length);
        
        revision_data data(
            vclock,
            offset_in_segment,
            offset_in_group,
            volume_revision.length,
            std::move(volume_revision.data)
        );

        datas.push_back(std::move(data));
    }
    return std::move(datas);
}

//private
uint64_t journal_segment::get_commitlog_offset(){
   return _seg_commitlog->get_offset();
}

void journal_segment::rollback_commitlog_offset(uint64_t offset){
   return _seg_commitlog->set_offset(offset);
}                                                            

// =======================================================================
// commitlog
// =======================================================================
future<lw_shared_ptr<segment_commitlog>>
segment_commitlog::make_primary_segment_commitlog(
        sstring volume_id,
        sstring segment_id, 
        sstring path, 
        uint64_t max_size){
    logger.debug("make_primary_segment_commitlog start, volume_id:{}, segment_id:{}, path:{}", volume_id, segment_id, path);
    file_open_options opt;
    sstring file_name = path + "/" + segment_id;

    return open_checked_file_dma(general_disk_error_handler, file_name, open_flags::rw, opt).then([
            volume_id, path, segment_id, max_size](auto fd){
        return make_ready_future<lw_shared_ptr<segment_commitlog>>(
                make_lw_shared<segment_commitlog>(volume_id, path, segment_id, fd, max_size, commitlog_flag::PRIMARY));
    });
}

future<lw_shared_ptr<segment_commitlog>>
segment_commitlog::make_secondary_segment_commitlog(
        sstring volume_id,
        sstring segment_id,
        sstring path,
        uint64_t max_size){
    logger.debug("make_secondary_segment_commitlog start, volume_id:{}, segment_id:{}, path:{}", volume_id, segment_id, path);
    file_open_options opt;
    sstring file_name = path + "/" + segment_id;

    return open_checked_file_dma(general_disk_error_handler, file_name, open_flags::rw, opt).then([
            volume_id, path, segment_id, max_size](auto fd){
        return make_ready_future<lw_shared_ptr<segment_commitlog>>(
                make_lw_shared<segment_commitlog>(volume_id, path, segment_id, fd, max_size, commitlog_flag::SECONDARY));
    });
}

segment_commitlog::segment_commitlog(
    sstring volume_id
    , sstring path
    , sstring segment_id
    , file fd
    , uint64_t max_size
    , commitlog_flag flag)
        : _volume_id(volume_id)
        , _path(path)
        , _segment_id(segment_id)
        , _fd(fd)
        , _max_size(max_size)
        , _flag(flag)
        , _write_flush_semaphore(1)
{
        
    auto& hive_service = get_local_hive_service();
    auto hive_config = hive_service.get_hive_config();
    auto commitlog_sync = hive_config->hive_commitlog_sync();
    if("periodic" == commitlog_sync){
        _sync_mode = sync_mode::PERIODIC; 
    }else if("batch" == commitlog_sync){
        _sync_mode = sync_mode::BATCH; 
    }else {
        _sync_mode = sync_mode::UNKOWN; 
    }

    set_timer();
}


segment_commitlog::~segment_commitlog(){
    logger.debug("destruct  segment_commitlog start ...volume_id:{}, segment_id:{}, path:{}"
        , _volume_id, _segment_id, _path);
    _timer.cancel();
    if(_can_delete){
        sstring file_name;
        file_name = _path + "/" + _segment_id;
        commit_io_check(::unlink, file_name.c_str());
    }
}

void segment_commitlog::set_timer(){
    if(_sync_mode == sync_mode::PERIODIC){
        auto& hive_service = get_local_hive_service();
        auto hive_config = hive_service.get_hive_config();
        auto periodic = hive_config->hive_commitlog_sync_period_in_ms();
        if(periodic > 0) {
            _timer.set_callback(std::bind(&segment_commitlog::on_timer, this));
            _timer.arm(lowres_clock::now() + std::chrono::milliseconds(300)
            , std::experimental::optional<lowres_clock::duration>{std::chrono::milliseconds(periodic)});
        }
    }
}

void segment_commitlog::on_timer(){
    if(_flush_step >= _write_step){
        return;
    }

    _write_flush_semaphore.wait().then([this](){
        _flush_step = _write_step; 
        return _fd.flush().finally([this](){
            _write_flush_semaphore.signal(); 
        });
    });
}

future<> segment_commitlog::sync_data(uint64_t write_size){
    if(_sync_mode == sync_mode::PERIODIC){
        this->_offset += write_size;
        return make_ready_future<>();  
    }else if(_sync_mode == sync_mode::BATCH){
        return _fd.flush().then([this, write_size]()mutable{
            this->_offset += write_size;
        });
    }else {
        logger.error("[{}] warn, unkown sync_mode:{}", __func__, _sync_mode);
        this->_offset += write_size;
        return make_ready_future<>();  
    }
}

future<> segment_commitlog::write_primary_commitlog(std::unique_ptr<char[], free_deleter> bufptr, sstring extent_id, uint64_t align_size){
    return _write_flush_semaphore.wait().then([this, bufptr=std::move(bufptr), extent_id, align_size]()mutable{
        auto buf = bufptr.get();
        return _fd.dma_write(_offset, buf, align_size).then([this, extent_id, bufptr=std::move(bufptr)](auto size){
            logger.debug("segment_commitlog::write_data success, file:{}, offset:{}, size:{} ", this->_segment_id, this->_offset, size);
            if(_offset >= _max_size){
                logger.error("segment_commitlog::write_data write_overflower segment_id:{}, offset:{}, size:{}",
                    _segment_id, _offset, size);
            }
            ++_write_step;
            if(_write_step <= 0){
                //another period of uint64_t
                _flush_step = 0; 
            }
            return this->sync_data(size);
        }).finally([this](){
            _write_flush_semaphore.signal(); 
        });
    });
}

future<> segment_commitlog::write_secondary_commitlog(bytes data, uint64_t offset_size, uint64_t size){
    logger.debug("segment_commitlog::write_secondary_commitlog offset:{}, sync_offset:{}, size:{}"
        , _offset, offset_size, size);
    return _write_flush_semaphore.wait().then([this, data=std::move(data), offset_size, size]()mutable{
        auto bufptr  = bytes_to_buffer(data, size);
        auto buf = bufptr.get();
        return _fd.dma_write(offset_size, buf, size).then([this, bufptr=std::move(bufptr)](auto size){
            return this->sync_data(size);
        }).finally([this](){
            _write_flush_semaphore.signal(); 
        });
    });
}

future<volume_revision> segment_commitlog::read_commitlog(revision_index index){
    logger.debug("segment_commitlog read_commitlog testpengjiang index:{}", index);
    file_open_options opt;
    sstring file_name = _path + "/" + _segment_id;
    return open_checked_file_dma(general_disk_error_handler, file_name, open_flags::ro, opt).then(
            [index](auto fd){
        auto size = index.serialize_length;
        auto offset = index.offset_in_segment;
        auto align_size = align_up(size, 512UL);
        auto align_offset = align_up(offset, 512UL);

        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        std::memset(bufptr.get(), 0, align_size);
        auto buf = bufptr.get();

        return fd.dma_read(align_offset, buf, align_size).then_wrapped([bufptr = std::move(bufptr)](auto f) mutable{
            try{
                auto read_size = f.get0();
                logger.debug("segment_commitlog do_read_data read_size:{}", read_size);
                volume_revision revision;
                revision.deserialize(bufptr);
                logger.debug("segment_commitlog do_read_data extent_revison:{}", revision);
                return make_ready_future<volume_revision>(std::move(revision));
            }catch(...){
                std::ostringstream out;
                out << "[read_file] error";
                out << ", exception:" << std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());
                return make_exception_future<volume_revision>(std::runtime_error(error_info));
            }

        }).finally([fd]()mutable{
            fd.close().finally([fd]{});
        });
    });
}
#if 0
future<std::unique_ptr<char[], free_deleter>>
segment_commitlog::read_commitlog_for_drain(){
    uint64_t align_size = align_up(_offset, 512UL);
    uint64_t align_offset = align_up(uint64_t(0), 512UL);

    auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
    auto buf = bufptr.get();
    return _fd.dma_read(align_offset, buf, align_size).then_wrapped([this, align_size, bufptr = std::move(bufptr)](auto f) mutable{
        try{
            auto read_size = f.get0();
            assert(align_size == read_size);
            return make_ready_future<std::unique_ptr<char[], free_deleter>>(std::move(bufptr));
        }catch(...){
            std::ostringstream out;
            out << "[read_file] error";
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<std::unique_ptr<char[], free_deleter>>(std::runtime_error(error_info));           
        }
    });
}
#endif

future<std::unique_ptr<char[], free_deleter>>
segment_commitlog::read_commitlog_for_drain(){
    sstring file_name = _path + "/" + _segment_id;
    file_open_options opt;
    return open_checked_file_dma(general_disk_error_handler, file_name, open_flags::ro, opt).then([this](auto fd){
        uint64_t align_size = align_up(_offset, 512UL);
        uint64_t align_offset = align_up(uint64_t(0), 512UL);

        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        auto buf = bufptr.get();

        return fd.dma_read(align_offset, buf, align_size).then_wrapped([this, align_size, bufptr = std::move(bufptr)](auto f) mutable{
            try{
                auto read_size = f.get0();
                assert(align_size == read_size);
                return make_ready_future<std::unique_ptr<char[], free_deleter>>(std::move(bufptr));
            }catch(...){
                std::ostringstream out;
                out << "[read_commitlog_for_drain] error";
                out << ", exception:" << std::current_exception();
                auto error_info = out.str();
                logger.error(error_info.c_str());
                return make_exception_future<std::unique_ptr<char[], free_deleter>>(std::runtime_error(error_info));           
            }
        }).finally([fd]()mutable{
            fd.close().finally([fd](){}); 
        });
    });
}

future<> segment_commitlog::discard_commitlog(){
    logger.debug("segment_commitlog::discard_commitlog start volume_id:{}, segment_id:{}, path:{}"
        , _volume_id, _segment_id, _path);
    _can_delete = true;
    return make_ready_future<>();
}

uint64_t segment_commitlog::get_offset(){
   return _offset;
}

void  segment_commitlog::set_offset(uint64_t offset){
    _offset = offset;
}

bool segment_commitlog::should_flush(){
    return _offset >= _max_size;
    //return true;
}

// =======================================================================
// memtable
// =======================================================================
future<lw_shared_ptr<segment_memtable>>
segment_memtable::make_segment_memtable(sstring volume_id){
    return make_ready_future<lw_shared_ptr<segment_memtable>>(make_lw_shared<segment_memtable>(volume_id));
}

segment_memtable::segment_memtable(sstring volume_id)
    :_volume_id(volume_id){}

future<> segment_memtable::insert_index(sstring extent_id, revision_index index){
    auto itor = _index_map.find(extent_id);
    if(itor != _index_map.end()){
        itor->second.push_back(index); 
    }else{
        std::vector<revision_index> revision_indexs = {index};
        auto ret = _index_map.insert(std::make_pair(extent_id, std::move(revision_indexs)));
        assert(ret.second);
    }

    //auto itor_1 = _group_index_map.find(extent_group_id);
    //if(itor_1 != _group_index_map.end()){
    //    itor_1->second.push_back(index); 
    //}else{
    //    std::vector<revision_index> revision_indexs = {index};
    //    auto ret = _group_index_map.insert(std::make_pair(extent_group_id, std::move(revision_indexs)));
    //    assert(ret.second);
    //}

    //for new drain add;
    _order_indexs.push_back(index);

    return make_ready_future<>();
}

//extent_id level
future<std::vector<revision_index>> segment_memtable::get_indexs(sstring extent_id){
    logger.debug(" segment_memtable::get_indexs level extent_id:{}", extent_id);
    std::vector<revision_index> revision_indexs;

    auto it = _index_map.find(extent_id);
    if(it != _index_map.end()){
        revision_indexs = it->second;
    }
    return make_ready_future<std::vector<revision_index>>(std::move(revision_indexs));
}

//future<std::vector<revision_index>> 
//segment_memtable::get_indexs_by_extent_group_id(sstring extent_group_id){
//    std::vector<revision_index> revision_indexs;
//    auto it = _group_index_map.find(extent_group_id);
//    if(it != _group_index_map.end()){
//        revision_indexs = it->second;
//    }
//    return make_ready_future<std::vector<revision_index>>(std::move(revision_indexs));
//}

//memtable level
future<std::vector<revision_index>> segment_memtable::get_indexs(){
    std::vector<revision_index> revision_indexs;
    for(auto it = _index_map.begin(); it != _index_map.end(); it++){
        auto indexs = it->second;
        for(auto index : indexs){
            revision_indexs.emplace_back(index);
        }
    }
    return make_ready_future<std::vector<revision_index>>(std::move(revision_indexs));
}

bool segment_memtable::empty() {
    return _index_map.empty();
}


void segment_memtable::rollback_for_sync(uint64_t offset){
    if (offset == 0)
       return;
    auto index = _order_indexs.back();
    auto last_offset = index.offset_in_segment + index.serialize_length;
    if(last_offset == offset)
        return;

    //not test
    logger.error("rollback when sync_secondary_commitlog ");
    logger.error("rollback ..... offset:{}, last_offset:{}, index_offset:{}, index_serialize_length:{}",
        offset, last_offset, index.offset_in_segment, index.serialize_length);
    auto extent_id = index.extent_id; 
    auto it = _index_map.find(extent_id);
    if(it != _index_map.end()){
        auto index = it->second.back();
        assert(last_offset > offset);
        it->second.pop_back();
        _order_indexs.pop_back();
        return; 
    }
    logger.error("segment_memtable::rollback_memtable for rollback can not find extent_id, extent_id:{}, offset:{}"
        , extent_id, offset);
    assert(false);
}


void segment_memtable::rollback_for_rebuild(uint64_t vclock){
    auto last_index = _order_indexs.back();
    auto last_vclock = last_index.vclock;
    if(last_vclock == vclock)
        return;

    assert(last_vclock >= vclock);
    auto extent_id = last_index.extent_id; 
    auto it = _index_map.find(extent_id);
    if(it != _index_map.end()){
        auto& index = it->second.back();
        assert(last_index == index);
        it->second.pop_back();
        _order_indexs.pop_back();
        return; 
    }else{
        logger.error("segment_memtable::rollback_memtable for rebuild can not find extent_id, extent_id:{}, offset:{}"
            , extent_id, vclock);
        assert(false);
    }
}

}//hive


