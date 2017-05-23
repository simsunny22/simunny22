#include "log.hh"
#include "hive/file_store.hh"
#include "core/seastar.hh"
#include "core/thread.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include <core/fstream.hh>
#include <seastar/core/enum.hh>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include <core/align.hh>
#include <cstdint>
#include <iostream>
#include <fstream>
#include <experimental/optional>
#include "gms/inet_address.hh"
#include "message/messaging_service.hh"
#include "disk-error-handler.hh"
#include "checked-file-impl.hh"

#include "hive/hive_tools.hh"
#include "hive/hive_config.hh"


namespace hive{

static logging::logger logger("file_store");

future<> create_file(sstring path, size_t truncate_size, bool create_new){
    logger.debug("[{}] start, path:{}, truncate_size:{}", __func__, path, truncate_size);
    return seastar::async([path, truncate_size, create_new](){
        bool is_exists = file_exists(path).get0(); 
        if(is_exists && !create_new){
            return; 
        }

        // 1. delete old file
        if(is_exists){
            delete_file(path).get();
        }

        // 2. create and trucate new file
        do_create_file(path, truncate_size).get();
    }).then_wrapped([path, truncate_size](auto fut){
        try {
            fut.get(); 
            logger.debug("[create_file] done, path:{}, truncate_size:{}", path, truncate_size);
            return make_ready_future<>(); 
        }catch (...){
            std::ostringstream out;
            out << "[create_file] error";
            out << ", path:" << path << ", truncate_size:" << truncate_size;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
	  });
}

future<> do_create_file(sstring path, size_t truncate_size){
    file_open_options opt;
    opt.extent_allocation_size_hint = truncate_size;
    return open_checked_file_dma(general_disk_error_handler, path, open_flags::wo|open_flags::create, opt)
            .then([truncate_size](file fd){
        return fd.truncate(truncate_size).finally([fd]()mutable{ 
            fd.close().finally([fd]{});  
        }); 
    });
}

future<> delete_file(sstring path){
    logger.debug("[{}] start, path:{}", path);
    return file_exists(path).then([path](auto is_exists){
        if(!is_exists) {
            //todo: print use error level for debug
            logger.error("[delete_file] warn, file is not exists, but return ok, path:{}", path);    
            return make_ready_future<>(); 
        }else {
            return remove_file(path).then_wrapped([path](auto fut){
                try {
                    fut.get(); 
                    logger.debug("[delete_file] done, path:{}", path);
                    return make_ready_future<>(); 
                }catch (...){
                    std::ostringstream out;
                    out << "[delete_file] error, path:" << path;
                    out << ", exception:" << std::current_exception();
                    auto error_info = out.str();
                    logger.error(error_info.c_str());
                    return make_exception_future<>(std::runtime_error(error_info));
                }
	          });
        }
    });
}

future<> copy_file(sstring from_file, sstring to_file){
    logger.debug("[{}] start, from_file:{}, to_file:{}", __func__, from_file, to_file);

    try{
        std::ifstream in;
        std::ofstream out;

        in.open(from_file, std::ios::binary);
        if(in.fail()){
            in.close();
            auto error_info = sprint("can not open file:%s", from_file);
            throw std::runtime_error(error_info);
        }

        out.open(to_file, std::ios::binary);
        if(out.fail()){
            in.close();
            out.close();
            auto error_info = sprint("can not create file:%s", to_file);
            throw std::runtime_error(error_info);
        }

        out << in.rdbuf();
        in.close();
        out.close();
        return make_ready_future<>();
    }catch(...){
        std::ostringstream out; 
        out << "[copy_file] error";
        out << ", from_file:" << from_file << ", to_file:" << to_file;
        out << ", exception:" << std::current_exception();
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<>(std::runtime_error(error_info));
    }
}


future<bytes> build_align_data(sstring path, size_t read_offset, size_t read_length
                     , size_t replay_offset, bytes& old_data){
    return read_file_ex(path, read_offset, read_length).then(
            [replay_offset, &old_data](bytes align_data)mutable{

        std::memcpy(align_data.begin()+replay_offset, old_data.begin(), old_data.size());
        return make_ready_future<bytes>(std::move(align_data));
    });
}

future<> write_file(sstring path, size_t offset, size_t size, bytes& data){
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL);

    if(align_offset != offset || align_size != size){
        size_t new_offset = align_down(offset, 512UL);
        size_t length = offset - new_offset + size;
        size_t new_size = align_up(length, 512UL);

        return build_align_data(path, new_offset, new_size, offset-new_offset, data).then(
                [path, new_offset, new_size](bytes align_data){

            return do_with(std::move(align_data), [path, new_offset, new_size]
                    (auto& align_data){
                return write_file_align(path, new_offset, new_size, align_data);
            });
        });
    }else {
        return write_file_align(path, offset, size, data);
    }
}

/////////////
//write file
/////////////
future<> write_file_align(sstring path, size_t offset, size_t size, bytes& data){
    logger.debug("[{}] start, path:{}, offset:{}, size:{}, data.size():{}"
        , __func__, path, offset, size, data.size()); 

    //1. params check
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL); 
    if(path.empty() || offset != align_offset || size != align_size || size != data.size()){
        std::ostringstream out;
        out << "[write_file] error, params invalid";
        out << "path can not empty, offset add size must align to 512, size must equal data.size()";
        out << ", path:" << path << ", offset:" << offset << ", size:" << size << ", data.size():" << data.size();
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<>(std::runtime_error(error_info));
    }

    //2. do write
    return open_file_dma(path, open_flags::wo).then([path, align_offset, align_size, &data](auto fd){
        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        auto buf = bufptr.get();
        //std::memset(buf, 0, align_size);
        std::copy_n(data.begin(), align_size, buf);
        return fd.dma_write(align_offset, buf, align_size).then(
                [fd, path, align_offset, align_size, bufptr=std::move(bufptr)](auto write_size)mutable{
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
    }).then_wrapped([path, offset, size](auto fut){
        try{
            fut.get();
            logger.debug("[write_file] done, path:{}, offset:{}, size:{}", path, offset, size);
            return make_ready_future<>();
        }catch(...){
            std::ostringstream out;
            out << "[write_file] error";
            out << ", path:" << path << ", offset:" << offset << ", size:" << size;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}

future<> write_file_ex(sstring path, size_t offset, size_t size, std::unique_ptr<char[], free_deleter> bufptr){
    logger.debug("[{}] start, path:{}, offset:{}, size:{}", __func__, path, offset, size);
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL); 
    if(offset != align_offset || size != align_size || path.empty() ) {
        std::ostringstream out;
        out << "[write_file_ex] error, params invalid";
        out << ", path can not is empty, offset add size must align to 512";
        out << ", path:" << path <<  ", offset:" << offset << ", size:" << size;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<>(std::runtime_error(error_info));
    }

    return open_file_dma(path, open_flags::wo).then(
            [path, align_offset, align_size, bufptr=std::move(bufptr)](auto fd)mutable{
        auto buf = bufptr.get();
        return fd.dma_write(align_offset, buf, align_size).then(
                [fd, path, align_offset, align_size, bufptr=std::move(bufptr)](auto write_size)mutable{
            if(write_size != align_size){
                std::ostringstream out;
                out << "[write_file_ex] error";
                out << ", need write size:" << align_size << ", not equal actually write size:" << write_size;
                auto error_info = out.str();
                throw std::runtime_error(error_info);
            }
            return fd.flush();
        }).finally([fd]()mutable{
            fd.close().finally([fd] {});  
        });

    }).then_wrapped([path, offset, size](auto fut){
        try{
            fut.get();
            logger.debug("[write_file_ex] done, path:{}, offset:{}, size:{}", path, offset, size);
            return make_ready_future<>(); 
        }catch(...){
            std::ostringstream out;
            out << "[write_file_ex] error";
            out << ", path:" << path << ", offset:" << offset << ", size:" << size;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<>(std::runtime_error(error_info));
        }
    });
}
////////////////////////////
// for write_file_batch
////////////////////////////
future<std::unique_ptr<char[], free_deleter>> 
read_align_data_for_batch_write(file fd, size_t align_offset, size_t align_size){
    auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
    auto buf = bufptr.get();
    return fd.dma_read(align_offset, buf, align_size).then(
                [bufptr=std::move(bufptr), align_size](auto read_size)mutable{
        if(read_size != align_size){
            std::ostringstream out;
            out << "[read_align_data_for_batch_write] error";
            out << "need_size:" << align_size << ", but actually read size:" << read_size;
            auto error_info = out.str();
            throw std::runtime_error(error_info);
        }
        return make_ready_future<std::unique_ptr<char[], free_deleter>>(std::move(bufptr));
    });
}

future<> do_write_for_batch(
    file fd
    , std::unique_ptr<char[], free_deleter> bufptr
    , size_t align_offset
    , size_t align_size){

    auto buf = bufptr.get();
    return fd.dma_write(align_offset, buf, align_size).then(
            [bufptr=std::move(bufptr), align_size](auto write_size)mutable{
        if(write_size != align_size){
            std::ostringstream out;
            out << "[write_file_for_batch] error";
            out << ", need write size:" << align_size << ", not equal actually write size:" << write_size;
            auto error_info = out.str();
            throw std::runtime_error(error_info);
        }
    });
}

future<> write_file_batch(sstring file_path, std::vector<revision_data> revision_datas){
    return open_file_dma(file_path, open_flags::rw).then([revision_datas=std::move(revision_datas)](auto fd)mutable{
        return do_with(std::move(revision_datas), [fd](auto& revision_datas)mutable{
            return do_for_each(revision_datas.begin(), revision_datas.end(), [fd](auto& revision_data)mutable{
                auto size = revision_data.length;
                auto offset = revision_data.offset_in_extent_group;
                
                auto align_offset = align_up(offset, 512UL);
                auto align_size   = align_up(size, 512UL);

                if(align_offset == offset && align_size == size){
                    auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
                    auto buf = bufptr.get();
                    std::memcpy(buf, revision_data.data.begin(), align_size);
                    return do_write_for_batch(fd, std::move(bufptr), align_offset, align_size);
                }else{
                    //need build align_data by read 
                    size_t new_align_offset = align_down(offset, 512UL);
                    size_t new_size = offset - new_align_offset + size;
                    size_t new_align_size = align_up(new_size, 512UL);
                    size_t replay_offset = offset-new_align_offset;

                    return read_align_data_for_batch_write(fd, new_align_offset, new_align_size).then(
                            [fd, new_align_offset, new_align_size, replay_offset, &revision_data]
                            (auto&& bufptr)mutable{
                        auto buf = bufptr.get(); 
                        std::memcpy(buf+replay_offset, revision_data.data.begin(), revision_data.data.size());
                        return do_write_for_batch(fd, std::move(bufptr), new_align_offset, new_align_size);
                    });
                }
            });
        }).then([fd]()mutable{
            return fd.flush(); 
        }).finally([fd]()mutable{
            fd.close().finally([fd](){});
        });    
    });
}


/////////////
//read file
/////////////
future<temporary_buffer<char>> read_file(sstring path, size_t offset, size_t size){
    logger.debug("[{}] start, path:{}, offset:{}, size:{}", __func__, path, offset, size);
    //1. params check
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL); 
    if(offset != align_offset || size != align_size || path.empty() ) {
        std::ostringstream out;
        out << "[read_file] error";
        out << ", params invalid, offset add size must align with 512, and path can not empty";
        out << ", path:" << path << ", offset:" << offset << ", size:" << size;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<temporary_buffer<char>>(std::runtime_error(error_info));
    }

    //2. do read
    return open_file_dma(path, open_flags::ro).then([path, align_offset, align_size](auto fd){
        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        auto buf = bufptr.get();
        //std::memset(buf, 0, align_size);
        return fd.dma_read(align_offset, buf, align_size).then(
                [bufptr=std::move(bufptr),path, align_offset, align_size](auto read_size){
            if(read_size != align_size){
                std::ostringstream out;
                out << "[read_file] error";
                out << "need_size:" << align_size << ", but actually read size:" << read_size;
                out << ", file_path:" << path;
                auto error_info = out.str();
                throw std::runtime_error(error_info);
            }
 
            temporary_buffer<char> data = temporary_buffer<char>(align_size);
      	    std::copy_n(bufptr.get(), align_size, data.get_write());
            return std::move(data);
        }).finally([fd]()mutable{
            fd.close().finally([fd] {});
        });
   }).then_wrapped([path, offset, size](auto fut){
        try{
            auto&& data = fut.get0();
            logger.debug("[read_file] done, path:{}, offset:{}, size:{}", path, offset, size);
            return make_ready_future<temporary_buffer<char>>(std::move(data)); 
        }catch(...){
            std::ostringstream out;
            out << "[read_file] error";
            out << ", path:" << path << ", offset:" << offset << ", size:" << size;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<temporary_buffer<char>>(std::runtime_error(error_info));
        }
    });

}//read_file

future<bytes> read_file_ex(sstring path, size_t offset, size_t size){
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL);

    if(align_offset != offset || align_size != size){
        size_t new_offset = align_down(offset, 512UL);
        size_t length = offset - new_offset + size;
        size_t new_size = align_up(length, 512UL);

        return read_file_ex_align(path, new_offset, new_size).then(
                [size, offset, new_offset](auto align_data){
            size_t split_offset = offset - new_offset;
            bytes data_buf(size, 0);
            std::memcpy(data_buf.begin(), align_data.begin()+split_offset, size);
            return std::move(data_buf);
        });
    }else {
        return read_file_ex_align(path, offset, size);
    }
}

future<bytes> read_file_ex_align(sstring path, size_t offset, size_t size){
    logger.debug("[{}] start, path:{}, offset:{}, size:{} ", __func__, path, offset, size);
    auto align_offset = align_up(offset, 512UL);
    auto align_size   = align_up(size, 512UL); 
    if(offset != align_offset || size != align_size || path.empty() ) {
        std::ostringstream out;
        out << "[read_file_ex] error";
        out << ", params invalid, offset add size must align with 512, and path can not empty";
        out << ", path:" << path << ", offset:" << offset << ", size:" << size;
        auto error_info = out.str();
        logger.error(error_info.c_str());
        return make_exception_future<bytes>(std::runtime_error(error_info));
    }

    return open_file_dma_ex(path, open_flags::ro).then([path, align_offset, align_size](auto fd){
        auto bufptr = allocate_aligned_buffer<char>(align_size, 512UL);
        auto buf = bufptr.get();
        //std::memset(buf, 0, align_size);
        return fd.dma_read(align_offset, buf, align_size).then(
                [bufptr=std::move(bufptr), path, align_offset, align_size](auto read_size)mutable{
            if(read_size != align_size){
                std::ostringstream out;
                out << "[read_file_ex] error";
                out << "need_size:" << align_size << ", but actually read size:" << read_size;
                out << ", file_path:" << path;
                auto error_info = out.str();
                throw std::runtime_error(error_info);
            }

            //covert to bytes
            auto read_buf = bufptr.get(); 
            bytes data_buf(align_size, 0);
            std::memcpy(data_buf.begin(), read_buf, align_size);
            return std::move(data_buf);
        }).finally([fd]()mutable{
            fd.close().finally([fd] {});
        });
   }).then_wrapped([path, offset, size](auto fut)mutable{
        try{
            auto&& data_buf = fut.get0(); 
            logger.debug("[read_file_ex] done, path:{}, offset:{}, size:{} ", path, offset, size);
            return make_ready_future<bytes>(std::move(data_buf)); 
        }catch(...){
            std::ostringstream out;
            out << "[read_file_ex] error";
            out << ", path:" << path << ", offset:" << offset << ", size:" << size;
            out << ", exception:" << std::current_exception();
            auto error_info = out.str();
            logger.error(error_info.c_str());
            return make_exception_future<bytes>(std::runtime_error(error_info));
        }
    });
} //read_file_ex


}//namespace hive
