#include "hive/cache/stream_cache.hh" 

#include <iostream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/align.hh"
#include <unistd.h>
#include <boost/intrusive_ptr.hpp>

#include "db/config.hh"
#include "log.hh"


#include "hive/cache/cache.hh" 

namespace hive {
distributed<stream_cache> _stream_cache;

stream_cache::stream_cache(const db::config& cfg)
: _cfg(std::make_unique<db::config>(cfg)){
}

stream_cache::~stream_cache(){
}


future<> stream_cache::stop() {
     return make_ready_future<>();
}

//
//cache with seq (rename version) functions
//
future<lw_shared_ptr<datum>> stream_cache::get(sstring key){
    return do_cache_get(key).then([this, key](auto entity_ptr){
        if (!entity_ptr) {
            datum data(seq++, datum_origin::CACHE);
            lw_shared_ptr<datum> data_ptr = make_lw_shared<datum>(std::move(data));
            return this->do_cache_set(key, data_ptr).then([this, data_ptr](auto ret)mutable{
                data_ptr->origin = datum_origin::POPULATE;
                return make_ready_future<lw_shared_ptr<datum>>(data_ptr);
            });
        }else{
            return make_ready_future<lw_shared_ptr<datum>>(entity_ptr);
        }
    });
}

future<bool> stream_cache::patch(sstring key, lw_shared_ptr<datum_differ> differ){
    assert(differ->data->length() == differ->length);
    return do_cache_get(key).then([this, key, differ](auto datum_ptr){
        if (datum_ptr) {
            datum_ptr->seq = seq++;
            if(datum_ptr->data){
               memcpy(datum_ptr->data->begin() + differ->offset, differ->data->begin(), differ->length);
            }
            return this->do_cache_set(key, datum_ptr);
        }
        return make_ready_future<bool>(true);

    });
}

future<bool> stream_cache::set(sstring key, lw_shared_ptr<datum> data){
    return do_cache_get(key).then([this, key, data](auto entity_ptr){
        if (entity_ptr){
            uint64_t src_seq = entity_ptr->seq;
            if(src_seq == data->seq){
                 return this->do_cache_set(key, data);
            }else{
                 return this->do_cache_del(key);
            }
        }
        return make_ready_future<bool>(true);
    });
}

//
//cache inner functions
//
future<bool> stream_cache::do_cache_set(sstring key, lw_shared_ptr<datum> data){
    auto& cache= hive::get_local_cache();
    item_insertion_data insertion;
    insertion.key = item_key{key}; 
    temporary_buffer<char> tmp_data = data->serialize();
    insertion.data = std::move(tmp_data);
    insertion.version = 0U; 
    return cache.set(insertion);
}

future<lw_shared_ptr<datum>> stream_cache::do_cache_get(sstring key){
    auto& cache = hive::get_local_cache();
    item_key item_key(key);
    return cache.get(item_key).then([this, key](auto entity_ptr){
        if (!entity_ptr) {
            return make_ready_future<lw_shared_ptr<datum>>(nullptr); 
        }else{
            datum data;
            data.deserialize(entity_ptr->datum().begin(), entity_ptr->datum().size());
            lw_shared_ptr<datum> data_ptr = make_lw_shared<datum>(std::move(data));
            return make_ready_future<lw_shared_ptr<datum>>(std::move(data_ptr));
        }
    });
}

future<bool> stream_cache::do_cache_del(sstring key){
    auto& cache = hive::get_local_cache();
    item_key item_key(key);
    return cache.remove(item_key);
}

} //namespace hive 

