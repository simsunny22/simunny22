#include "memcached_service.hh"

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
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "utils/latency.hh"
#include "utils/flush_queue.hh"
#include "atomic_cell.hh"
#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"
#include "net/byteorder.hh"
#include "utils/UUID_gen.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include "bytes.hh"
#include <chrono>
#include <functional>
#include <cstdint>
#include "hive/hive_service.hh"

using namespace std::chrono_literals;

namespace memcache {

__thread slab_allocator<item>* slab;

distributed<memcached_service> _the_memcached_service;
using namespace exceptions;

memcached_service::memcached_service() {
    auto cfg = hive::get_local_hive_service().get_hive_config();
    uint64_t per_cpu_slab_size = cfg->memcache_per_cpu_slab_size();
    uint64_t slab_page_size = cfg->memcache_slab_page_size();;
    _cache = make_lw_shared<cache>(per_cpu_slab_size, slab_page_size);
}

memcached_service::~memcached_service(){}

future<>
memcached_service::stop() {
    return make_ready_future<>();
}

future<> memcached_service::flush_all() {
    return _the_memcached_service.invoke_on_all([this] (auto& memcached_cache) {
        return memcached_cache._cache->flush_all();
    });
}

future<> memcached_service::flush_at(uint32_t time) {
    return _the_memcached_service.invoke_on_all([this, time] (auto& memcached_cache) {
        return memcached_cache._cache->flush_at(time);
    });
}

clock_type::duration memcached_service::get_wc_to_clock_type_delta() {
    return _the_memcached_service.local()._cache->get_wc_to_clock_type_delta();
}

// interfaces
future<bool> memcached_service::set(item_insertion_data insertion) {
    auto cpu = get_cpu(insertion.key);
    //std::cout << "set key: " << insertion.key.key() << " cpu: " << cpu << " cpuid: " << engine().cpu_id() << std::endl;
    if (engine().cpu_id() == cpu) {
        return make_ready_future<bool>(_the_memcached_service.local()._cache->set(std::move(insertion)));
    }
    return smp::submit_to(cpu, [insertion=std::move(insertion)] () mutable {
        return _the_memcached_service.local()._cache->set<remote_origin_tag>(std::move(insertion));
    });
}

future<bool> memcached_service::add(item_insertion_data insertion) {
    auto cpu = get_cpu(insertion.key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<bool>(_the_memcached_service.local()._cache->add(std::move(insertion)));
    }
    return smp::submit_to(cpu, [insertion=std::move(insertion)] () mutable {
        return _the_memcached_service.local()._cache->add<remote_origin_tag>(std::move(insertion));
    });
}

future<bool> memcached_service::replace(item_insertion_data insertion) {
    auto cpu = get_cpu(insertion.key);
    if (engine().cpu_id() == cpu) {
        return make_ready_future<bool>(_the_memcached_service.local()._cache->replace(std::move(insertion)));
    }
    return smp::submit_to(cpu, [insertion=std::move(insertion)] () mutable {
        return _the_memcached_service.local()._cache->replace<remote_origin_tag>(std::move(insertion));
    });
}

future<bool> memcached_service::remove(item_key key) {
    auto cpu = get_cpu(key);
    return _the_memcached_service.invoke_on(cpu, [this, key=std::move(key)] (auto& memcached_cache) mutable {
        return memcached_cache._cache->remove(std::move(key));
    });
}

future<item_ptr> memcached_service::get(item_key key) {
    auto cpu = get_cpu(key);
//    std::cout << "key: " << key.key() << " cpu: " << cpu << " cpuid: " << engine().cpu_id() << std::endl;
    return _the_memcached_service.invoke_on(cpu, [this, key=std::move(key)] (auto& memcached_cache) mutable {
        return memcached_cache._cache->get(std::move(key));
    });
}

future<cas_result> memcached_service::cas(item_insertion_data insertion, item::version_type version) {
   auto cpu = get_cpu(insertion.key);
   if (engine().cpu_id() == cpu) {
       return make_ready_future<cas_result>(_the_memcached_service.local()._cache->cas(std::move(insertion), version));
   }
   return smp::submit_to(cpu, [insertion=std::move(insertion), version=std::move(version)] () mutable {
       return _the_memcached_service.local()._cache->cas<remote_origin_tag>(std::move(insertion), std::move(version));
   });
}

future<std::pair<item_ptr, bool>> memcached_service::incr(item_key key, uint64_t delta) {
   auto cpu = get_cpu(key);
   if (engine().cpu_id() == cpu) {
       return make_ready_future<std::pair<item_ptr, bool>>(
           _the_memcached_service.local()._cache->incr(std::move(key), delta));
   }
   return smp::submit_to(cpu, [key=std::move(key), delta=std::move(delta)] () mutable {
       return _the_memcached_service.local()._cache->incr<remote_origin_tag>(std::move(key), std::move(delta));
   });
}

future<std::pair<item_ptr, bool>> memcached_service::decr(item_key key, uint64_t delta) {
   auto cpu = get_cpu(key);
   if (engine().cpu_id() == cpu) {
       return make_ready_future<std::pair<item_ptr, bool>>(
           _the_memcached_service.local()._cache->decr(std::move(key), delta));
   }
   return smp::submit_to(cpu, [key=std::move(key), delta=std::move(delta)] () mutable {
       return _the_memcached_service.local()._cache->decr<remote_origin_tag >(std::move(key), std::move(delta));
   });
}

} //namespace hive
