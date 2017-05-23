#include "memcache_handler.hh"
#include <cstdlib>
#include <iostream>

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
#include "hive/hive_tools.hh"
#include "hive/hive_service.hh"
#include "hive/memcache/memcached_service.hh"
#include "log.hh"

namespace hive{

static logging::logger logger("memcache-handler");


static sstring get_header_value(sstring header_key, header_map& headers){
    header_map::const_iterator itor = headers.find(header_key);

    sstring header_value = "";
    if( itor != headers.end() ){
        header_value = itor->second;
    }

    return header_value;
}

future<std::unique_ptr<reply> > memcache_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    std::cout << "memcache handler run" << std::endl;

    header_map& headers = req->_headers;

    sstring operation = get_header_value("X-Operation", headers);
    sstring key       = get_header_value("X-Key", headers);
    sstring value     = get_header_value("X-Value", headers);
    sstring expiry    = get_header_value("X-Expiry", headers);

    // params validate
    if (operation == ""  || (operation != "set" && operation != "get" && operation != "remove")) {
        rep->_content = "operation is wrong.";
        rep->done("text");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
    if (key == "") {
        rep->_content = "key is not specify";
        rep->done("text");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
    if (operation == "set" && value == "") {
        rep->_content = "value is not specify for set opertion";
        rep->done("text");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
    

    // set key value
    if (operation == "set") {
        return make_ready_future<>().then([key_=std::move(key), value_=std::move(value), expiry=std::move(expiry)] () {
            auto& memcached = memcache::get_local_memcached_service();

            memcache::item_insertion_data insertion;
            memcache::item_key key(key_);
            insertion.key = std::move(key);
            insertion.data = temporary_buffer<char>(value_.c_str(), value_.length());

            if (expiry != "") {
                int32_t timeout = hive::hive_tools::str_to_int32(expiry);
                std::cout << "timeout is: " << timeout << std::endl;
                memcache::expiration expiration(memcached.get_wc_to_clock_type_delta(), timeout);
                insertion.expiry = expiration;
            }

            //set value
            return memcached.set(std::move(insertion));
        }).then([rep=std::move(rep)] (bool existed) mutable {
            rep->_content = "ok";
            rep->done("text");
            return std::move(rep);
        });
    } else if (operation == "get") {
        return make_ready_future<>().then([key_=std::move(key)] () {
            auto& memcached = memcache::get_local_memcached_service();
            memcache::item_key key(key_);
            return memcached.get(key);
        }).then([rep=std::move(rep)] (auto itemptr) mutable {
            if (itemptr) {
                rep->_content = itemptr->value().get();
            } else {
                rep->_content = "not found";
            }
            rep->done("text");
            return std::move(rep);
        });

    } else {
        return make_ready_future<>().then([key_=std::move(key)] () {
            auto& memcached = memcache::get_local_memcached_service();
            memcache::item_key key(key_);
            return memcached.remove(key);
        }).then([rep=std::move(rep)] (auto founded) mutable {
            if (founded) {
                rep->_content = "removed";
            } else {
                rep->_content = "not found";
            }
            rep->done("text");
            return std::move(rep);
        });

    }
}
}//namespace hive
