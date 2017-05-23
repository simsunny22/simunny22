#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>
#include "types.hh" 

namespace hive{

using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;
using request_params = std::map<sstring, data_value>;

class read_extent_group_handler: public httpd::handler_base {
private:
    lw_shared_ptr<request_params> parse_read_params(request& req);

    //future<std::unique_ptr<reply>> read(std::unique_ptr<request> req, std::unique_ptr<reply> rep);

public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 
            
}; 

}//namespace hive
