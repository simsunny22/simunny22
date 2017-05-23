#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>

#include "types.hh"
#include "hive/hive_request.hh"
#include "hive/trail/trail_service.hh"

namespace hive{
using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class write_access_trail_handler: public httpd::handler_base {
private:
   trail_data& make_trail_data(std::unique_ptr<request>& req, trail_data& data);
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 
            
}; 

}//namespace hive
