#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>

#include "types.hh"
#include "hive/hive_request.hh"

namespace hive{
using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class read_object_handler: public httpd::handler_base {
private:
    sstring get_header_value(header_map& headers, sstring header_key, sstring default_value="");
    hive_read_command build_read_command(request& req);
public:
    future<std::unique_ptr<reply> > handle(
        const sstring& path
        , std::unique_ptr<request> req
        , std::unique_ptr<reply> rep) override; 
            
}; 

}//namespace hive
