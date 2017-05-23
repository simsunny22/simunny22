#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>

#include "types.hh"
#include "hive/hive_request.hh"
#include "hive/api/volume_reader.hh"

namespace hive{
using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class create_log_table_handler: public httpd::handler_base {
private:
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 
            
}; 

}//namespace hive
