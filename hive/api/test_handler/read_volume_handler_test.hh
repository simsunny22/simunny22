#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>

#include "types.hh"
#include "hive/api/volume_reader.hh"
#include "hive/hive_request.hh"

namespace hive{

using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class read_volume_handler_test: public httpd::handler_base {
private:
    lw_shared_ptr<hive_read_command> build_read_cmd(request& req);
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 
            
}; 

}//namespace hive
