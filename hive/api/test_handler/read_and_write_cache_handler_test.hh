#pragma once

#include "http/httpd.hh"
#include "http/handlers.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <map>
#include <unordered_map>

#include "types.hh"
#include "hive/hive_request.hh"
#include "hive/extent_revision_set.hh"

namespace hive{

using namespace httpd;
using header_map = std::unordered_map<sstring, sstring>;

class read_and_write_cache_handler_test: public httpd::handler_base {
private:
    void check_read_cmd(lw_shared_ptr<hive_read_command> read_cmd);
    lw_shared_ptr<hive_read_command> build_read_cmd(request& req);
    void check_write_cmd(hive_write_command& write_cmd);
    hive_write_command build_write_cmd(request& req);
    extent_revision_set prepare_extent_revision(hive_write_command write_cmd, std::vector<std::tuple<sstring, sstring, size_t, size_t>>& remove_cache_params);
public:
    future<std::unique_ptr<reply>> handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 
    future<std::unique_ptr<reply>> remove_cache(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep);
    future<std::unique_ptr<reply>> read_cache(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep);
            
}; 

}//namespace hive
