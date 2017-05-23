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

class read_volume_handler: public httpd::handler_base {
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override; 

private:
    sstring get_header_value(header_map& headers, sstring header_key, sstring default_value="");
    bool                             check_is_access_trail_test(request& req);
    hive_access_trail_command        build_access_trail_command(request& req);
    lw_shared_ptr<hive_read_command> build_read_cmd(request& req);
    void check_read_cmd(lw_shared_ptr<hive_read_command> read_cmd);

    future<std::unique_ptr<reply>> redirect_read(std::unique_ptr<reply> rep, sstring redirect_ip);
    future<std::unique_ptr<reply>> pseudo_read(std::unique_ptr<reply> rep
                                             , lw_shared_ptr<hive_read_command> read_cmd 
                                             , hive_access_trail_command access_trail_cmd);
    future<> trace_access_trail(sstring disk_ids
                              , sstring extent_group_id
                              , sstring node_id
                              , uint64_t size
                              , sstring options);
}; 

}//namespace hive
