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
using request_params = std::map<sstring, data_value>;

class rwrite_volume_handler_test_need_body: public httpd::handler_base {
private:
    bool check_is_access_trail_test(request& req);
    hive_access_trail_command build_access_trail_command(request& req);
    void check_write_command(hive_write_command& write_cmd);
    hive_write_command build_write_command(request& req);

    future<std::unique_ptr<reply>> rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                                         , hive_write_command write_cmd);

    future<std::unique_ptr<reply>> redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip);
    sstring build_return_json(int64_t vclock);
    future<std::unique_ptr<reply>> pseudo_rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                                                 , hive_write_command write_cmd
                                                                 , hive_access_trail_command access_trial_cmd);
    future<> trace_access_trail(sstring disk_ids
                              , sstring extent_group_id
                              , sstring node_id
                              , uint64_t size
                              , sstring options);
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
};


}//namespace hive
