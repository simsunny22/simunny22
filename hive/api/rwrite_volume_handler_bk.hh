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

class rwrite_volume_handler: public httpd::handler_base {
private:
    sstring get_header_value(header_map& headers, sstring header_key, sstring default_value="");
    bool                      check_is_access_trail_test(request& req);
    hive_access_trail_command build_access_trail_command(request& req);
    hive_write_command        build_write_command(request& req);
    void check_write_command(hive_write_command& write_cmd);

    future<std::unique_ptr<reply>> redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip);

    future<std::unique_ptr<reply>> rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                                         , hive_write_command write_cmd);

    future<std::unique_ptr<reply>> pseudo_rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                                                 , hive_write_command write_cmd
                                                                 , hive_access_trail_command access_trial_cmd);

    future<> maybe_learn_extent_group_context(sstring extent_group_id
                                            , sstring disk_ids
                                            , uint64_t vclock);

    future<> trace_access_trail(sstring disk_ids
                              , sstring extent_group_id
                              , sstring node_id
                              , uint64_t size
                              , sstring options);

    sstring build_return_json(int64_t vclock);
    
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
};


}//namespace hive
