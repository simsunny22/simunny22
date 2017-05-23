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

class read_and_rwrite_volume_handler_test: public httpd::handler_base {

private:
    lw_shared_ptr<hive_read_command> build_read_cmd(request& req);

    void check_write_command(hive_write_command& write_cmd);
    hive_write_command build_write_cmd(request& req);
    
    future<std::unique_ptr<reply>> rwrite_by_volume_stream(std::unique_ptr<reply> rep
                                                         , hive_write_command write_cmd);

    future<std::unique_ptr<reply>> redirect_rwrite(std::unique_ptr<reply> rep, sstring redirect_ip);
    sstring build_return_json(int64_t vclock);
public:
    future<std::unique_ptr<reply> > handle(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;

    future<std::unique_ptr<reply> > read_test(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep);

    future<std::unique_ptr<reply> > write_test(const sstring& path,
    std::unique_ptr<request> req, std::unique_ptr<reply> rep);
};


}//namespace hive
