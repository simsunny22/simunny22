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

class rwrite_volume_handler_test: public httpd::handler_base {
private:
    sstring get_header_value(header_map& headers, sstring header_key, sstring default_value="");

    hive_write_command build_write_command(request& req);

    future<std::unique_ptr<reply>> redirect_rwrite(
        std::unique_ptr<reply> rep
        , sstring redirect_ip);

    future<std::unique_ptr<reply>> rwrite_by_volume_stream(
        std::unique_ptr<reply> rep
        , hive_write_command write_cmd);

    sstring build_return_json();


    future<std::unique_ptr<reply>> test(
        std::unique_ptr<reply> rep
        , hive_write_command write_cmd);

public:
    future<std::unique_ptr<reply>> 
    handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
};


}//namespace hive
