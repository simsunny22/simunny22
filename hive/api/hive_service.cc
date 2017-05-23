/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "hive/api/hive_service.hh"
#include "hive/api/api-doc/hive_service.json.hh"
#include "api/api-doc/utils.json.hh"
#include "hive/hive_service.hh"
#include <iostream>
#include <vector>

#include "core/temporary_buffer.hh"

namespace hive {
extern hive::hive_status hive_service_status;
}

namespace api {
using namespace json;
using namespace httpd;

//callback by scylla service
void set_hive_service(http_context& ctx, routes& r) {

    httpd::hive_service_json::start.set(r, [] (std::unique_ptr<request> req) {
        return seastar::async([req = std::move(req)]()mutable{
            if(hive::hive_service_status == hive::hive_status::IDLE){
                hive::get_hive_service().start().get();
                hive::get_hive_service().invoke_on(0, [](auto& hive_service){
                    return hive_service.init(); 
                }).get();
            }
        }).then([] {
            return make_ready_future<json::json_return_type>("ok");
        });

    });

    httpd::hive_service_json::stop.set(r, [] (std::unique_ptr<request> req) {
        return seastar::async([req = std::move(req)]()mutable{
            if(hive::hive_service_status == hive::hive_status::NORMAL){
                hive::get_hive_service().invoke_on(0, [](auto& hive_service){
                    return hive_service.close_services(); 
                }).get();
            }
        }).then([] {
            return make_ready_future<json::json_return_type>("ok");
        });
    });

  
} //set_hive_service
} //namespace api
