#include "http_client_test_handler.hh"
#include <cstdlib>
#include <iostream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/lexical_cast.hpp>
#include "hive/hive_service.hh"
#include "hive/http/seawreck.hh"

namespace hive{


future<std::unique_ptr<reply> > http_client_test_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    
    std::cout << "handle start =========" << std::endl;
    hive::HttpClient client;
    sstring metadata_uri = "http://10.100.123.69:9000/v1/actions/ping_wuzhao";
    return do_with(std::move(client), [metadata_uri, rep=std::move(rep)](auto& client)mutable{
        return client.post(metadata_uri, "").then_wrapped([rep=std::move(rep)](auto f)mutable{
            try {
                auto response = f.get0();
                if(!response.is_success()){
                    std::ostringstream out;
                    out << "response error code:" << response.status();
                    auto error_info = out.str();
                    throw std::runtime_error(error_info); 
                }

                std::cout << "handle success =========" << std::endl;
                sstring ret = "success";
                rep->_content = ret;
                rep->done("text");
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            } catch (...) {
                std::cout << "handle error =========, exception:" << std::current_exception() << std::endl;
                throw std::runtime_error("error");
            }
        });
    });    
}


}//namespace hive
