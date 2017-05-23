#include "ping_handler.hh"
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

namespace hive{
extern hive::hive_status hive_service_status;

long getCurrentTime()    
{    
     struct timeval tv;    
     gettimeofday(&tv,NULL);    
     return tv.tv_sec * 1000 + tv.tv_usec / 1000;    
}    

std::string ltos(long l)  
{  
    std::ostringstream os;  
    os<<l;  
    std::string result;  
    std::istringstream is(os.str());  
    is>>result;  
    return result;  
}

future<std::unique_ptr<reply> > ping_handler::handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep){
    std::string time = ltos(getCurrentTime());
    //switch (hive_service_status){
    //   case hive::hive_status::NORMAL:
    //       status = "normal";
    //       break;
    //   case hive::hive_status::IDLE:
    //       status = "idle";
    //       break;
    //   case hive::hive_status::BOOTING:
    //       status = "booting";
    //       break;
    //   
    //}
    //std::cout << "system time:" << time << std::endl;
    sstring ret = "pong, current_time:" + sstring(time);
    rep->_content = ret;
    rep->done("text");
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}


}//namespace hive
