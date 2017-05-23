#include"hive/response_handler.hh"

namespace hive {

void test_func(){
    //auto data= std::make_shared<int>(1); 
    //auto data = std::make_shared<abstract_md>();
    union_md data;
    std::unordered_set<gms::inet_address> targets; 

    //common_response_handler<int> handler(0, data, targets);
    common_response_handler handler(0, data, targets);

    auto response_id = handler.response_id();
    std::cout << "test response_id:" << response_id << std::endl;
}



}//namespace hive
