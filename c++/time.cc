#include<chrono>
#include<iostream>
#include<string>




int main(){
    std::chrono::system_clock::time_point time_point = std::chrono::system_clock::now();
    uint64_t l_time = time_point.time_since_epoch().count();
    std::string s_time = std::to_string(l_time); 
    std::cout << "time:" << time_point.time_since_epoch().count() << std::endl; 
}
