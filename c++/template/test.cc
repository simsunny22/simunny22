//g++ -std=c++11 test.cc
#include <iostream>



template<typename T>
T sum(T t){
    return t;
}

template<typename T, typename ... Types>
T sum (T first, Types ... rest){
    //return first + sum<T>(rest...);
    return first + sum(rest...);
}





using namespace std;

int main(){
    auto res = sum(1,2,3,4); //10
    std::cout << "res:" << res <<std::endl;


}
