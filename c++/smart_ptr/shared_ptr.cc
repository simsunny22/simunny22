#include<memory>
#include<iostream>

using namespace std;
int main(){
    shared_ptr<bool> res = std::make_shared<bool>();

    //*res = *res|true;
    cout << *res << std::endl;
    
}
