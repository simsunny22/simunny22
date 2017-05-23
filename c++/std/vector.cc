#include <vector>
#include <iostream>
#include <string>


using namespace std;

class stu{
public:
   string name;
   stu(string name){
        cout << "construct name:" << name << endl;
   }

   ~stu(){
       cout << "destory name:" << name << endl;
   }
};



int main(){
    vector<int> stus;

    auto i = stus.back();
    std::cout  << "test:" << i <<endl;//segment fault

    

    stus.push_back(3);
    stus.push_back(1);
    stus.push_back(4);



    for(auto& it: stus){
       cout << "it:" << it <<endl;
    }  
    
}
