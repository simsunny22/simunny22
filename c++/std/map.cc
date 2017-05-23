#include <vector>
#include <map>
#include <iostream>
#include <string>
#include <boost/intrusive/unordered_set.hpp>

using namespace std;

class stu{
public:
   string name;
   stu(string name){
        cout << "construct name:" << name << endl;
        this->name = name;
   }


   stu(const stu& stu){
       cout << "construct 11111111111111" << endl;
       name = stu.name;
   }

   ~stu(){
       cout << "destory name:" << name << endl;
   }
};

void test_earse(){
   map<string , int> test;
   test.insert(std::pair<string ,int>("wz", 25));
   test.insert(std::pair<string ,int>("wd", 25));
   test.insert(std::pair<string ,int>("ch", 26));
   test.insert(std::pair<string ,int>("dn", 24));

   auto it_none = test.find("wztest");
   if(it_none != test.end()){
       cout << "22222222222222222222222222" << endl;
       test.erase(it_none);
   }


   for(auto& it: test){
      cout << "name:" << it.first << endl;
   }

   cout << "1111111111111111111111111" << endl;

   auto it = test.find("wz");
   for(auto begin = test.begin(); begin != it; ++begin){
      cout << "name:" << begin->first << endl;

   }
   cout << "1111111111111111111111111" << endl;
   //test.erase(it);
   test.erase(test.begin(), ++it);

   for(auto& it: test){
      cout << "name:" << it.first << endl;
   }

   cout << "1111111111111111111111111" << endl;
}

int main(){
    vector<stu> stus;
    stu stu1("wz");
    stu stu2("wd");
    stu stu3("aa");
    stus.push_back(stu1);
    stus.push_back(stu2);
    stus.push_back(stu3);
    cout << "szie1111:" << stus.size() << endl;


    for(auto& it: stus){
        cout << "stu:" << it.name << endl;
    }

    stus.erase(stus.begin());
    cout << "sziei2222:" << stus.size() << endl;


    test_earse();
    
    
}
