#include <vector>
#include <map>
#include <iostream>
#include <string>
#include <boost/intrusive/unordered_set.hpp>

//reference
//http://blog.csdn.net/ac_huang/article/details/38361263

using namespace std;


class stu {
public:
    stu(string name_, int age_):
      name(name_),
      age(age_){
    }
    string name;
    int age;
};

//for map and set
void test_key_delete(){
    //1. init map
    map<string, int> test;
    test["wz"] = 25;
    test["wd"] = 25;
    test["ch"] = 26;
    test["dn"] = 24;

    cout << "---------------iteor begin ----------------------" << endl;
    for(auto& it: test ){
           cout << "name:" << it.first << ", age:" << it.second << endl;
    }

    //2. delete method1
    for(auto it = test.begin(); it != test.end(); ){
        if(it->second % 2 == 0){
           //test.erase(it);
           test.erase(it++); //important
        }else{
           ++it;             //important
        }
    }

    //3. deltete method2
    for(auto it = test.begin(); it != test.end(); it ++){
        if(it->second % 2 == 0){
           test.erase(it); //important
        }
    }

    cout << "---------------iteor end ----------------------" << endl;
    for(auto& it: test ){
           cout << "name:" << it.first << ", age:" << it.second << endl;
    }
}


void test_map_insert(){
    map<string, int > test;
    //test.insert(std::make_pair("wz", 25));
    //test.insert(std::make_pair("wz", 26));

    test.emplace("wz", 25);
    test.emplace("wz", 26);

    auto it = test.find("wz");
    cout << "age:" << it->second << endl;
    
}

void test_map_pointer_insert(){
    map<string, stu*> test;
    //test.insert(std::make_pair("wz", 25));
    //test.insert(std::make_pair("wz", 26));
    


    test.insert(std::make_pair("wz", new stu("wz", 26)));
    test.insert(std::make_pair("wd", new stu("wd", 26)));


    cout << "before m ........................" <<endl;
    for(auto& it : test){
        cout << "name:" << it.second->name << ", age:" << it.second->age << endl;
    }

    auto itor = test.find("wz");
    itor->second->age = 27;
    
    cout << "after m ........................" <<endl;
    for(auto& it : test){
        cout << "name:" << it.second->name << ", age:" << it.second->age << endl;
    }

    
}

// g++ -std=c++11 map.cc
int main(){
    test_map_pointer_insert();
    //cout << "********************** vector delete ****************************" << endl;
    //test_delete();
    //cout << "********************** map delete ****************************" << endl;
    //test_key_delete();
}
