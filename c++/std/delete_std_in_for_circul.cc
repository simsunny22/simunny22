#include <vector>
#include <map>
#include <iostream>
#include <string>
#include <boost/intrusive/unordered_set.hpp>

//reference
//http://blog.csdn.net/ac_huang/article/details/38361263

using namespace std;

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

void test_delete(){
    vector<int> test;
    test.push_back(28);   //wz
    test.push_back(26);   //wd
    test.push_back(27);   //ch
    test.push_back(25);   //dn

    cout << "---------------iteor begin ----------------------" << endl;
    for(auto& it: test ){
           cout << "name:" << it <<  endl;
    }

    for(auto it = test.begin(); it != test.end(); ){
        cout << "it:" << *it << endl;
        if(*it % 2 ==0){
             test.erase(it);  //when erase, it auto ++, so not need it++ in for;
        }else{
            ++it; //for not implement it ++,so we need ++it 
        }
        
    }

    #if 0
    // this is error, because earse(it) will auto it ++, but 'in for' we it++ again
    for(auto it = test.begin(); it != test.end(); it++){  //when erase, it auto ++, so not need it++ in for;
        cout << "it:" << *it << endl;
        if(*it % 2 ==0){
        cout << "===========start =================" <<endl;
        cout << "it:" << *it << endl;
             test.erase(it);  //when erase, it auto ++, so not need it++ in for;
        cout << "it:" << *it << endl;
        cout << "===========end =================" <<endl;
        }
    }
    #endif

    cout << "---------------iteor end ----------------------" << endl;
    for(auto& it: test ){
           cout << "name:" << it <<  endl;
    }
  
}



// g++ -std=c++11 delete_std_in_for_circul.cc
int main(){
    cout << "********************** vector delete ****************************" << endl;
    test_delete();
    cout << "********************** map delete ****************************" << endl;
    test_key_delete();
}
