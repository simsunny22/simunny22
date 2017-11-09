#include <set>
#include <iostream>
#include <string>


using namespace std;

class stu{
public:
   string name;
   int age;
   
   stu(string name_, int age_):name(name_),age(age_){}

   friend inline bool operator > (const stu &l, const stu &r){
       return l.name > r.name;
   }
   friend inline bool operator < (const stu &l, const stu &r){
       return l.name < r.name;
   }
   friend inline bool operator == (const stu &l, const stu &r){
       return l.name == r.name;
   }


   struct compare{
       bool operator()(const stu& l, const stu& r){
           return l.name < r.name; 
       }
       bool operator()(const stu& l, const string& r){
           return l.name == r; 
       }
       bool operator()(const string& l, const stu& r){
           return l == r.name; 
       }
   };

};

int main(){
    std::set<stu> set_c;
    set_c.insert(stu("wuzhao", 25));
    std::cout << "size:" << set_c.size() << std::endl;
    set_c.insert(stu("wudong", 26));
    std::cout << "size:" << set_c.size() << std::endl;
    set_c.insert(stu("wudong", 27));
    std::cout << "size:" << set_c.size() << std::endl;
    auto it = set_c.find(stu("wuzhao", 27));
    if(it != set_c.end()){
       std::cout << "111111111111111111" << std::endl;
       std::cout << "name:" << it->name << ",age:" << it->age << std::endl;
    }

    std::cout << "-----------------------------------" << std::endl;

    std::set<stu, stu::compare> set_c1;
    set_c1.insert(stu("wuzhao", 25));
    std::cout << "size:" << set_c1.size() << std::endl;
    set_c1.insert(stu("wudong", 26));
    std::cout << "size:" << set_c1.size() << std::endl;
    set_c1.insert(stu("wudong", 27));
    std::cout << "size:" << set_c1.size() << std::endl;

    auto it1 = set_c1.find(stu("wudong", 34));
    if(it1 != set_c1.end()){
       std::cout << "111111111111111111" << std::endl;
       std::cout << "name:" << it1->name << ",age:" << it1->age << std::endl;
    }

    //set_c1.find("wuzhao", stu::compare());
}
