#include <iostream>
#include <functional>
#include <string>


using namespace std;
class test{
public:
   string name;
   int age;

   test(){
       name = "wz";
       age = 25;
   }

   void test_print(){
       std::cout << "wztest" << endl;
   }

   //void test_print(int test){
   //   std::cout << "wztest1111" << endl;
   //}

   //void test_print(int test1, int test2){
   //    std::cout << "wztest2222" << endl;
   //}
};


void test_fun(){
   std::cout << "test_fun111111" << endl;
}


int main(){
    test test1;
    
    auto func1 = std::bind(&test::test_print, &test1);

    auto func2 = std::bind(test_fun);

    func1();

    func2();
    
}
