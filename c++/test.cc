#include <iostream>

using namespace std;

class stu{
public:
   int age1;
   int age2;

   //stu(){
   //   age1 = 3;
   //   age2 = 3;
   //}

   stu(int a1, int a2){
      age1 = a1;
      age2 = a2;
   }

   void print(){
      cout << "age1:" << age1 << endl;
      cout << "age2:" << age2 << endl;
   }
};

void test_move(){
   stu stu1(3,3);
   stu1.print();
   std::cout << std::endl;

   stu1.age1 = 1;
   stu1.age2 = 2;
   stu1.print();
   std::cout << std::endl;

   stu stu2(stu1);
   stu2.print();
   std::cout << std::endl;

   stu stu3 = std::move(stu1);
   stu3.print();
}

int main(){
   uint16_t i = 256;
   uint8_t j = i;
   //uint8_t j =reinterpret_cast<uint8_t>(i);

   uint8_t i1 = 255;
   uint16_t j1 =i1;

   cout << "j:" << j << endl;
   cout << "j1:" << j1 << endl;

}



