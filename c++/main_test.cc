#include <iostream>

using namespace std; 
class test{
  int i;
};

struct test1{
};

class test2{
public:
   virtual ~test2() {}
};
int main(){
   cout << "class:" <<  sizeof(test) << std::endl;
   cout << "struct:" <<  sizeof(test1) << std::endl;
   cout << "virtual:" <<  sizeof(test2) << std::endl;
}
