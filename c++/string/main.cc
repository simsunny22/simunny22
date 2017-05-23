#include <iostream>
#include <string>
#include <string.h>

using namespace std;

int main(){

   char a[3] = {'a', 'b', 'c'};
   char b[4] = "abc";
   cout << "a:" << a << ", len:" << strlen(a)<<endl;
   cout << "b:" << b << ", len:" << strlen(b)<<endl;


   char* c;
   c = a;
   cout << "c_a:" << c << endl;


   c = b;
   cout << "c_b:" << c << endl;
}
