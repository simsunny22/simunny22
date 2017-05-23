
#include <string>
#include <iostream>


using namespace std;

void test_key(string);
//void test_key(const char* key);
int main(){
   
    int i =10;
    while(i > 0){
       cout <<"i:" << i << endl;
       i? i--: i;
    }
    cout <<"i:" << i << endl;

}



void test_key(string key){
    cout << "test_key:" <<  key << endl;
}


//void test_key(const char* key){
//   cout << "test_key...:" << key <<endl;
//   test_key(string(key));
//}
