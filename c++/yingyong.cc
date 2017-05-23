
#include<iostream>

using namespace std;
int& test(int& i){
    return i;
}

int test1(int i){
   return i;
}

int main(){
    int i =1;
    int i1 = test(i);
    int& i2 = test(i);

    i =2;
    cout << "i :" <<   i << endl;
    cout << "i1 :" <<  i1 << endl;
    cout << "i2 :" <<  i2 << endl;

    cout << "================================" << endl;

    int j = 1;
    int j1 = test1(j);
    int& j2 = test1(j);
    j= 2;

    cout << "j :" <<   j << endl;
    cout << "j1 :" <<  j1 << endl;
    cout << "j2 :" <<  j2 << endl;
}
