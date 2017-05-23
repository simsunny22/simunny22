#include<iostream>
#include<vector>

using namespace std;

// g++ -std=c++11 test_brace.cc 

vector<int> get_v_int_1(){
    //only c++11
    return {1,2,3};
}

vector<int> get_v_int_2(){
    //only c++11
    return {{1,2,3}};
}

void print_v_int(vector<int>& v_int){
    for(vector<int>::iterator it = v_int.begin(); it != v_int.end(); it++){
        cout << *it << endl;
    }
}

int main(){

    vector<int> v_int_1 = get_v_int_1();
    vector<int> v_int_2 = get_v_int_2();

    print_v_int(v_int_1);
    cout << "====================" << endl;
    print_v_int(v_int_2);
}
