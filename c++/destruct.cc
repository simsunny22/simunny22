#include<iostream>

using namespace std;

class child1{
public:
child1(){
    std::cout << "child1 construct ....." << std::endl;
}
~child1(){
    std::cout << "child1 destruct ......" << std::endl;
}

};

class child2{
public:
child2(){
    std::cout << "child2 construct ....." << std::endl;
}
~child2(){
    std::cout << "child2 destruct ......" << std::endl;
}

};

class father{
public:
    child1 c1;
    child2 c2;

~father(){
    // dont do that this
    //c1.~child1(); 
    //c2.~child2();
    std::cout << "father destruct ......" << std::endl;
}

};

int main(){
   father f;
}
