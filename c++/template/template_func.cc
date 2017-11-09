#include <iostream>

using namespace std;


template<class Type>
Type test_fun(Type i1, Type i2){
    return i1 + i2;
}

//template<class Type , class int = 10>
//Type test_fun1(Type i1, int i2){
//    return i1;
//}


class test{
private:
    string _name;
    int _age;

public:
    test(string name, int age):_name(name), _age(age){}
    test():_name(""),_age(0){}
    void print(){
        cout << "name:" << _name  << endl;
        cout << "age:" << _age << endl;
    }
};

template<class Name, class Age>
class test_template{
private:
    Name _name;
    Age _age;

public:
    test_template(Name name, Age age):_name(name), _age(age){};
    test_template(){};
    void print(){
        cout << "name:" << _name  << endl;
        cout << "age:" << _age << endl;
    }
};


test test_class(){

    return test();
}



int main(){
    std::cout << "test:"<< test_fun<double>(1, 2) << std::endl;
    test_class();

    test_template<string, int> a;
    a = test_template<string, int>("wuzhao", 26);
    a.print();

    a = test_template<string, int>();
    a.print();
}


