#include<iostream>

using namespace std;

class Base {
public:
    void f(int i){
        cout << "Base::f(int)" << endl;
    }

    void f(int i1, int i2){
        cout << "Base::f(int, int)" << endl;
    }

    virtual void g(int i){
        cout << "Base::g(int)" << endl;
    }
    
    virtual void g(int i1, int i2){
        cout << "Base::g(int, int)" << endl;
    }

    virtual void h(int i){
        cout << "Base::h(int)" << endl;
    }

    void j(int i){
        cout << "Base::j(int)" << endl;
    }



};


class Derived : public Base{
public:
    void f(int i){
        cout << "Derived::f(int)" << endl;
    }

    virtual void g(int i){
        cout << "Derived::g(int)" << endl;
    }

    virtual void h(int){
        cout << "Derived::h(int)" << endl;
    }
    
};

/*
隐藏的规则
(1)如果派生类的函数与基类的函数同名，但是参数不同。此时，不论有无virtual关键字，基类的函数将被隐藏（注意别与重载混淆）。
(2)如果派生类的函数与基类的函数同名，并且参数也相同，但是基类函数没有virtual关键字。此时，基类的函数被隐藏（注意别与覆盖混淆）。
*/

int main(){
    Derived d;
    Base*    pBase = &d;
    Derived* pDerived = &d;

    pBase->f(1);
    pBase->f(1, 1);
    pDerived->f(1);
    //pDerived->f(1,1); //子类隐藏了父类的所有f方法, h(int)满足隐藏的规则1和规则2 
    cout << "=====================================" << endl;

    pBase->g(1);
    pBase->g(1,1);
    pDerived->g(1);
    //pDerived->g(1,1); //子类隐藏了父类的所有f方法, virtual g(int)满足隐藏的规则1
    cout << "=====================================" << endl;

    pBase->h(1);    //覆盖 子类覆盖了父类的方法
    pDerived->h(1); 
    cout << "=====================================" << endl;

    pBase->j(1);
    pDerived->j(1); //继承 子类继承了父类的方法


}
