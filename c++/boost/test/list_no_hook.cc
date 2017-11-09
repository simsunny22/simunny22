#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <iostream>
#include "list_no_hook.hh"
namespace bi = boost::intrusive;


Foo::Foo(){
    std::cout << "create" << std::endl;
    list.push_back(*this);
}

Foo::~Foo(){
    std::cout << "destroy" << std::endl;
    list.erase(list.iterator_to(*this));
}

void Foo::print(){
   std::cout << "test" << std::endl;
}



int main(){
    //Foo foo_object;
    Foo* foo_object = new Foo();
    Foo* foo_object1 = new Foo();
    std::cout << "size:" << list.size() << std::endl;

    //FooList list;
    //FooList1 list1;
    //list.push_back(*foo_object);
    //list1.push_back(*foo_object);

    std::cout<< "11111111111111111111" << std::endl;
    //auto i = list.iterator_to(*foo_object);
    //i->~Foo();
    //auto j = list.iterator_to(*foo_object1);
    //j->~Foo();
    for(auto& f: list){
       f.print();
       f.~Foo();
    }


    //std::cout << "size:" << list.size() << std::endl;

    //delete foo_object;
    std::cout<< "2222222222222222222" << std::endl;
    std::cout << "size:" << list.size() << std::endl;
}

