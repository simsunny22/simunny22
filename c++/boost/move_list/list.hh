#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <iostream>
#include <string>
namespace bi = boost::intrusive;


class Foo{
public:
    bi::list_member_hook<bi::link_mode<bi::auto_unlink>> hook_;
    std::string name;
    int age;

Foo(){
    std::cout << "create 0000" << std::endl;
}

Foo(std::string name_, int age_){
    this->name = name_;
    this->age = age_;
    std::cout << "create 1111" << std::endl;
}

Foo(Foo&& o) noexcept;


~Foo(){
    std::cout << "destroy" << std::endl;
}

void print(){
    std::cout << "foo, name:" << name << std::endl;
    std::cout << "foo, age:" << age << std::endl;
}

};

typedef bi::member_hook<Foo, bi::list_member_hook<bi::link_mode<bi::auto_unlink>>, &Foo::hook_> MemberHookOption;
typedef bi::list<Foo, MemberHookOption, bi::constant_time_size<false>> FooList;

struct delete_disposer
{
   void operator()(Foo *delete_this){  
       delete delete_this; 
   }
};
struct print_disposer
{
   void operator()(Foo* foo){  
       foo->print();
   }
};


