#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <iostream>
namespace bi = boost::intrusive;


//using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;

class Foo{
public:
    bi::list_member_hook<> hook_;
    bi::list_member_hook<> hook1_;
    //bi::set_member_hook<bi::link_mode<bi::auto_unlink>> hook2_;


Foo(){
    std::cout << "create" << std::endl;
}
~Foo(){
    std::cout << "destroy" << std::endl;
}
};

class test{
    bi::set_member_hook<> hook1_;
};

typedef bi::member_hook<Foo, bi::list_member_hook<>, &Foo::hook_> MemberHookOption;
typedef bi::member_hook<Foo, bi::list_member_hook<>, &Foo::hook1_> MemberHookOption1;
//typedef bi::member_hook<Foo, bi::list_member_hook<bi::link_mode<bi::auto_unlink>>, &Foo::hook_> MemberHookOption;
//typedef bi::member_hook<Foo, bi::list_member_hook<bi::link_mode<bi::auto_unlink>>, &Foo::hook1_> MemberHookOption1;

typedef bi::list<Foo, MemberHookOption, bi::constant_time_size<false>> FooList;
typedef bi::list<Foo, MemberHookOption1, bi::constant_time_size<false>> FooList1;

struct delete_disposer
{
   void operator()(Foo *delete_this){  
       delete delete_this; 
   }
};

int main(){
    //Foo foo_object;
    Foo* foo_object = new Foo();
    FooList list;
    //FooList1 list1;
    list.push_back(*foo_object);
    //list1.push_back(*foo_object);
    std::cout<< "11111111111111111111" << std::endl;
    list.pop_back();
    delete foo_object;
    std::cout<< "2222222222222222222" << std::endl;
    std::cout << "size:" << list.size() << std::endl;
    //list1.pop_back();
    //list.pop_back_and_dispose(delete_disposer());
    std::cout << "size:" << list.size() << std::endl;
    //assert(&list.front() == &foo_object);
}

