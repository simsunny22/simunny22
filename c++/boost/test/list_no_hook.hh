#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <iostream>
namespace bi = boost::intrusive;



class Foo{
public:
    bi::list_member_hook<> hook_;
    bi::list_member_hook<> hook1_;

    Foo();
    ~Foo();
    void print();
};


typedef bi::member_hook<Foo, bi::list_member_hook<>, &Foo::hook_> MemberHookOption;
typedef bi::member_hook<Foo, bi::list_member_hook<>, &Foo::hook1_> MemberHookOption1;

typedef bi::list<Foo, MemberHookOption, bi::constant_time_size<false>> FooList;
typedef bi::list<Foo, MemberHookOption1, bi::constant_time_size<false>> FooList1;


FooList list;

struct delete_disposer
{
   void operator()(Foo *delete_this){  
       delete delete_this; 
   }
};


