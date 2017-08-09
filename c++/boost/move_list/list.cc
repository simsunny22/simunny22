#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <iostream>
#include <string>
#include "list.hh"


Foo::Foo(Foo&& o) noexcept
     //: hook_(std::move(o.hook_))
     : hook_()
     , name(std::move(o.name))
     , age(std::move(o.age))
{
    std::cout << "move 22222 " << std::endl;
    if (o.hook_.is_linked()) {
        auto prev = o.hook_.prev_;
        o.hook_.unlink();
        FooList::node_algorithms::link_after(prev, hook_.this_ptr());
    }
}


int main(){
    FooList list;
    Foo* src = new Foo("wuzhao", 26);
    list.push_back(*src);
    std::cout << "size:" << list.size() << std::endl;

    Foo* dst = new Foo();
    new (dst) Foo(std::move(*src));
    src->~Foo();
    //delete src;
    std::cout << "size:" << list.size() << std::endl;

    list.pop_back_and_dispose(print_disposer());
}

