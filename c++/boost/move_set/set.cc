#include "set.hh"

Foo:: Foo(Foo&& o) noexcept
     //: hook_(std::move(o.hook_))
     : hook_()
     , _name(std::move(o._name))
     , _age(std::move(o._age))
{
    std::cout << "create move " << std::endl;
    {   
        using container_type = fooset;
        container_type::node_algorithms::replace_node(o.hook_.this_ptr(), hook_.this_ptr());
        container_type::node_algorithms::init(o.hook_.this_ptr());
    }


}


int main(){
    Foo* src = new Foo("wuzhao", 26);
    fooset fset;
    fset.insert(*src);

    Foo* dst = new Foo();
    new (dst) Foo(std::move(*src));
    src->~Foo();
    //delete dst;

    auto it = fset.find("wuzhao", Foo::compare());
    if(it != fset.end()){
        it->print();
    }else{
       std::cout << "not find " << std::endl;
    }

    std::cout << "size:" << fset.size() << std::endl;
}

