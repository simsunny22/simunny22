#include "set_test.hh"
namespace bi = boost::intrusive;


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
    Foo* foo_object = new Foo("wuzhao", 26);
    fooset fset;
    fset.insert(*foo_object);

    Foo* dst = new Foo();
    new(dst) Foo(std::move(*foo_object));
    
    //new (static_cast<T*>(dst)) T(std::move(*src_t));


    auto it = fset.find("wuzhao", Foo::compare());

    if(it != fset.end()){
        it->print();
    }else{
       std::cout << "not find " << std::endl;
    }
    
    
    std::cout << "size:" << fset.size() << std::endl;


    //FooList1::s_iterator_to(*foo_object);
}

