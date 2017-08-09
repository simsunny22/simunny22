#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <string>
#include <iostream>
namespace bi = boost::intrusive;


class Foo{
    std::string _name;
    int _age;
public:
    bi::set_member_hook<bi::link_mode<bi::auto_unlink>> hook_;
     
    Foo(){
        std::cout << "create" << std::endl;
    }

    //Foo(std::string name):_name(name){
    Foo(std::string name, int age):
        _name(name),
        _age(age){

        std::cout << "create params" << std::endl;
    }

    Foo(Foo&& o) noexcept;


    ~Foo(){
        std::cout << "destroy" << std::endl;
    }

    void print(){
        std::cout  << "foo, _name:" << _name  << std::endl;
        std::cout  << "foo, _age:" << _age  << std::endl;
    }

   friend bool operator == (const Foo &a, const Foo &b)
      {  return a._name == b._name;  }

   struct compare{
   public:
       inline bool operator () (const Foo& l, const Foo& r) const {
           return l._name == r._name;
       }
       
       inline bool operator () (const Foo& f, const std::string& s) const {
           auto res = f._name > s;
           return res;
       }

       inline bool operator () (const std::string& s, const Foo& f) const {
           auto res =  f._name < s;
           return res;
       }
       
   };

};

typedef bi::member_hook<Foo, bi::set_member_hook<bi::link_mode<bi::auto_unlink>>, &Foo::hook_> MemberHookOption;

typedef bi::set<Foo, MemberHookOption, bi::constant_time_size<false>,  bi::compare<Foo::compare>> fooset;

struct delete_disposer
{
   void operator()(Foo *delete_this){  
       delete delete_this; 
   }
};

//int main(){
//    Foo* foo_object = new Foo("wuzhao", 26);
//    fooset fset;
//    fset.insert(*foo_object);
//
//    Foo* dst = new Foo();
//    new(dst) Foo(std::move(*foo_object));
//    
//    //new (static_cast<T*>(dst)) T(std::move(*src_t));
//
//
//    auto it = fset.find("wuzhao", Foo::compare());
//
//    if(it != fset.end()){
//        it->print();
//    }else{
//       std::cout << "not find " << std::endl;
//    }
//    
//    
//    std::cout << "size:" << fset.size() << std::endl;
//
//
//    //FooList1::s_iterator_to(*foo_object);
//}

