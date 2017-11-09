#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <string>
#include <iostream>
namespace bi = boost::intrusive;


//using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;

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

    ~Foo(){
        std::cout << "destroy" << std::endl;
    }

    void print(){
        std::cout  << "foo, value:" << _age  << std::endl;
    }
 
   //friend bool operator< (const Foo &a, const Foo &b)
   //   {  return a._age < b._age;  }
   //friend bool operator> (const Foo &a, const Foo &b)
   //   {  return a._age > b._age;  }
   friend bool operator == (const Foo &a, const Foo &b)
      {  return a._name == b._name;  }

   struct compare{
   public:
       inline bool operator () (const Foo& l, const Foo& r) const {
           return l._name == r._name;
       }
       
       inline bool operator () (const Foo& f, const std::string& s) const {
           std::cout << "11111 f_name:" << f._name << std::endl;
           std::cout << "11111 s:" << s << std::endl;
           //auto res = f._name == s;
           auto res = f._name > s;
           std::cout << "1111 res:" << res << std::endl;
           return res;
       }

       inline bool operator () (const std::string& s, const Foo& f) const {
           std::cout << "222 res:" << f._name << std::endl;
           auto res =  f._name < s;
           std::cout << "222 res:" << res << std::endl;
           return res;
           //return f._name == s;
       }
       
   };

   //friend bool operator< (std::string name, const Foo &a)
   //   {  return a._name < name;  }
   //friend bool operator> (std::string name, const Foo &a)
   //   {  return a._name > name;  }
   //friend bool operator== (std::string name, const Foo &a)
   //   {  return a._name < name;  }
};

typedef bi::member_hook<Foo, bi::set_member_hook<bi::link_mode<bi::auto_unlink>>, &Foo::hook_> MemberHookOption;

typedef bi::set<Foo, MemberHookOption, bi::constant_time_size<false>,  bi::compare<Foo::compare>> fooset;

struct delete_disposer
{
   void operator()(Foo *delete_this){  
       delete delete_this; 
   }
};

int main(){
    Foo* foo_object = new Foo("wuzhao", 26);
    fooset fset;
    //fooset fset();
    fset.insert(*foo_object);
    auto it = fset.find("wuzhao", Foo::compare());

    if(it != fset.end()){
        it->print();
    }else{
       std::cout << "not find " << std::endl;
    }
    std::cout << "size:" << fset.size() << std::endl;
    //FooList1::s_iterator_to(*foo_object);
}

