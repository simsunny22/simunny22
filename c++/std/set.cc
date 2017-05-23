#include <set>
#include <iostream>
#include <string>
#include <tuple>

using namespace std;

class stu{
public:
   string name;
   stu(string name){
        cout << "construct name:" << name << endl;
   }

   ~stu(){
       cout << "destory name:" << name << endl;
   }
};


class extent_entry{
public:
   string  extent_id;
   int  extent_offset_in_group;

   bool operator < (const extent_entry& other){
       if(extent_id == other.extent_id) 
           return extent_offset_in_group < other.extent_offset_in_group;
       return extent_id < other.extent_id;
   }   

   bool operator > (const extent_entry& other){
       if(extent_id == other.extent_id) 
           return extent_offset_in_group > other.extent_offset_in_group;
       return extent_id > other.extent_id;
   }   

   bool operator = (const extent_entry& other){
       return extent_id == other.extent_id &&  
              extent_offset_in_group == other.extent_offset_in_group;
   }   
};


extent_entry make_extent_entry(string id, int offset){
    extent_entry entry;
    entry.extent_id = id;
    entry.extent_offset_in_group = offset;
    return entry;
}

using extent_entry_set = std::set<std::tuple<string, int>>;

int main(){
    set<int> stus;

    stus.insert(3);
    stus.insert(1);
    stus.insert(4);

    stus.insert(stus.begin(),5);


    for(auto& it: stus){
       cout << "it:" << it <<endl;
    }  

    extent_entry entry1 = make_extent_entry("1", 1);
    extent_entry entry2 = make_extent_entry("2", 2);
    
    bool compare = entry1 < entry2;

    cout << "compare:" << compare << endl;
    
    extent_entry_set entry_set;
    entry_set.insert(std::make_tuple("1",1));
    entry_set.insert(std::make_tuple("2",2));

    
}
