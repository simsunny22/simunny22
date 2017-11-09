#include "entry.hh"
#include "map"
#include "test.hh"
#include <memory>
#include <boost/smart_ptr/shared_ptr.hpp>
using namespace std;



void test_class_point();
void test_class_object();
void test_dynamic_cast();
void test_vector_reduce();
void test_jicheng();
void test_shared_ptr();




vector<int> vector_reduce(vector<int> new_files, vector<int> old_files);

int main(){
   size_t i;
   cout<< "size_t:" << sizeof(i) << endl;
   //string a = "#eztest";
   //test_class_point();
   //cout << "-------------------------" << endl;
   //test_class_object();
   //cout << "-------------------------" << endl;
   //test_dynamic_cast();
   cout << "-------------------------" << endl;
   //test_vector_reduce();
   //test_jicheng();
   //test_shared_ptr();
}



void test_class_point(){    
    entry* entry1 = new volume_entry("volume_1", "1:2:3", 1);
    entry* entry2 = new extent_group_entry("extent_group_1", "2:3:4");
    map<string, entry*> entry_map; //this is pointer
    entry_map.insert(pair<string, entry*>("volume_1", entry1));
    entry_map.insert(pair<string, entry*>("extent_group_1", entry2));

    for(map<string, entry*>::iterator it = entry_map.begin(); it !=entry_map.end(); it++){
        entry* entry_test = it->second;
        entry_test->get_entry(); //here we execute child method (volume_entry, extent_group_entry)
    }

}



void test_class_object(){
    entry entry1 = volume_entry("volume_1", "1:2:3", 1);
    entry entry2 = extent_group_entry("extent_group_1", "2:3:4");
    map<string, entry> entry_map; //this is object
    entry_map.insert(pair<string, entry>("volume_1", entry1));
    entry_map.insert(pair<string, entry>("extent_group_1", entry2));

    for(map<string, entry>::iterator it = entry_map.begin(); it !=entry_map.end(); it++){
        entry entry_test = it->second;
        entry_test.get_entry(); //here we execute father method
    }
}


void test_dynamic_cast(){
    entry* entry1 = new volume_entry("volume_1", "1:2:3", 1);
    entry* entry2 = new extent_group_entry("extent_group_1", "2:3:4");

    dynamic_cast<volume_entry*>(entry1) -> test_volume_entry();
    dynamic_cast<extent_group_entry*>(entry2) -> test_extent_group_entry();

}

void test_shared_ptr(){
    shared_ptr<entry> entry1 = make_shared<volume_entry>("volume_1", "1:2:3", 1);
    entry1->get_entry();


}

void test_jicheng(){
   entry* entry1 = new volume_entry("volume_1", "1:2:3", 1);
   entry* entry2 = new extent_group_entry("extent_group_1", "2:3:4");
   //entry1.test_volume_entry();
   //entry2.test_extent_group_entry();

   //entry1->get_entry()->test_volume_entry();
   //entry2->get_entry()->test_extent_group_entry();

   //auto volume = entry1->get_entry();
   //auto extent = entry2->get_entry();
   
   //volume->test_volume_entry();
   //extent->test_extent_group_entry();

}

void test_vector_reduce(){
    vector<int>  v1;
    vector<int>  v2;
    for(int i = 0; i < 10; i ++){
        v1.push_back(i);
    }

    for(int i=3; i<7; i++){
        v2.push_back(i);
    }


    auto v3 = vector_reduce(v1, v2);
    for(auto v: v3){
        cout << "v:" << v << std::endl;
    }
}





vector<int> vector_reduce(vector<int> new_files, vector<int> old_files){
    for(auto old_it = old_files.begin(); old_it !=  old_files.end(); old_it ++){
        auto new_it = new_files.begin();
        auto new_end = new_files.end();
        while(new_it != new_end){
            cout << "old_it:" << *old_it <<endl;
            cout << "new_it:" << *new_it <<endl;
            if (*old_it == *new_it){
                new_files.erase(new_it);
                break;
            }   
            new_it++;
        }   
        cout << "======================" << endl;
    }   
    return new_files;
}



