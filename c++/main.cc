#include "entry.hh"
#include "map"

using namespace std;


void test_class_point();
void test_class_object();

int main(){
   test_class_point();
   cout << "-------------------------" << endl;
   test_class_object();
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

