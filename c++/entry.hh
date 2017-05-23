#include <iostream>
#include <string>
using namespace std;


//father
class entry{
public:
    virtual entry* get_entry(){
        std::cout << "get get_entry" << std::endl;
        return this;
    };

    virtual entry get_entry_object(){
        std::cout << "get get_entry_object" << std::endl;
        return *this;
    }
};

//child1
class volume_entry: public entry{
    private:
        string _volume_id;
        string _disk_ids;
        int    _vclock;
    public:
         volume_entry(string volume_id, string disk_ids, int vclock)
             :_volume_id(volume_id)
             ,_disk_ids(disk_ids)
             ,_vclock(vclock){}

        volume_entry* get_entry(){
            std::cout << "get volume_entry" << std::endl;
            return this;
        }

        entry get_entry_object(){
            std::cout << "get volume_entry_object" << std::endl;
            return *this;
        }
        int test_volume_entry(){
            std::cout << "test_volume_entry" << std::endl;
            return 0;
        }


        int get_volume_type(){
            return 0;
        }
};

//child2
class extent_group_entry: public entry{
    private:
        string _extent_group_id;
        string _disk_ids;
    public:
        extent_group_entry(string extent_group_id, string disk_ids)
            :_extent_group_id(extent_group_id)
            ,_disk_ids(disk_ids){}

        extent_group_entry* get_entry(){
            std::cout << "get extent_group_entry" << std::endl;
            return this;
        }

        entry get_entry_object(){
            std::cout << "get extent_group_entry_object" << std::endl;
            return *this;
        }

        int test_extent_group_entry(){
            std::cout << "test_extent_group_entry" << std::endl;
            return 0;
        }


        int get_extent_group_entry(){
            return 1;
        }
};
