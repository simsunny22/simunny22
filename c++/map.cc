#include<unordered_map>
#include<iostream>
#include<string>

using namespace std;


int main(){
    unordered_map<string, int> unmap;
    unmap.insert(std::make_pair<string, int>("wuzhao", 26));
    unmap.insert(std::make_pair<string, int>("wudong", 26));
    unmap.insert(std::make_pair<string, int>("wuzhao", 27));
    unmap.insert(std::make_pair<string, int>("wudong", 28));
    unmap["conghui"] = 26;
    unmap["conghui"] = 29;
    unmap.insert(std::make_pair<string, int>("wuzhao", 27));

    for(auto it : unmap){
        std::cout << "name:" << it.first << " ,age:" << it.second << endl;
    }
}
