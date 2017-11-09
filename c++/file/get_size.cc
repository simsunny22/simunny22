#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <experimental/filesystem>


using namespace std;

int GetFileSizeInBytes(ifstream& inFile)
{
    int begin = inFile.tellg();

    inFile.seekg(0, ios::end);

    int fileSize = inFile.tellg();

    // restore to initial read position
    inFile.seekg(begin, ios::beg);

    return fileSize;
}


//http://www.cnblogs.com/xudong-bupt/p/3506772.html
void test(){
    FILE * pFile;
    long size;
    pFile = fopen ("//opt/vega/data/hive/master_journal/default-PrimaryJournal-1-1-1.log","rb");
    if (pFile==NULL){
        cout << "error ...." << endl;
    }else{
        fseek (pFile, 0, SEEK_END);
        size=ftell (pFile);
        fclose (pFile);
        cout << "szize:" <<size <<endl;
    }

}
int main(){
     test();
   // string filepath = "hive_io.cc";
   // ifstream ifs(filepath.c_str());
   // int size = GetFileSizeInBytes(ifs);
   // cout << "szie:" << size <<endl;
}
