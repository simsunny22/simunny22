#include<iostream>
#include<fstream>
#include<string>
#include"map"

using namespace std;

void write_file_map(string filename, map<string, int>& stu){
   //string flush;
   //string close;
   fstream file(filename.c_str(),  ios::out|ios::app);//filename.c_str() for pre-c++11
   if(file){
       for(map<string, int>::iterator it = stu.begin(); it != stu.end(); it++){
           string name = it->first;
           int    age  = it->second;
           file << name << " " << age << endl;
       }
       //cout << "pause for flush file" <<endl;
       //cin >> flush;
       //file.flush();
       //cout << "pause for close file" <<endl;
       //cin >> close;
       file.close();
   }
}


void read_file_map(string filename){
   map<string, int> stu;
   string name;
   int age;

   fstream file(filename.c_str(), ios::in);
   while(!file.eof()){
     file >> name  >> age ;
     stu.insert(make_pair<string, int>(name, age));
   }
   file.close();

   for(map<string, int>::iterator it = stu.begin(); it != stu.end(); it++){
       name = it->first;
       age  = it->second;
       cout << "name:" << name << " " << "age:" << age << endl;
   }
}

void read_file_map_line(string filename){
   char line[100]; // char *line; segment fault

   fstream file(filename.c_str(), ios::in);
   while(file.peek() != EOF){
   //while(!file.eof()){
     file.getline(line, 99);
     cout << line << endl;
   }
   file.close();
}


int main(){
    //map<string, int> stu;
    //stu.insert(make_pair("wz", 20));
    //stu.insert(make_pair("wd", 20));
    //write_file_map("file_map", stu);
    read_file_map("file_map");
    read_file_map_line("file_map");
}
