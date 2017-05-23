#include <iostream>
#include <unistd.h>
#include <chrono>

using namespace std;

typedef chrono::duration<int> seconds_type;
int main(){ 
    


    chrono::system_clock::time_point tp1 = chrono::system_clock::now();
    chrono::time_point<chrono::system_clock,seconds_type> tp11 = chrono::time_point_cast<seconds_type>(chrono::system_clock::now());
    usleep(10* 1000*1000);
    cout << "................................." << endl;
    chrono::system_clock::time_point tp2 = chrono::system_clock::now();
    chrono::time_point<chrono::system_clock,seconds_type> tp22 = chrono::time_point_cast<seconds_type>(chrono::system_clock::now());

    auto diff = tp2 - tp1;
    
    cout << "diff:" << diff.count() << endl;
    cout << "diff:" << chrono::duration_cast<chrono::seconds>(diff).count() << endl;

    cout << "diff:" << (tp22 - tp11).count() << endl;






}
