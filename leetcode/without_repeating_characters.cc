#include <iostream>
#include <map>
#include <string.h>


bool find(char key, std::map<char, bool>& clist) {
    std::map<char, bool>::iterator it = clist.find(key);
    if (it != clist.end()) {
            return true;
    }
    return false;
}

int main() {
       char raw_list[] = "dfjsdjglsdlficg";
       int i = 0, j = 1;
       int max_len = 0;
       int start, end;
       int length = strlen(raw_list);
       std::cout << raw_list << std::endl;
       std::cout << length << std::endl;

       std::map<char, bool> clist; 
       
       
       clist.insert(std::pair<char, bool>(raw_list[i], true));
       while ( j < length ) {
               std::cout << "i:" << i << ", j:" << j  << std::endl;
               if ( find(raw_list[j], clist) ) {
                    int len = j - i;
                    if (len > max_len) {
                            max_len = len;
                            start = i;
                            end = j-1;
                            std::cout << start  << "," << end << std::endl; 
                    }
                    i++;
                    j = i + 1;
                    clist.clear();
                    continue;
               }
               clist.insert(std::pair<char, bool>(raw_list[j], true));
               j++;
       }

       std::cout << start  << "," << end << std::endl; 
}

