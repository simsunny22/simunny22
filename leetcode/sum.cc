#include <iostream>
#include <stdlib.h>
#include <stdio.h>



int cmpfunc(const void* a , const void* b) {
     return (*(int*)a - *(int*)b) ;
}

void print_list(const int *a, int size) {

     for (int i = 0; i < size; i++) {
            std::cout << a[i] << ",";
     }
     std::cout << std::endl;
}


void find3(const int* a, int size, int res3) {
        for (int k=0; k < size; k++) {
                int res2 = res3 - a[k];
                printf("%d, %d \n", a[k], res2);
                int i = 0, j = size-1;
                while(i < j) {
                        printf("a[i]:%d, a[j]:%d \n", a[i], a[j]);
                        if (a[i] + a[j] == res2) {
                                printf("bingooo %d, %d, %d \n", a[k], a[i], a[j]); 
                                i++;
                                j--;
                        } else if (a[i] + a[j] < res3) {
                                i++;
                        } else {
                                j--;
                        }
                }
                std::cout << "================" << std::endl;
        }
      
}

int main() {
    int a[] = {-1, 0, 1, 2, -1, -4};
    //int a[]  = {-1, -5, -7, 12, 3, -34, -5, 7};
    int size = sizeof(a)/sizeof(int);
    print_list(a, size);
    int res = 0;


    qsort(a, size, sizeof(int), cmpfunc) ;
    print_list(a, size);
    find3(a, size, res);

    return 0;
}
