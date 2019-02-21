#include <stdio.h>
#include <stdlib.h>

void delete_point0(int* a){  
    free(a);  
    a = NULL;  
}  


void delete_point1(int*& a){  
    free(a);  
    a = NULL;  
}  

int main() {
    int *p0 = (int*) malloc(1);
    delete_point0(p0);
    if (p0 == NULL) {
        printf("p0 is null \n");
    } else {
        printf("p0 is not null \n");
    }
    
    int *p1 = (int*) malloc(1);
    delete_point1(p1);
    if (p1 == NULL) {
        printf("p1 is null \n");
    } else {
        printf("p1 is not null \n");
    }
    
}


