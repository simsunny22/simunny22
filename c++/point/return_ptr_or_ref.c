#include <stdio.h>


int* ret_point(int* a) {
        *a= *a + 1;
        return a;
}

int& ret_ref(int& b) {
        b = b + 1;
        return b;
}

int main() {
    int a = 10;
    printf("a==>%d, %p \n", a, &a);

    int b = 20;    
    printf("b==>%d, %p \n", b, &b);

    printf("============== \n");

    int *p_a = ret_point(&a);
    printf("p_a==>%d, %p \n", *p_a, p_a);

    int& r_b = ret_ref(b);
    printf("r_b==>%d, %p \n", r_b, &r_b);

    int c = ret_ref(b);
    printf("c==>%d, %p \n", c, &c);
}
