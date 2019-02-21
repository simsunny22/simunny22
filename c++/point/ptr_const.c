#include <stdio.h>

struct Stu {
       char* name;
       int  age;
};

void test_ptr_const() {
    int a =100;

    int *const ptr1=&a;
    *ptr1=300;
}

void test_ptr_const1() {
    struct Stu stu1 = {.name="wztest", .age=22};

    struct Stu *const ptr1 = &stu1; 
    ptr1->name="wdtest";
    ptr1->age=23;
}

int main () {
    test_ptr_const();
    test_ptr_const1();
}
