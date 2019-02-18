#include <stdio.h>

struct Stu {
       char* name;
       int  age;
};

void test_const_ptr() {
    const int *ptr;

    int a = 100;
    ptr= &a;
    
    int b = 200;
    ptr = &b;

    b=200;
    //*ptr=200;
}

void test_const_ptr1() {
    const struct Stu *ptr ;

    struct Stu stu1 = {.name="wztest", .age=22};
    ptr=&stu1;

    struct Stu stu2 = {.name="wdtest", .age=22};
    ptr=&stu2;
    
    stu2.name="test";
    //ptr->name="test";
}

int main () {
    test_const_ptr();
    test_const_ptr1();
}
