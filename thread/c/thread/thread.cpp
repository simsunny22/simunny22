// 必须的头文件
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
 
 
#define NUM_THREADS 5

void thread_join();
void thread_detach();
 
void* say_hello(void* args) {
	sleep(1);
	int tid = *((int*)args);
    	printf("Hello Runoob id is %d！\n", tid);
    return 0;
}
 
int main() {
	thread_join();
	thread_detach();
	//never do this, because thread_detach call thread_exit
	printf("main thread is exit \n");
}

void thread_join() {
	void* thread_ret;
	pthread_t tids[NUM_THREADS];
	int indexs[NUM_THREADS];
	for(int i = 0; i < NUM_THREADS; ++i) {
	    indexs[i] = i; 
	    //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
	    //因为声明周期的问题，不能直接传递i，作为线程的参数，需要传递全局变量.
	    int ret = pthread_create(&tids[i], NULL, say_hello, (void *)&(indexs[i]));
	    if (ret != 0) {
	       printf("pthread_create error: error_code=%d\n", ret) ;
	    }
	    //here wait thread finish and release thread resource
	    ret = pthread_join(tids[i], &thread_ret);
	    if (ret != 0) {
	       printf("pthread_join error: error_code=%d\n", ret) ;
	    }
	    int* ptr = (int*)thread_ret;
	    if (ptr == NULL) {
	    	 printf("thread exit coid is null \n ");
	    } else {
	    	printf("thread exit code: %d\n ", *ptr);
	    }
	}
	printf("main thread join is exit \n");
}

void thread_detach() {
	pthread_t tids[NUM_THREADS];
	int indexs[NUM_THREADS];
	for(int i = 0; i < NUM_THREADS; ++i) {
	    indexs[i] = i; 
	    //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
	    //因为声明周期的问题，不能直接传递i，作为线程的参数，需要传递全局变量.
	    int ret = pthread_create(&tids[i], NULL, say_hello, (void *)&(indexs[i]));
	    if (ret != 0) {
	       printf("pthread_create error: error_code=%d\n", ret) ;
	    }
	    ret = pthread_detach(tids[i]);
	    if (ret != 0) {
	       printf("pthread_join error: error_code=%d\n", ret) ;
	    }
	}
	printf("main thread detach is exit \n");
	//here wait all thread and exit process
    	pthread_exit(NULL);
}

