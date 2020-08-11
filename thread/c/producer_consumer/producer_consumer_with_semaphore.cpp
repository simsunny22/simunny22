#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#define CAPACITY 8     // 缓冲区的最大容量
int buffer[CAPACITY];  // 缓冲区数组
int in;                // 缓冲区的写指针
int out;               // 缓冲区的读指针
int size;              // 缓冲区中的数据个数

typedef int semaphore;

void buffer_init() {
    in = 0;
    out = 0;
    size = 0;
}

// 判断缓冲区是否为空
int buffer_is_empty() {
    return size == 0; 
}

// 判断缓冲区是否为满
int buffer_is_full() {
    return size == CAPACITY; 
}

// 向缓冲区中追加一个数据
void buffer_put(int item) {
    buffer[in] = item;
    in = (in + 1) % CAPACITY;
    size++;
}

// 从缓冲区中取走一个数据
int buffer_get() {
    int item;

    item = buffer[out];
    out = (out + 1) % CAPACITY;
    size--;

    return item;
}
//队列中有数据格子的数量，如果是空，就是所有的格子都是空，表示不能消费
semaphore full_semaphore = 0; //for conumse
//列表中空闲格子的数量，如果是空，就是所有的格子都有数据，表示不能生产
semaphore empty_semaphore = CAPACITY; //for prouducer
pthread_mutex_t mutex;

// 生产者线程执行的流程
void *producer_loop(void *arg) {
    int i;
    for (i = 0; i < CAPACITY*2; i++) {       
    	printf("produce %d\n", i);
	while(empty_semaphore <= 0) {
		sleep(1);
	}
	empty_semaphore--;

        pthread_mutex_lock(&mutex);
        buffer_put(i);
        pthread_mutex_unlock(&mutex);        

	//生产之后，有数据的格子数量+1，如果在+1前，有数量的格子数量是0，表示队列为空
	//消费者会被full_semaphore这个信号量阻塞住，通过full_semaphore++，可以唤醒消费者
	full_semaphore++;
    }

    return NULL;
}

// 消费者线程执行的流程
void *consumer_loop(void *arg) {
    int i;
    for (i = 0; i < CAPACITY*2; i++) {  
	while(full_semaphore <= 0) {
		sleep(1);
	}
	full_semaphore--;

        pthread_mutex_lock(&mutex);
        // 此时，缓冲区肯定不是空的，从缓冲区取数据
        int item = buffer_get();
        pthread_mutex_unlock(&mutex);        

        //消费之后，空闲的格子数量+1，如果在+1前，空闲的格子数量是0，表示队列已经满了
	//生产者会被empty_semaphore这个信号量阻塞住，通过empty_semaphore++，可以唤醒生产者
	empty_semaphore++;

    	printf("\tconsume %d\n", item);
    }

    return NULL;
}

int main() {
    pthread_t producer;
    pthread_t consumer;

    buffer_init();
    pthread_create(&producer, NULL, producer_loop, NULL);
    pthread_create(&consumer, NULL, consumer_loop, NULL);

    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);
    return 0;
}
