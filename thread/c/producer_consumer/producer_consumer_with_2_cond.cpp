#include <stdio.h>
#include <pthread.h>

#define CAPACITY 8     // 缓冲区的最大容量
int buffer[CAPACITY];  // 缓冲区数组
int in;                // 缓冲区的写指针
int out;               // 缓冲区的读指针
int size;              // 缓冲区中的数据个数

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

pthread_cond_t consume_cond;
pthread_cond_t produce_cond;
pthread_mutex_t mutex;

// 生产者线程执行的流程
void *producer_loop(void *arg) {
    int i;

    // 生产CAPACITY*2个数据
    for (i = 0; i < CAPACITY*2; i++) {       
    	printf("produce %d\n", i);
        pthread_mutex_lock(&mutex);

        // 当缓冲区为满时，生产者需要等待
        while (buffer_is_full()) {   
            // 当前线程已经持有了mutex，首先释放mutex，然后阻塞，醒来后再次获取mutex
            pthread_cond_wait(&produce_cond, &mutex);
        }

        // 此时，缓冲区肯定不是满的，向缓冲区写数据
        buffer_put(i);
        pthread_mutex_unlock(&mutex);        

	// 如果队列满空了，此时消费者会被consume_cond阻塞住，通过consume_cond生产者线程
        pthread_cond_signal(&consume_cond);
    }

    return NULL;
}

// 消费者线程执行的流程
void *consumer_loop(void *arg) {
    int i;

    // 消费CAPACITY*2个数据
    for (i = 0; i < CAPACITY*2; i++) {  
        pthread_mutex_lock(&mutex);

        // 当缓冲区为空时，消费者需要等待
        while (buffer_is_empty()) {   
            // 当前线程已经持有了mutex，首先释放mutex，然后阻塞，醒来后再次获取mutex
            pthread_cond_wait(&consume_cond, &mutex);
        }

        // 此时，缓冲区肯定不是空的，从缓冲区取数据
        int item = buffer_get();
        pthread_mutex_unlock(&mutex);        

        // 如果队列满了，此时生产者会被produce_cond阻塞住，通过produce_cond生产者线程
        pthread_cond_signal(&produce_cond);

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
