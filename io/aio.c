#include <aio.h>
#include <errno.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>


#include <sys/syscall.h> 
#define gettid() syscall(__NR_gettid)

//gcc aio.c -lrt
//dd if=/dev/urandom of="foobar.txt" count=10000

const int SIZE_TO_READ = 100;

void aio_completion_handler(sigval_t sigval) {
	printf("aio_completion_handler ppid:%d, pid:%d, tid:%d \n", getppid(), getpid(), gettid());
	struct aiocb* req = (struct aiocb *)sigval.sival_ptr;
	
	/* Did the request complete? */
	if (aio_error(req) == 0) {
		/* Request completed successfully, get the return status */
	 	int ret = aio_return( req );
		if (ret != -1) {
	  		printf("aio_completion_handler Success! buffer:%s\n", req->aio_buf);
		} else {
			printf("aio_completion_handler Error!\n");
		}
	}
	free(req);	
	return;
}

//here we malloc aio_cb, not struct, to avoid the struct be free
struct aiocb* make_aio_cb(int fd, char* buffer) {
	struct aiocb* cb = (struct aiocb*) malloc(sizeof(struct aiocb));
	memset(cb, 0, sizeof(struct aiocb));
	
	cb->aio_nbytes = SIZE_TO_READ;
	cb->aio_fildes = fd;
	cb->aio_offset = 0;
	cb->aio_buf = buffer;
	//set aio return by other thread
	cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
	//set aio callback func
	cb->aio_sigevent.sigev_notify_function = aio_completion_handler;
	//set aio callback params
	cb->aio_sigevent.sigev_value.sival_ptr = cb;
	cb->aio_sigevent.sigev_notify_attributes = NULL;
	return cb;
}

void aio_thread_test(int fd, char* buffer) {
  	printf("aio_thread_test ppid:%d, pid:%d, tid:%d \n", getppid(), getpid(), gettid());
	struct aiocb* cb  = make_aio_cb(fd, buffer);
	int ret = aio_read(cb);
  	if (ret < 0 ) {
		printf("aio_read error");
  	} 
}

void aio_loop_test(int fd, char* buffer) {
  	printf("aio_loop_test ppid:%d, pid:%d, tid:%d \n", getppid(), getpid(), gettid());

	struct aiocb cb;
	memset(&cb, 0, sizeof(struct aiocb));
	cb.aio_nbytes = SIZE_TO_READ;
	cb.aio_fildes = fd;
	cb.aio_offset = 0;
	cb.aio_buf = buffer;
	
	if (aio_read(&cb) == -1) {
		printf("Unable to create request!\n");
	  	exit(EXIT_FAILURE);
	}
	printf("Request enqueued!\n");
	
	// wait until the request has finished
	while(aio_error(&cb) == EINPROGRESS) {
		printf("Working...\n");
	}
	
	int numBytes = aio_return(&cb); // success?
	if (numBytes != -1)
		printf("aio_loop_test Success! buffer:%s\n", buffer);
	else
	  	printf("aio_loop_test Error!\n");
}

int open_file() {
	int file = open("foobar.txt", O_RDONLY, 0);
	if (file == -1) {
        	printf("Unable to open file!\n");
  	    	exit(EXIT_FAILURE);
  	}
	return file;
}

int main() {
	int fd = open_file();
	char* buffer = (char *)malloc(SIZE_TO_READ);
	aio_loop_test(fd, buffer);
	memset(buffer, 0, SIZE_TO_READ);
	aio_thread_test(fd, buffer);
	sleep(10);
	close(fd);
	return 0;
}
