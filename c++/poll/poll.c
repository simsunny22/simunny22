// Server side C/C++ program to demonstrate Socket programming 
#include <unistd.h> 
#include <stdio.h> 
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h> 
#include <string.h> 
#define PORT 8080 
#define OPEN_MAX 1024 

#include <poll.h>
#include <arpa/inet.h>


void handle_poll(int listen_fd);

void handle_accept(int listenfd, struct pollfd *clientfds, int *imax);
void handle_connect(struct pollfd *clientfds, int num);


int main(int argc, char const *argv[]) 
{ 
	int server_fd, new_socket, valread; 
	struct sockaddr_in address; 
	int opt = 1; 
	int addrlen = sizeof(address); 
	char buffer[1024] = {0}; 
	const char *hello = "Hello from server"; 
	
	// Creating socket file descriptor 
	if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) 
	{ 
		perror("socket failed"); 
		exit(EXIT_FAILURE); 
	} 
	
	// Forcefully attaching socket to the port 8080 
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) 
	{ 
		perror("setsockopt"); 
		exit(EXIT_FAILURE); 
	} 

	address.sin_family = AF_INET; 
	address.sin_addr.s_addr = INADDR_ANY; 
	address.sin_port = htons( PORT ); 
	
	// Forcefully attaching socket to the port 8080 
	if (bind(server_fd, (struct sockaddr *)&address, sizeof(address))<0) 
	{ 
		perror("bind failed"); 
		exit(EXIT_FAILURE); 
	} 

	if (listen(server_fd, 3) < 0) 
	{ 
		perror("listen"); 
		exit(EXIT_FAILURE); 
	} 

    handle_poll(server_fd);
	return 0; 
} 


void handle_poll(int listenfd)
{
	int connfd, sockfd;
	struct sockaddr_in cliaddr;
	socklen_t cliaddrlen;
	struct pollfd clientfds[OPEN_MAX];
	int nready;


	//添加监听描述符
	clientfds[0].fd = listenfd;
	clientfds[0].events = POLLIN;

	//初始化客户连接描述符
    //int i, conn_num = 1;
    int i, imax = 0;
	for (i = 1; i < OPEN_MAX; i++)
	{
        	clientfds[i].fd = -1;
	}
    //循环处理
    while(1)
    {
	    //获取可用描述符的个数
	    printf("wztest ==================\n");

	    nready = poll(clientfds, imax + 1 , -1);
	    if (nready == -1)
	    {
	        perror("poll error:");
	        exit(1);
	    }

        if (clientfds[0].revents & POLLIN) {
            handle_accept(listenfd, clientfds, &imax);
        }

        //if (--nready > 0){
            handle_connect(clientfds, imax);
        //}

	}
}

void handle_accept(int listenfd, struct pollfd *clientfds, int *maxi) 
{
    int connfd;
    struct sockaddr_in cliaddr;
    socklen_t cliaddrlen;
    cliaddrlen = sizeof(cliaddr);

    //接受新的连接
    if ((connfd = accept(listenfd,(struct sockaddr*)&cliaddr,&cliaddrlen)) == -1)
    {
        exit(1);
    }
    fprintf(stdout,"accept a new client: %s:%d\n", inet_ntoa(cliaddr.sin_addr),cliaddr.sin_port);

    //将新的连接描述符添加到数组中
    int i;
    for (i = 1;i < OPEN_MAX;i++)
    {
        if (clientfds[i].fd < 0)
        {
            clientfds[i].fd = connfd;
            //将新的描述符添加到读描述符集合中
            clientfds[i].events = POLLIN;
            break;
        }
    }

    if (i == OPEN_MAX)
    {
        fprintf(stderr,"too many clients.\n");
        exit(1);
    }

    //将新的描述符添加到读描述符集合中
    //clientfds[i].events = POLLIN;
    
    //记录客户连接套接字的个数
    *maxi = (i > *maxi ? i : *maxi);
}



#define MAXLINE 1024
void handle_connect(struct pollfd *clientfds, int num) 
{
    printf("wztest handle_connect \n");
    int i,n;
    char buf[MAXLINE];
    memset(buf, 0, MAXLINE);

    for (i = 1; i <= num; i++)
    {
        if (clientfds[i].fd < 0){
            continue;
        }
        //测试客户描述符是否准备好
        if (clientfds[i].revents & POLLIN)
        {
            //接收客户端发送的信息
            n = read(clientfds[i].fd, buf, MAXLINE);
            printf("wztest handle_connect no:%d, read_count:%d, buf:%s \n", i, n, buf );
            if (n == 0)
            {
                close(clientfds[i].fd);
                clientfds[i].fd = -1;
                continue;
            }

            write(STDOUT_FILENO, buf, n);
            printf("\n");
            //printf("wztest %s", buf);
            
            //向客户端发送buf
            write(clientfds[i].fd, buf, n);
            //send(clientfds[i].fd, buf, strlen(buf), 0);
        }
    }
}
