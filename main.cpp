#include<cstdio>
#include<stdlib.h>
#include<string.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<unistd.h>
#include<errno.h>
#include<fcntl.h>
#include<sys/epoll.h>
#include<signal.h>
#include "locker.h"
#include "threadpool.h"
#include "http_conn.h"


// 最大的文件描述符个数
#define MAX_FD 65535

// 一次监听的最大的文件描述符的数量
#define MAX_EVENT_NUMBER 10000


// 添加信号捕捉
void addsig(int sig, void(handler)(int))
{
    struct sigaction sa;
    memset(&sa, '\0', sizeof(sa));
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    sigaction(sig, &sa, NULL);
}

// 添加文件描述符到epoll中
extern void addfd(int epollfd, int fd, bool one_shot);


// 从文件描述符中删除文件描述符
extern void removefd(int epollfd, int fd);

// 在epoll中，修改文件描述符(重置socket上EOPOLLONESHOT事件，以确保下一次可读时，EPOLLIN可以被触发)
extern void modfd(int epollfd, int fd, int ev);



int main(int argc, char* argv[])
{
    if(argc <= 1)
    {
        // 如果参数个数小于等于1，有问题
        printf("安照如下格式运行：%s port_number\n",basename(argv[0]));  // argv[0] 程序名称
        exit(-1);
    }

    // 获取端口号
    int port = atoi(argv[1]); // argv[1] 端口号

    // 对SIGPIPE信号进行处理，遇到SIGPIPE时，会终止进程，因此做进行忽略处理
    addsig(SIGPIPE,SIG_IGN);

    // 创建线程池，初始化线程池
    threadpool<http_conn>* pool = NULL;
    try
    {
        pool = new threadpool<http_conn>;
    }
    catch(...)
    {
        exit(-1);
    }

    // 创建一个数组，用于保存所有的客户端信息
    http_conn* users = new http_conn[MAX_FD];

    
    // 网络通信
    // 1. 创建监听套接字
    int listenfd = socket(PF_INET,SOCK_STREAM,0);
    if(listenfd == -1)
    {
        perror("socket");
        exit(-1);
    }
    // 2. 设置端口复用(要在绑定之前设置)
    int reuse = 1;
    setsockopt(listenfd,SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    // 3. 绑定
    struct sockaddr_in saddr;
    saddr.sin_family = PF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port = htons(port);
    bind(listenfd,(struct sockaddr*)&saddr,sizeof(saddr));

    // 4. 监听
    listen(listenfd,5);

    // 5. 创建epoll对象，事件数组，添加
    epoll_event events[MAX_EVENT_NUMBER];
    int epollfd = epoll_create(5);

    // 6. 将监听的文件描述符添加到epoll中
    addfd(epollfd, listenfd, false); // 监听的文件描述符不需要添加oneshot
    http_conn::m_epollfd = epollfd; // 静态变量的引用方法 

    //主线程循环检测
    while(true)
    {
        // 检测到了几个事件
        int num = epoll_wait(epollfd, events, MAX_EVENT_NUMBER, -1);
        if( (num < 0)&&(errno!= EINTR) )
        {
            printf("调用epoll失败\n");
            break;
        }

        // 循环遍历事件数组
        for(int i = 0; i < num; i++)
        {
            int sockfd = events[i].data.fd;
            if(sockfd == listenfd)
            {
                // 有客户端连接进来
                struct sockaddr_in client_address;
                socklen_t client_addrlen = sizeof(client_address);
                int connfd = accept(listenfd, (struct sockaddr*)&client_address, &client_addrlen);
                
                if(http_conn::m_user_count >= MAX_FD)
                {
                    // 当前的连接数已满
                    // 给客户端回写提示信息：服务器正忙...
                    close(connfd);
                    continue;
                }

                // 将新的客户的数据初始化，放到数组中
                users[connfd].init(connfd, client_address);
            }
            else if(events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
            {
                // 对方异常断开或错误
                users[sockfd].close_conn();
            }
            else if(events[i].events & EPOLLIN)
            {
                if(users[sockfd].read())
                {
                    // 一次性把所有数据都读完
                    pool->append(users + sockfd);
                }
                else
                {
                    // 没读到数据，关闭连接
                    users[sockfd].close_conn();
                }
            }
            else if(events[i].events & EPOLLOUT)
            {
                if(!users[sockfd].write())  // 一次性写完所有数据
                {
                    // 失败
                    users[sockfd].close_conn();
                }
            }
        }
    }

    close(epollfd);
    close(listenfd);
    delete[] users;
    delete pool;
    return 0;

}