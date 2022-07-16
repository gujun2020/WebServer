#include "http_conn.h"


// 定义HTTP响应的一些状态信息
const char* ok_200_title = "OK";
const char* error_400_title = "Bad Request";
const char* error_400_form = "Your request has bad syntax or is inherently impossible to satisfy.\n";
const char* error_403_title = "Forbidden";
const char* error_403_form = "You do not have permission to get file from this server.\n";
const char* error_404_title = "Not Found";
const char* error_404_form = "The requested file was not found on this server.\n";
const char* error_500_title = "Internal Error";
const char* error_500_form = "There was an unusual problem serving the requested file.\n";

// 网站的根目录
const char* doc_root = "/home/gujun/WebServer/resources";


// 对静态变量初始化
int http_conn::m_epollfd = -1; // 所有的socket上的事件都被注册到同一个epoll对象中
int http_conn::m_user_count = 0; // 统计用户的数量


// 设置文件描述符非阻塞
void setnonblocking(int fd)
{
    int old_flag = fcntl(fd, F_GETFL);
    int new_flag = old_flag | O_NONBLOCK;
    fcntl(fd,F_SETFL,new_flag);

}

// 向epoll中添加需要监听的文件描述符
void addfd(int epollfd, int fd, bool one_shot)
{
    epoll_event event;
    event.data.fd = fd;
    // event.events = EPOLLIN | EPOLLRDHUP; // EPOLLRDHUP 在底层自动处理连接断开的情形
    event.events = EPOLLIN | EPOLLRDHUP;
    if(one_shot)
    {
        event.events |= EPOLLONESHOT;
    }

    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);

    // 设置文件描述符非阻塞，防止无数据可读时进入阻塞状态
    setnonblocking(fd);
}

// 从epoll中删除文件描述符
void removefd(int epollfd, int fd)
{
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

// 在epoll中，修改文件描述符(重置socket上EOPOLLONESHOT事件，以确保下一次可读时，EPOLLIN可以被触发)
void modfd(int epollfd, int fd, int ev)
{
    epoll_event event;
    event.data.fd = fd;
    event.events = ev | EPOLLONESHOT | EPOLLRDHUP;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}

// 初始化新接收的连接,外部调用初始化套接字地址
void http_conn::init(int sockfd, const sockaddr_in& addr)
{
    m_sockfd = sockfd;
    m_address = addr;

    // 设置端口复用
    int reuse = 1;
    setsockopt(m_sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    // 添加到epoll中
    addfd(m_epollfd, m_sockfd, true);

    // 客户端的user_count增加1
    m_user_count++;

    init();
}

// 初始化连接的其他信息
void http_conn::init()
{
    m_check_state = CHECK_STATE_REQUESTLINE; // 初始化状态为解析请求首行
    m_linger = false; // 默认不保持链接  Connection : keep-alive保持连接
    m_method = GET;   // 默认请求方式为GET
    m_url = 0;
    m_version = 0;
    m_content_length = 0;
    m_host = 0;
    m_checked_index = 0; //当前正在解析的字符的位置
    m_start_line = 0; // 当前正在解析的行的索引
    m_read_idx = 0; // 当前正在读的读缓冲区的位置
    m_write_idx = 0;

    bzero(m_read_buf, READ_BUFFER_SIZE);
    bzero(m_write_buf, READ_BUFFER_SIZE);
    bzero(m_real_file, FILENAME_LEN);
}

// 关闭连接
void http_conn::close_conn()
{
    if(m_sockfd != -1)
    {
        removefd(m_epollfd,m_sockfd);
        m_sockfd = -1;
        m_user_count--;  // 总用户数量减少
    }
}


// 非阻塞一次性读完所有数据
// 循环读取客户数据，直到无数据可读或者对方关闭连接
bool http_conn::read()
{
    if(m_read_idx>=READ_BUFFER_SIZE)
    {
        // 标识读缓冲区的指针超过了读缓冲区的大小
        // 数据已经被读完
        // 通常读缓冲区大小是足够的，这种情况很少
        return false;
    }

    // 读取到的字节
    int byte_read = 0;
    while (true)
    {
        byte_read = recv(m_sockfd, m_read_buf + m_read_idx, READ_BUFFER_SIZE-m_read_idx, 0);
        if(byte_read == -1)
        {
            if(errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // 没有数据
                break;
            }
            return false;
        }
        else if(byte_read == 0)
        {
            // 对方关闭了连接
            return false;
        }

        // 读到了数据，更新索引
        m_read_idx += byte_read;
    }
    printf("读取到的数据：%s\n",m_read_buf);
    return true;
}

// 主状态机，从整个请求的角度解析请求
// 解析HTTP请求
http_conn::HTTP_CODE http_conn::process_read()
{
    LINE_STATUS line_status = LINE_OK;
    HTTP_CODE ret = NO_REQUEST;

    char* text = 0;

    // 如果当前主状态机检查请求数据并且从状态机所在行有数据
    // 或者 从状态机当前解析的行有数据
    while (((m_check_state == CHECK_STATE_CONTENT) && (line_status == LINE_OK)) || ((line_status = parse_line())==LINE_OK))
    {
        // 解析到了请求体是完整的数据   或者  解析到了一行数据，该行数据是完整的数据
        // 获取一行数据
        text = get_line();

        // 将正在解析的字符的位置  赋值更新  解析行的位置
        m_start_line = m_checked_index;

        printf("获取一行http信息：%s\n",text);

        switch (m_check_state)
        {
            case CHECK_STATE_REQUESTLINE:
            {
                // 解析请求行
                ret = parse_request_line(text);
                if(ret == BAD_REQUEST)
                {
                    // 语法错误
                    return BAD_REQUEST;
                }
                break;
            }

            case CHECK_STATE_HEADER:
            { 
                ret = parse_headers(text);
                if(ret == BAD_REQUEST)
                {
                    return BAD_REQUEST;
                }
                else if(ret == GET_REQUEST)
                {
                    // 获取了一个完整的请求
                    return do_request(); // 解析具体的请求信息
                }
                break;
            }

            case CHECK_STATE_CONTENT:
            {
                ret = parse_content(text);
                if(ret == GET_REQUEST)
                {
                    // 获得一个完整的请求
                    return do_request();
                }
                line_status = LINE_OPEN; // 行数据尚不完整
                break;
            }
            default:
            {
                return INTERNAL_ERROR;
            }
        }
        
    }
    return NO_REQUEST;// 最后返回NO_REQUEST请求不完整，需要继续获取客户的信息
}

// 解析具体的请求信息
// 当得到一个完整、正确的HTTP请求时，我们就分析目标文件的属性，
// 如果目标文件存在、对所有用户可读，且不是目录，则使用mmap将其
// 映射到内存地址m_file_address处，并告诉调用者获取文件成功
http_conn::HTTP_CODE http_conn::do_request()
{
    // "/home/gujun/WebServer/resources"
    strcpy( m_real_file, doc_root );
    int len = strlen( doc_root );
    strncpy( m_real_file + len, m_url, FILENAME_LEN - len - 1 );
    // 获取m_real_file文件的相关的状态信息，-1失败，0成功
    if ( stat( m_real_file, &m_file_stat ) < 0 ) 
    {
        return NO_RESOURCE;
    }

    // 判断访问权限
    if ( ! ( m_file_stat.st_mode & S_IROTH ) ) 
    {
        return FORBIDDEN_REQUEST;
    }

    // 判断是否是目录
    if ( S_ISDIR( m_file_stat.st_mode ) ) 
    {
        return BAD_REQUEST;
    }

    // 以只读方式打开文件
    int fd = open( m_real_file, O_RDONLY );
    // 创建内存映射，将网页资源映射到m_file_address上
    m_file_address = ( char* )mmap( 0, m_file_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );
    close( fd );
    return FILE_REQUEST;
}

// 对内存映射区执行munmap操作
void http_conn::unmap() 
{
    if( m_file_address )
    {
        munmap( m_file_address, m_file_stat.st_size );
        m_file_address = 0;
    }
}

// 解析请求首行，获取请求方法，目标URL，HTTP版本
http_conn::HTTP_CODE http_conn::parse_request_line(char* text)
{
    // GET / HTTP/1.1
    m_url = strpbrk(text, " \t"); // 查找第一次出现空格的索引，并返回(将text截断)
    if (! m_url)
    { 
        return BAD_REQUEST;
    }

    *m_url++ = '\0';  // 将text截断
    // GET\0/ HTTP/1.1
    char* method = text;
    if(strcasecmp(method,"GET")==0)
    {
        m_method = GET;
    }
    else
    {
        return BAD_REQUEST;
    }

    // / HTTP/1.1
    m_version = strpbrk(m_url," \t");
    if(!m_version)
    {
        return BAD_REQUEST;
    }
    *m_version++ = '\0';
    if(strcasecmp(m_version,"HTTP/1.1")!=0)
    {
        return BAD_REQUEST;
    }

    // http://192.168.220.137/index.html
    if(strncasecmp(m_url,"http://",7) ==0)
    {
        m_url += 7;
        m_url = strchr(m_url,'/');  // /index.html
    }
    if(!m_url || m_url[0]!='/')
    {
        return BAD_REQUEST;
    }

    m_check_state = CHECK_STATE_HEADER; // 解析完请求行后，主状态机状态变成检查请求头
    return NO_REQUEST;

}
// 解析请求头
http_conn::HTTP_CODE http_conn::parse_headers(char* text)
{
    // 遇到空行，表示头部字段解析完毕
    if( text[0] == '\0' ) 
    {
        // 如果HTTP请求有消息体，则还需要读取m_content_length字节的消息体，
        // 状态机转移到CHECK_STATE_CONTENT状态
        if ( m_content_length != 0 ) 
        {
            m_check_state = CHECK_STATE_CONTENT;
            return NO_REQUEST;
        }
        // 否则说明我们已经得到了一个完整的HTTP请求
        return GET_REQUEST;
    } 
    else if ( strncasecmp( text, "Connection:", 11 ) == 0 ) 
    {
        // 处理Connection 头部字段  Connection: keep-alive
        text += 11;
        text += strspn( text, " \t" );
        if ( strcasecmp( text, "keep-alive" ) == 0 ) 
        {
            m_linger = true;
        }
    } 
    else if ( strncasecmp( text, "Content-Length:", 15 ) == 0 ) 
    {
        // 处理Content-Length头部字段
        text += 15;
        text += strspn( text, " \t" );
        m_content_length = atol(text);
    } 
    else if ( strncasecmp( text, "Host:", 5 ) == 0 ) 
    {
        // 处理Host头部字段
        text += 5;
        text += strspn( text, " \t" );
        m_host = text;
    } 
    else 
    {
        printf( "该字段为未知请求头部: %s\n", text );
    }
    return NO_REQUEST;
}
// 解析请求体
// 我们没有真正解析HTTP请求的消息体，只是判断它是否被完整的读入了
http_conn::HTTP_CODE http_conn::parse_content(char* text)
{
    if ( m_read_idx >= ( m_content_length + m_checked_index ) )
    {
        text[ m_content_length ] = '\0';
        return GET_REQUEST;
    }
    return NO_REQUEST;
}

// 解析具体某一行，行的判断依据是 \r\n
http_conn::LINE_STATUS http_conn::parse_line()
{

    char temp;
    for(;m_checked_index < m_read_idx; ++m_checked_index)
    {
        temp = m_read_buf[m_checked_index];
        if(temp == '\r')
        {
            if((m_checked_index+1)==m_read_idx)
            {
                // 正在解析的位置的下一个索引是下一段读缓冲区的开始位置
                // 数据不完整
                return LINE_OPEN;
            }
            else if(m_read_buf[m_checked_index+1]=='\n')
            {
                // 将 \r\n 置为读缓冲区的字符结束符
                m_read_buf[m_checked_index++] = '\0';
                m_read_buf[m_checked_index++] = '\0';
                return LINE_OK;
            }
            return LINE_BAD;
        }
        else if(temp == '\n')
        {
            if((m_checked_index>1)&& (m_read_buf[m_checked_index-1]=='\r'))
            {
                m_read_buf[m_checked_index-1] = '\0';
                m_read_buf[m_checked_index++] = '\0';
                return LINE_OK;
            }
            return LINE_BAD;
        }
    }
    return LINE_OPEN;
}


// 写HTTP响应
// 非阻塞一次性写完所有数据
bool http_conn::write()
{
    int temp = 0;
    int bytes_have_send = 0;    // 已经发送的字节
    int bytes_to_send = m_write_idx;// 将要发送的字节 （m_write_idx）写缓冲区中待发送的字节数
    
    if ( bytes_to_send == 0 ) 
    {
        // 将要发送的字节为0，这一次响应结束。
        modfd( m_epollfd, m_sockfd, EPOLLIN ); 
        init();
        return true;
    }

    while(1) {
        // 分散写
        temp = writev(m_sockfd, m_iv, m_iv_count);
        if ( temp <= -1 ) 
        {
            // 如果TCP写缓冲没有空间，则等待下一轮EPOLLOUT事件，虽然在此期间，
            // 服务器无法立即接收到同一客户的下一个请求，但可以保证连接的完整性。
            if( errno == EAGAIN ) 
            {
                modfd( m_epollfd, m_sockfd, EPOLLOUT );
                return true;
            }
            unmap();
            return false;
        }
        bytes_to_send -= temp;
        bytes_have_send += temp;
        if ( bytes_to_send <= bytes_have_send ) 
        {
            // 发送HTTP响应成功，根据HTTP请求中的Connection字段决定是否立即关闭连接
            unmap();
            if(m_linger) 
            {
                init();
                modfd( m_epollfd, m_sockfd, EPOLLIN );
                return true;
            } else 
            {
                modfd( m_epollfd, m_sockfd, EPOLLIN );
                return false;
            } 
        }
    }
}

// 往写缓冲write_buf中写入待发送的数据
bool http_conn::add_response( const char* format, ... ) 
{
    if( m_write_idx >= WRITE_BUFFER_SIZE ) 
    {
        return false;
    }
    va_list arg_list;
    va_start( arg_list, format );
    int len = vsnprintf( m_write_buf + m_write_idx, WRITE_BUFFER_SIZE - 1 - m_write_idx, format, arg_list );
    if( len >= ( WRITE_BUFFER_SIZE - 1 - m_write_idx ) )
    {
        return false;
    }
    m_write_idx += len;
    va_end( arg_list );
    return true;
}

bool http_conn::add_status_line( int status, const char* title ) 
{
    return add_response( "%s %d %s\r\n", "HTTP/1.1", status, title );
}

bool http_conn::add_headers(int content_len) 
{
    add_content_length(content_len);
    add_content_type();
    add_linger();
    add_blank_line();
}

bool http_conn::add_content_length(int content_len) 
{
    return add_response( "Content-Length: %d\r\n", content_len );
}

bool http_conn::add_linger()
{
    return add_response( "Connection: %s\r\n", ( m_linger == true ) ? "keep-alive" : "close" );
}

bool http_conn::add_blank_line()
{
    return add_response( "%s", "\r\n" );
}

bool http_conn::add_content( const char* content )
{
    return add_response( "%s", content );
}

bool http_conn::add_content_type() 
{
    return add_response("Content-Type:%s\r\n", "text/html");
}

// 根据服务器处理HTTP请求的结果，决定返回给客户端的内容
bool http_conn::process_write(HTTP_CODE ret) 
{
    switch (ret)
    {
        case INTERNAL_ERROR:
            add_status_line( 500, error_500_title );
            add_headers( strlen( error_500_form ) );
            if ( ! add_content( error_500_form ) ) 
            {
                return false;
            }
            break;
        case BAD_REQUEST:
            add_status_line( 400, error_400_title );
            add_headers( strlen( error_400_form ) );
            if ( ! add_content( error_400_form ) ) 
            {
                return false;
            }
            break;
        case NO_RESOURCE:
            add_status_line( 404, error_404_title );
            add_headers( strlen( error_404_form ) );
            if ( ! add_content( error_404_form ) ) 
            {
                return false;
            }
            break;
        case FORBIDDEN_REQUEST:
            add_status_line( 403, error_403_title );
            add_headers(strlen( error_403_form));
            if ( ! add_content( error_403_form ) ) 
            {
                return false;
            }
            break;
        case FILE_REQUEST:
            add_status_line(200, ok_200_title );
            add_headers(m_file_stat.st_size);
            m_iv[ 0 ].iov_base = m_write_buf;
            m_iv[ 0 ].iov_len = m_write_idx;
            m_iv[ 1 ].iov_base = m_file_address;
            m_iv[ 1 ].iov_len = m_file_stat.st_size;
            m_iv_count = 2;
            return true;
        default:
            return false;
    }

    m_iv[ 0 ].iov_base = m_write_buf;
    m_iv[ 0 ].iov_len = m_write_idx;
    m_iv_count = 1;
    return true;
}


// 由线程池中的工作线程调用的，这是处理http请求的入口函数
// 处理客户端的请求
void http_conn::process()
{
    // 解析HTTP请求
    HTTP_CODE read_ret = process_read();
    if ( read_ret == NO_REQUEST ) 
    {
        modfd( m_epollfd, m_sockfd, EPOLLIN );
        return;
    }

    // 生成响应
    bool write_ret = process_write( read_ret );
    if ( !write_ret ) 
    {
        close_conn();
    }
    modfd( m_epollfd, m_sockfd, EPOLLOUT);
}