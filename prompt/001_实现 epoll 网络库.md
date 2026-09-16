### 001

实现epoll网络库：
1. 使用单个epoll eventloop+逻辑线程池
2. 每个fd封装为struct，struct指针作为epoll add event时epoll_event的data参数，方便接收event时直接取到对应的fd的struct
3. 每个fd的struct包括一个mutex和一个事件队列，包括等待发送数据的数组列表（写失败时如果失败是因为发送缓冲区满，则把fd可写事件加入到epoll event中，可写并写完所有待发送数据后则把可写事件移出epoll event）
4. epoll eventloop采用ET模式，处理所有读、写、错误事件，eventloop收到事件后：
   1）如果是读事件并且epoll_event的data字段为空，说明是新建连接，则创建fd的struct并添加该fd的读事件、错误事件；
   2）如果epoll_event的data字段不为空、说明已经建立了连接，则把事件append到fd struct的事件队列尾部，append前判断当前事件是否为队列头，如果是队列头，则通过mutex和cond_t投递给逻辑线程池去执行对应的事件处理（读区+解析数据/写数据/关闭fd），如果不是队列头并且事件为可读事件、则遍历队列中之前的事件、如果有可读事件未执行，则本次事件不需要append到队列中


### 002

epoll_connection_send当前都是把数据放到connection的发送队列、加入可写事件、等待可写事件再发送。
优化为：
1. 如果当前没有待发送的数据，则直接发送，否则加入到发送队列尾部
2. flush_output既支持可配置、是否使用writev提高性能
3. flush_output中单次write/writev之前，从待发送队列中取数据时加锁的粒度要小，可以用一个新的item记录所有取出来并且发送完成的链表，用于flush_output函数退出前清理
4. flush_output直到没有待发送数据时，先清空发送队列、再解锁，以此来保证epoll_connection_send、flush_output之间的并发一致性、避免漏掉添加可写事件导致数据不能及时发送


### 003

优化Connection读写事件处理，优化为更好的背压机制,Connection执行RunTask/process时，依次执行：
1. flushOutput
2. drainPriorityInput
3. drainInput：执行drainInput前先判断Connection当前是否仍有待发送数据，如果有、则不执行drainInput，如果没有则执行drainInput。
4. socketError
5. 1-4都执行完毕后，如果本轮已经执行了flushOutput、但仍然有待发送数据等待发送，并且有可读事件但是没有执行drainInput进行数据读取，则重新设置pendingEvents的可读状态，确保下一次可写事件到来并flushOutput清空剩余待发送数据后可以进行正常数据读取、避免有数据却不能读取成为僵尸连接

