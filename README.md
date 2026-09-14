# Single-loop epoll server

一个 Linux C11 网络库，采用单个 edge-triggered epoll event loop 和逻辑线程池。

## 设计

- 监听 fd 注册时 `epoll_event.data.ptr == NULL`；连接建立后创建 `epoll_connection`，后续事件的 `data.ptr` 直接指向该结构。
- 每个连接拥有 mutex、FIFO 事件队列和待发送数据队列。
- event loop 负责 accept、收集读/写/错误事件及所有 `epoll_ctl`/close 操作。
- 事件首次进入空闲连接队列时，通过带 mutex/condition variable 的线程池投递。一个连接始终只由一个 worker 串行处理，不同连接可并行。
- ET 读循环持续到 `EAGAIN`；写循环持续到队列清空或 `EAGAIN`。存在待发送数据时注册 `EPOLLOUT`，清空后移除。
- 若连接队列中已有尚未执行的读事件，新读事件会合并丢弃；正在执行的读事件不算“未执行”，避免在 `recv` 返回 `EAGAIN` 边界丢失新 edge。
- 通过 `eventfd` 将 worker 的写关注更新和关闭请求送回 event loop，引用计数保护跨线程连接生命周期。

## 构建和运行

```sh
make
./echo_server 9000
```

另一个终端可执行：

```sh
printf 'hello\n' | nc 127.0.0.1 9000
```

运行并发集成测试：

```sh
make test
```

公共接口位于 `include/epoll_server.h`。`on_data` 在对应连接的逻辑 worker 上调用，可解析协议并调用 `epoll_connection_send`；发送函数会复制传入数据，因此回调返回后原缓冲区可立即复用。
