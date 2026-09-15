# Go 版网络库

这是根目录 C11 实现的 Go 移植版，保留相同的核心架构：

- 单个 edge-triggered epoll event loop 独占所有 `epoll_ctl` 和 fd 关闭操作。
- connection 是本地 `taskpool.TaskPool` 的任务单位。TaskPool 参考
  `github.com/lesismal/nbio/taskpool` 的弹性 fork、缓冲排队和任务排空模型，
  connection 不与某个 worker 固定绑定。
- 每个 connection 有 FIFO 事件队列；同一 connection 串行执行，不同 connection 动态负载均衡。
- ET 读写均排空到 `EAGAIN`；仅发送队列非空时关注 `EPOLLOUT`。
- 读 buffer 由 Server 级 `sync.Pool` 复用，大小通过 `Config.ReadBufferSize`
  设置，默认 16 KiB。
- `DefaultConfig` 根据 `runtime.NumCPU()` 计算池容量：`WorkerCount` 默认为
  CPU 线程数乘以 1000，CPU 线程数不少于 4 时下限为 10000；`MaxEvents`
  默认为 CPU 线程数乘以 1000，并限制在 10000～100000。
- `Send` 先直接发送，余量复制进发送队列；可选 `writev` 批量发送。
- worker 通过 command queue 与 `eventfd` 请求 event loop 刷新写关注或关闭连接。

## 平台支持

- Linux：原生 edge-triggered epoll、eventfd 和可选 writev，完整对应 C 版架构。
- macOS、Windows：使用 Go 标准库的系统网络轮询器负责 socket I/O；读取事件仍以
  connection 为单位进入本地 TaskPool，同一连接的回调保持 FIFO 串行执行。

非 Linux 后端保留完全相同的公共 API。`Backlog`、`UseWritev` 和
`OnPriorityData` 对应的 TCP `MSG_OOB` 处理目前仅在 Linux 后端生效；非 Linux
后端的 `Connection.FD()` 返回 `-1`。

## API

```go
config := epoll.DefaultConfig()
config.BindAddress = "127.0.0.1"
config.ReadBufferSize = 32 * 1024
server, err := epoll.Bind(config, epoll.HandlerFuncs{
    Data: func(c *epoll.Connection, data []byte) {
        if c.Send(data) != nil { c.Close() }
    },
	PriorityData: func(c *epoll.Connection, data []byte) {
		// 处理由 EPOLLPRI 触发并通过 MSG_OOB 读取的带外数据。
	},
	Close: func(c *epoll.Connection, err error) {
		// 主动 Close 时 err 为 nil；对端正常关闭时为 io.EOF；
		// 网络或系统调用失败时为对应的原始错误。
	},
})
if err != nil { panic(err) }
defer server.Close()
if err := server.Run(); err != nil { panic(err) }
```

运行示例和测试：

```sh
cd go
go run ./examples/echo_server 9000 true
go test ./...
```
