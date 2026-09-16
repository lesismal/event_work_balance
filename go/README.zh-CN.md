# Go 版网络库

这是根目录 C11 实现的 Go 移植版，保留相同的核心架构：

- 单个 edge-triggered epoll event loop 独占所有 `epoll_ctl` 和 fd 关闭操作。
- connection 是本地 `taskpool.TaskPool` 的任务单位。TaskPool 使用常驻、有界
  worker，避免短事件触发大量 goroutine 创建和栈扩容；connection 不与某个
  worker 固定绑定。
- 每个 connection 有 FIFO 事件队列；同一 connection 串行执行，不同 connection 动态负载均衡。
- ET 读写均排空到 `EAGAIN`；仅发送队列非空时关注 `EPOLLOUT`。
- 读 buffer 由 Server 级 `sync.Pool` 复用，大小通过 `Config.ReadBufferSize`
  设置，默认 16 KiB。
- `DefaultConfig` 根据 `runtime.GOMAXPROCS(0)` 计算池容量：`WorkerCount`
  默认等于可运行的 Go 线程数；`MaxEvents` 默认为其 256 倍，并限制在
  4096～65536。
- `Config.TaskPoolMode` 可选择 `taskpool.ModeCond`（默认，基于 `sync.Cond`
  的有界环形队列）、`taskpool.ModeFixed`（channel worker）或
  `taskpool.ModeElastic`（nbio 风格的弹性 fork/dispatcher）。
- `Send` 先直接发送，余量复制进发送队列。默认启用自适应 writev：单缓冲走
  write，`SendParts` 的两段数据和包含多个缓冲的发送队列走 writev；仅在背压
  时复制未发送部分。
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

## HTTP 子 package

`http` package 在原始连接之上提供 HTTP/1.0、HTTP/1.1 的增量解析和响应处理，
支持 TCP 分包/粘包、流水线请求、`Content-Length`、chunked body、trailer、
keep-alive 以及请求大小限制：

```go
handler := epollhttp.NewHandler(epollhttp.HandlerFunc(
    func(c *epollhttp.Context, request *http.Request) {
        _ = c.Respond(http.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
    },
))
server, err := epoll.Bind(config, handler)
```

完整示例：

```sh
cd go
go run ./examples/http_server
```

## WebSocket 子 package

`http/websocket` package 实现 RFC 6455 Upgrade 握手、增量帧解析、
分片消息重组、客户端掩码校验、Ping/Pong、Close 握手、子协议协商和消息大小限制：

```go
handler := websocket.NewHandler(websocket.HandlerFuncs{
    Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
        _ = c.WriteMessage(opcode, data)
    },
})
server, err := epoll.Bind(config, handler)
```

运行 WebSocket echo 示例：

```sh
cd go
go run ./examples/websocket_server
```
