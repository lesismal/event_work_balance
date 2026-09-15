# Go 版单 Event Loop epoll 网络库

这是根目录 C11 实现的 Go 移植版，保留相同的核心架构：

- 单个 edge-triggered epoll event loop 独占所有 `epoll_ctl` 和 fd 关闭操作。
- connection 是本地 `taskpool.TaskPool` 的任务单位。TaskPool 参考
  `github.com/lesismal/nbio/taskpool` 的弹性 fork、缓冲排队和任务排空模型，
  connection 不与某个 worker 固定绑定。
- 每个 connection 有 FIFO 事件队列；同一 connection 串行执行，不同 connection 动态负载均衡。
- ET 读写均排空到 `EAGAIN`；仅发送队列非空时关注 `EPOLLOUT`。
- `Send` 先直接发送，余量复制进发送队列；可选 `writev` 批量发送。
- worker 通过 command queue 与 `eventfd` 请求 event loop 刷新写关注或关闭连接。

## API

```go
config := epoll.DefaultConfig()
config.BindAddress = "127.0.0.1"
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

仅支持 Linux。运行示例和测试：

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
