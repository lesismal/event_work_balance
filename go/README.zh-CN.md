# Go 版网络库

这是根目录 C11 实现的 Go 移植版，保留相同的核心架构：

- 单个 edge-triggered event loop（Linux 上是 epoll，macOS 上是 kqueue，Windows 上是 IOCP）
  独占所有事件注册和 fd 关闭操作。
- connection 是本地 `taskpool.TaskPool` 的任务单位。TaskPool 使用常驻、有界
  worker，避免短事件触发大量 goroutine 创建和栈扩容；connection 不与某个
  worker 固定绑定。
- 每个 connection 有 FIFO 事件队列；同一 connection 串行执行，不同 connection 动态负载均衡。
- ET 读写均排空到 `EAGAIN`；仅发送队列非空时关注 `EPOLLOUT`。
- 每轮事件处理先 flush 发送队列，再读 OOB，再读普通数据；发送队列仍有数据时跳过读取，并把可读状态保留到下一轮，待可写事件清空队列后再读，既限制用户态缓冲又不会漏读。
- 读 buffer 由 Engine 级 `sync.Pool` 复用，大小通过 `Config.ReadBufferSize`
  设置，默认 16 KiB。
- `Config.TaskPoolMode` 可选 `taskpool.ModeCond`（基于 `sync.Cond` 的有界环形
  队列，按 worker 数分片）或 `taskpool.ModeElastic`（nbio 风格的弹性
  fork/dispatcher，原生后端默认）。
- 池容量按 Mode 分别给默认值，因为 `WorkerCount` 在两个 Mode 下含义不同：
  ModeCond 会预先创建这么多协程并让它们挂在条件变量上，这个数就是实际存在的
  协程数量，多了只是让调度器在同样的核上搬运更多协程；ModeElastic 则是按需
  fork、空闲短暂驻留后回收，这个数是上限而不是实际数量，调高在负载没到之前
  不产生开销。`DefaultPoolSizing(mode)` 返回对应 Mode 的默认值。
- `SetTaskPoolMode(mode)` 切换 Mode 时会同时把 `WorkerCount`、`MaxEvents`
  换成该 Mode 的默认值；`SetPoolSizing(workerCount, maxEvents)` 固定为自己的
  取值，之后再调 `SetTaskPoolMode` 也不会被覆盖，两者调用顺序无关。传 0 表示
  该项保持不变：

  ```go
  config := fib.DefaultConfig()
  config.SetTaskPoolMode(taskpool.ModeCond)  // 容量随之切到 cond 的默认值
  config.SetPoolSizing(500, 10000)           // 固定成自己的取值
  ```
- 背压有两道界，暂停读的原因只能是其中之一，`Engine.Stats()` 会分别计数
  （`ReadsPausedByWatermark`、`ReadsPausedByBudget`、`ReadsResumed`、
  `PendingBytes`）：
  - `WriteBufferHighWatermark` 只看这条连接自己积压了多少，是对端跟不上；
  - `MaxPendingBytes` 是整个 server 共享的总量，被别的连接耗尽时，**一条自己
    几乎没有积压的连接也会被暂停读**。排查「水位线明明远大于单条消息却触发了
    背压」时，先看这两个计数哪个在涨。
- 注意水位线要比**一轮读取**产生的回包总量大，而不只是比单条消息大：一轮读取
  是 corked 的，这一轮内所有回包先进队列、轮末一次性 flush，所以即使 TCP 发送
  缓冲区是空的，队列里也会短暂地存在这一轮的全部回包；一轮最多读
  `ReadBufferSize` 字节。严格一问一答、单条 1KiB、水位线 8KiB 这种配置有 8 倍
  余量，不会触发背压（`TestPingPongUnderWatermarkNeverPausesReads` 固定了这一点）。
- `SharedTaskPool` 默认开启；同一进程内配置相同的多个 Engine 共享 worker
  和任务队列，避免多监听端口重复创建大量 goroutine 与队列。需要完全隔离时
  可显式设为 `false`。
- `Send` 先直接发送，余量复制进发送队列。默认启用自适应 writev：单缓冲走
  write，`SendParts` 的两段数据和包含多个缓冲的发送队列走 writev；仅在背压
  时复制未发送部分。
- worker 通过 command queue 与 `eventfd` 请求 event loop 刷新写关注或关闭连接。

## 平台支持

三个原生后端共用同一套 worker 调度、发送队列和背压逻辑，只有事件来源不同：

- Linux：edge-triggered epoll，`eventfd` 唤醒，可选 `writev`，完整对应 C 版架构。
- macOS：kqueue，所有过滤器以 `EV_CLEAR` 注册，语义与 epoll ET 一致；
  `EVFILT_USER` 唤醒；带外数据由 `EVFILT_EXCEPT`/`NOTE_OOB` 报告；`writev`
  经由 libc。暂停读取时删除读过滤器，恢复时重新添加，添加时会立即报告 socket
  里已有的数据。
- Windows：I/O 完成端口（IOCP），在其上模拟就绪模型。可读由零字节的 overlapped
  `WSARecv` 报告，worker 随后用非阻塞接收排空 socket；非阻塞发送写不完时，剩余
  数据交给 overlapped `WSASend`，它的完成就相当于可写事件。连接用 `AcceptEx`
  接入，唤醒用 `PostQueuedCompletionStatus`。`UseWritev` 对应多个 `WSABUF`
  的一次 `WSASend`。

Windows 后端不会调用 `OnPriorityData`：零字节读不报告带外数据。Windows 上
`Connection.FD()` 返回 socket handle。

其他系统（如 FreeBSD）使用 Go 标准库网络轮询器的兼容后端：公共 API 相同，读取
事件仍以 connection 为单位进入 TaskPool 并保持 FIFO 串行执行，但 `Backlog`、
`UseWritev`、带外数据和背压统计不生效，`Connection.FD()` 返回 `-1`。

## API

```go
import fib "github.com/lesismal/fib/go"

config := fib.DefaultConfig()
// Network、Addr 与标准库 net.Listen 的参数含义一致：
// Network 取 "tcp"、"tcp4"、"tcp6"，Addr 形如 ":9000"、"127.0.0.1:9000"、"[::1]:9000"，
// 端口为 0 时由内核分配。Network 为空按 "tcp" 处理，Addr 为空按 ":0" 处理。
config.Network = "tcp"
config.Addr = "127.0.0.1:9000"
config.ReadBufferSize = 32 * 1024
server, err := fib.Bind(config, fib.HandlerFuncs{
    Data: func(c *fib.Connection, data []byte) {
        if c.Send(data) != nil { c.Close() }
    },
	PriorityData: func(c *fib.Connection, data []byte) {
		// 处理由 EPOLLPRI 触发并通过 MSG_OOB 读取的带外数据。
	},
	Close: func(c *fib.Connection, err error) {
		// 主动 Close 时 err 为 nil；对端正常关闭时为 io.EOF；
		// 网络或系统调用失败时为对应的原始错误。
	},
})
if err != nil { panic(err) }
defer server.Close()
if err := server.Run(); err != nil { panic(err) }
```

一个 server 监听多个地址时用 `Addrs`（此时 `Addr` 被忽略），它们共用同一个事件循环、
描述符表、任务池和缓冲池；`LocalAddrs()` 按配置顺序返回各监听地址，端口为 0 的会
返回内核实际分配的端口：

```go
config.Addrs = []string{"127.0.0.1:9000", "127.0.0.1:9001"}
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
server, err := fib.Bind(config, handler)
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
server, err := fib.Bind(config, handler)
```

运行 WebSocket echo 示例：

```sh
cd go
go run ./examples/websocket_server
```

## GOMAXPROCS

工作协程在自己的协程里直接执行连接的 read/write，而协程进入系统调用时会一直占着它的 P，
直到调度器把这个 P 收回转交出去。所以 GOMAXPROCS 等于核数时，核会在等这次转交的过程中空转：
10 万连接的 echo 压测里，进程在分到的 5 个核上只用掉 2.3 个核，execution trace 显示 2 秒窗口内
有 872 秒的「已就绪但没在运行」时间，几乎全部落在被事件循环唤醒的 worker 上。

把 GOMAXPROCS 设成核数的 2 倍即可：同一份构建下 echo 从 330k/s 提升到 415k/s，建连从 55k/s
提升到 71k/s，TP99 从 145ms 降到 69ms。这是使用方的选择，库不会去改这个全局设置：

```go
runtime.GOMAXPROCS(2 * runtime.NumCPU())
```

需要注意 `runtime.NumCPU()` 取的是本进程的 CPU 亲和性掩码，被 taskset 或 cpuset 限制时它已经
是实际可用的核数。

## InlineHandlers

`Config.InlineHandlers` 让事件循环直接执行连接的这一轮处理，不再交给工作协程。省掉这次交接
在高消息速率下很可观：同一压测里 echo 446k/s 对 395k/s，建连 104k/s 对 95k/s。

代价是 handler 会阻塞它所在的整个 server —— 在它返回之前，事件循环无法收事件、无法 accept、
也无法服务这个 server 上的其他连接。只有在所有 handler 都很短且不会阻塞时才开启；会做 I/O、
抢锁或执行不定长工作的 handler 应该继续走工作协程池，这个池存在的意义正是让一条慢连接不拖住其他连接。
