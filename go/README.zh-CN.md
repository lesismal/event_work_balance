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
  队列，按 worker 数分片）、`taskpool.ModeElastic`（nbio 风格的弹性
  fork/dispatcher）或 `taskpool.ModeAdaptive`（见下，所有后端的默认值，
  `taskpool.New` 也默认使用它）。
- `taskpool.ModeAdaptive` 同样基于 `sync.Cond`、同样分片，worker 空闲时挂在条件
  变量上，但常驻数量随负载在下限和上限之间变化：
  - 扩容：任务入队时没有空闲 worker 可以接手，就新起一个 worker，直到上限。
    已被唤醒、还没开始跑的 worker 不算空闲，所以一批连发的任务不会都指望同一个
    worker。
  - 缩容：后台每隔 `ShrinkInterval`（默认 1 秒）看一次上个周期里**最少**有几个
    worker 空闲，这些 worker 整个周期都没用上，退掉其中一半，但不低于下限。按一半
    退是为了突发过后分几个周期逐步回落，而不是把下一次突发要用的 worker 一次退光。
  - 运行中可以用 `TaskPool.Resize(min, max)` 调整上下限：调高下限立即补足 worker；
    调低上限时，空闲 worker 立即退出，忙碌的在手头任务完成后退出。
    `TaskPool.Workers()` 返回当前 worker 数（其他 Mode 调 `Resize` 返回 false）。
  - 直接使用：`taskpool.NewAdaptive(taskpool.AdaptiveConfig{MinWorkers: 16,
    MaxWorkers: 4096, QueueSize: 10000})`；`NewWithMode(ModeAdaptive, max, queue)`
    的下限默认为每个 P 十个 worker。在 fib 里，`WorkerCount` 是上限，
    `Config.MinWorkerCount` 是下限（0 表示每个 P 十个）。
- 池容量按 Mode 分别给默认值，因为 `WorkerCount` 在不同 Mode 下含义不同
  （ModeAdaptive 与 ModeElastic 一样是上限，默认值也相同）：
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
- `config.SetTaskPool(pool)` 让 Engine 使用外部提供的任务池（实现 `fib.TaskPool`
  接口，`*taskpool.TaskPool` 本身即满足）。设置后 `TaskPoolMode`、
  `WorkerCount`、`SharedTaskPool` 和池容量配置都不再生效；Engine 关闭时不会
  停止该池，由调用方在所有使用它的 Engine 关闭后自行停止。`GoTasks` 返回接受的
  前缀长度，未被接受的任务对应的连接会被关闭，所以池只应在停止时拒绝任务。
- 默认 `WriteBufferHighWatermark` 为 64 KiB，`MaxPendingBytes` 为 1 GiB。
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

### 异步 Dial

`Engine.Dial` 发起四层（TCP）连接，立即返回，不阻塞调用方。连接建立后的 fd 与
accept 进来的连接一样由同一个事件循环管理，走同一个 handler、同一个 worker 池和
同一套背压：

```go
err := server.Dial("tcp", "127.0.0.1:9001", 3*time.Second, func(c *fib.Connection, err error) {
    if err != nil {
        // 连接失败、超时（errors.Is(err, os.ErrDeadlineExceeded)）或 Engine 已关闭
        // （errors.Is(err, net.ErrClosed)），err 为 *net.OpError。
        return
    }
    c.Send([]byte("hello"))
})
```

- network、addr 的含义与 `net.Dial` 一致；host 不是 IP 字面量时在单独的 goroutine
  里解析，调用方不会等 DNS。timeout 为 0 表示只受操作系统自身的连接超时约束。
- 原生后端由事件循环创建非阻塞 socket 并发起 `connect`，fd 随即以边沿触发注册到
  epoll/kqueue，连接结果由它的第一个可写事件报告；Windows 上用 overlapped
  `ConnectEx`，由完成端口报告结果。
- 成功时先调用 handler 的 `OnOpen`，再调用 done；失败时只调用 done（连接为
  nil），handler 不会收到任何回调。done 恰好调用一次，与 `OnOpen` 一样在事件循环
  上执行，不能阻塞；可以传 nil。
- 只有能立即判定无法发起时（未知 network、非法或无法解析的字面量地址、Engine 已
  停止），`Dial` 才直接返回错误，此时 done 不会被调用。
- Engine 关闭时，尚未完成的 Dial 都会以 `net.ErrClosed` 通知 done。
- 兼容后端（如 FreeBSD）用 `net.DialTimeout` 在 goroutine 里连接，done 在该
  goroutine 上执行。
- `DialWithHandler` 让这条连接使用自己的 handler，`OnOpen`、`OnData`、`OnClose`
  都只交给它，Engine 的 handler 收不到；同一个 Engine 因此可以同时承载 server 和
  使用不同协议的 client 连接。`Dial` 等价于 handler 传 nil，即使用 Engine 的。
- `fib.NewEngine(config, handler)` 创建不监听任何地址的 Engine，只用于 Dial 出去
  的连接；它的 `LocalAddr` 返回错误。

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

### 异步 HTTP client

`http.Client` 在 Engine 上发送 HTTP/1.1 请求，调用方不会阻塞。响应由 Engine 的
worker 读取，body 完整缓存后再交给回调；也可以用 `Go` 拿到 Future 等待结果：

```go
engine, _ := fib.NewEngine(fib.DefaultConfig(), nil) // 或直接复用 server 的 Engine
go engine.Run()
client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())

req, _ := http.NewRequest("GET", "http://127.0.0.1:8080/hello", nil)
client.Do(req, func(resp *http.Response, err error) {
    // 响应、错误、超时或取消，恰好回调一次
})

resp, err := client.Go(req).Wait() // Future：Wait 阻塞，Done() 可用于 select
```

- 只支持 `http://`；`https://` 返回 `ErrUnsupportedScheme`。
- 每个 host:port 维护连接池：keep-alive 复用，`MaxConnsPerHost` 限制同时打开或正在
  建立的连接数，超出的请求排队；`MaxIdleConnsPerHost`、`IdleConnTimeout` 控制空闲
  连接的保留。每条连接同时只跑一个请求，不做 pipelining。
- 支持 `Content-Length`、chunked（含 trailer）、以关闭连接为结束的 body，HEAD、
  204、304 不读 body，1xx 中间响应自动跳过。`MaxResponseHeaderBytes`、
  `MaxResponseBodyBytes` 限制单个响应大小。
- `Timeout` 覆盖从 `Do` 到响应完整的全过程（排队、建连、发送、读取），超时错误满足
  `errors.Is(err, os.ErrDeadlineExceeded)`；取消请求的 context 同样会结束请求。被
  放弃的请求所在连接会被关闭。
- 复用的空闲连接如果已被服务端关闭、且没有收到任何响应字节，GET/HEAD/OPTIONS/TRACE
  会在新连接上自动重试一次；其他方法直接返回错误。
- 回调可能在任意 goroutine 上执行：成功的响应在读取它的 worker 上回调，超时和取消在
  定时器 goroutine 上，连接失败在单独的 goroutine 上，`Do` 立即拒绝的请求在调用方
  goroutine 上。回调不要长时间阻塞；开启 `InlineHandlers` 时成功回调会在事件循环上执行。
- `Do` 会在调用方 goroutine 里读完 `req.Body`。
- 先 `client.Close()` 再关闭 Engine：`Close` 让排队中的请求以 `ErrClientClosed` 失败，
  已发出的请求照常完成；直接关闭 Engine 不会通知 client，已发出的请求只能等超时。

## WebSocket 子 package

`websocket` package 实现 RFC 6455 Upgrade 握手、增量帧解析、
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

### 异步 WebSocket client

`Dialer` 在 Engine 上发起 WebSocket 连接并完成握手，调用方不会阻塞。握手成功后，
client 连接与 server 端连接走同一套 `Handler` 回调、同一个 worker 池和同一个
`Connection` 类型：

```go
engine, _ := fib.NewEngine(fib.DefaultConfig(), nil) // 或直接复用 server 的 Engine
go engine.Run()
dialer := websocket.NewDialer(engine, websocket.DefaultDialerConfig())

handler := websocket.HandlerFuncs{
    Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
        // 服务端发来的消息；data 只在回调期间有效
    },
}
dialer.Dial("ws://127.0.0.1:8080/ws", nil, handler, func(c *websocket.Connection, resp *http.Response, err error) {
    if err != nil {
        return // 连接失败、握手被拒（errors.Is(err, websocket.ErrBadHandshake)）或超时
    }
    _ = c.WriteText("hello")
})

conn, resp, err := dialer.Go(url, header, handler).Wait() // Future 形式
```

- 只支持 `ws://`；`wss://` 返回 `ErrUnsupportedScheme`。
- 成功时先调用 handler 的 `OnOpen`（参数是握手请求），再调用 done；失败时只调用
  done，连接为 nil，服务端有响应时一并传入 resp（例如 403），便于查看状态码和头部。
- 握手会校验 101 状态、`Upgrade`/`Connection` 头、`Sec-WebSocket-Accept` 与本次
  key 是否匹配，以及服务端选择的子协议是否在 `Subprotocols` 里；服务端选择了未提供的
  扩展同样视为失败。
- `header` 用于附加 Origin、鉴权等头部；`Upgrade`、`Connection`、
  `Sec-WebSocket-Key`/`Version`/`Protocol`/`Extensions` 由 Dialer 设置，不能传入。
- `HandshakeTimeout` 覆盖建连和握手全过程，超时错误满足
  `errors.Is(err, os.ErrDeadlineExceeded)`。
- client 发出的每一帧都按 RFC 6455 用随机 key 掩码，需要复制一次 payload；收到
  带掩码的服务端帧按协议错误以 1002 关闭。与握手响应同一次读到的首帧也会正常交付。
- done 可能在任意 goroutine 上执行：握手成功在读取它的 worker 上，超时在定时器
  goroutine 上，连接失败在单独的 goroutine 上，URL 非法时在调用方 goroutine 上。

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
