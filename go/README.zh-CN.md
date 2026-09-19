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

三个原生后端都支持 UDP（见下文 UDP 一节）。

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

### SendFile 零拷贝发送

`Connection.SendFile(f, offset, count)` 把文件的一段排进发送队列，与前后的 `Send` 保持
顺序。Linux 和 macOS 上由 `sendfile(2)` 直接从文件发到 socket，数据不经过用户态；
Windows 上按 socket 的发送进度每次读取一块再发送。无论哪种方式，文件都是随对端的接收
进度读取的，大文件也不会占用更多内存：

```go
f, _ := os.Open("video.mp4")
_ = c.Send(header)
_ = c.SendFile(f, 0, size) // 连接使用自己复制的描述符，f 可以立即关闭
f.Close()
```

- 带 layer 的连接（如 TLS）需要先加密，无法零拷贝：`SendFile` 会在返回前读出这段文件
  并经 layer 发送。UDP 连接返回 `ErrSendFileDatagram`。
- 文件比声明的范围短时会关闭连接，因为对端已经在等这些字节。
- `OnData` 里的发送会被暂存（cork），在 handler 返回后合并成一次写入；需要在 handler
  运行期间就把已发送的数据交给 socket 时（例如流式响应），调用 `Connection.Flush()`。

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

### Unix Socket

`Config.Network` 设为 `"unix"` 时，`Config.Addr` 是 socket 文件路径，Engine 在
Unix domain stream socket 上监听；`Dial`/`DialWithHandler` 的 network 传 `"unix"`、
addr 传路径即可拨号。连接的调度、发送队列、背压、TLS 以及 http/websocket 子
package 与 TCP 完全相同：

```go
config.Network = "unix"
config.Addr = "/tmp/fib.sock"
server, err := fib.Bind(config, handler)

err = engine.Dial("unix", "/tmp/fib.sock", 3*time.Second, done)
```

- 语义与 `net.Listen("unix", path)` 一致：路径已存在时 `Bind` 失败（不会删除别人的
  socket 文件），Engine `Close` 时删除自己创建的文件；Linux 上以 `@` 开头的路径是
  abstract socket，没有文件。
- `Connection.RemoteAddr()` 返回 `*net.UnixAddr`（对端未 bind 时名字为空）；
  `Engine.ListenAddrs()` 返回所有监听地址（TCP、Unix、UDP 均可），`LocalAddr` 只
  报告 TCP 地址。
- macOS 的 Unix socket 没有带外数据，不会调用 `OnPriorityData`；Linux 5.15 起支持。
- Windows 10 1803 起支持 AF_UNIX。由于 `ConnectEx` 只支持 TCP，Windows 上的 Unix
  拨号在 event loop 上使用普通 `connect`（与 net 包相同），本地连接会立即完成或被拒绝。
- 目前只支持 stream 类型（`"unix"`），不支持 `"unixgram"` 和 `"unixpacket"`。

### UDP

`Config.Network` 设为 `"udp"`、`"udp4"` 或 `"udp6"` 时，`Bind` 绑定 UDP socket；
`Dial`/`DialWithHandler` 的 network 传 `"udp"` 时拨号一个已 connect 的 UDP socket。
UDP 沿用同一套 Handler、worker 和 `Connection`：

```go
config.Network = "udp"
config.Addr = "127.0.0.1:9001"
server, err := fib.Bind(config, fib.HandlerFuncs{
    Data: func(c *fib.Connection, datagram []byte) { _ = c.Send(datagram) },
})
addr, _ := server.LocalUDPAddr()
```

- 监听 socket 上，每个对端地址是一条独立的 `Connection`：该地址的第一个数据报触发
  `OnOpen`，之后它的数据报都交给这条连接；`Connection.RemoteAddr()` 返回对端地址，
  `IsUDP()` 返回 true。UDP 没有关闭报文，对端静默超过 `Config.UDPIdleTimeout`
  （默认 `DefaultUDPIdleTimeout`，60 秒；负数表示不超时）后连接被关闭，`OnClose`
  收到 `ErrUDPIdleTimeout`。拨号出去的 UDP 连接不做超时。
- 数据报边界保持不变：每次 `OnData` 恰好是一个数据报，每次 `Send`/`SendParts`
  恰好发送一个数据报。
- UDP socket 由 event loop 自己读取（level-triggered，每轮每个 socket 最多读
  256 个数据报），按对端地址分发到各连接的队列，再由 worker 依次调用 `OnData`；
  每条连接最多排队 1024 个数据报，超出的直接丢弃，和内核接收缓冲满时一样。
- 发送直接写 socket，从不排队：socket 没有空间时这个数据报被丢弃，`Send` 返回错误，
  连接保持打开。因此 UDP 连接不参与写水位和背压。
- Windows 上监听 socket 用 overlapped `WSARecvFrom`、拨号 socket 用带缓冲区的
  overlapped `WSARecv` 接收，并关闭 `SIO_UDP_CONNRESET`，以免某个对端的 ICMP
  端口不可达导致整个监听 socket 的接收失败。兼容后端用 `net.ListenUDP` 实现同样的语义。

运行测试：

```sh
cd go
go test ./...
```

## TLS 子 package

`tls` package 以 handler 包装的方式在 Engine 的连接上提供 TLS，加解密由标准库
`crypto/tls` 完成，不需要额外依赖：

```go
import fibtls "github.com/lesismal/fib/go/tls"

// server：把任意 handler 包在 fibtls.NewServer 里
server, err := fib.Bind(config, fibtls.NewServer(tlsConfig, handler))

// client：config 没有 ServerName 时取 addr 的 host
err = fibtls.Dial(engine, "tcp", "example.com:443", 3*time.Second, tlsConfig, handler, done)
```

- 被包装的 handler 看到的是普通连接：`OnData` 收到的是明文，`Send`、`SendOwned`、
  `SendParts`、`CloseAfterSend` 自动加密，所以 http、websocket 子 package 不用改动
  就能跑在 TLS 上（它们的 client 通过 `ClientConfig.TLSConfig`、
  `DialerConfig.TLSConfig` 支持 https://、wss://）。`CloseAfterSend` 会先发送 close_notify。
- TCP 和 Unix socket 上都可以使用。
- `OnOpen`（以及 Dial 的 done）在连接建立后立即调用，早于 TLS 握手；此时就可以
  `Send`，数据会先缓存，握手完成后按顺序加密发出。握手失败或超时会关闭连接，
  `OnClose` 收到对应错误。
- `crypto/tls` 的握手是阻塞调用，所以每条连接的握手在单独的 goroutine 里进行，握手
  结束后 goroutine 退出；之后的记录由 worker 在 `OnData` 中非阻塞解密，跨多轮读到达
  的记录会被正确拼接。
- `Handler.HandshakeTimeout` 限制握手时长，0 表示 `DefaultHandshakeTimeout`
  （10 秒），负数表示不限制。
- `fibtls.ConnectionState(c)` 在握手完成后返回协商结果（版本、ALPN、对端证书等）。
- 这个 package 只使用 fib 的公开 API：它通过 `Connection.SetLayer` 安装一个
  `fib.Layer` 来加密发送，用 `SendRaw`、`CloseAfterSendRaw` 直接写 socket，用
  `CloseWithError` 把握手失败的原因交给 `OnClose`。其他加密或分帧协议也可以用同样
  的方式接入；本库不内置 DTLS，UDP 上需要加密时可以用这个接口接入自己选择的实现。

## HTTP 子 package

`http` package 在原始连接之上提供 HTTP/1.0、HTTP/1.1 和 HTTP/2 的增量解析和响应
处理，支持 TCP 分包/粘包、流水线请求、`Content-Length`、chunked body、trailer、
keep-alive、流式响应、sendfile 零拷贝发送文件以及请求大小限制：

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
go run ./examples/http/nontls/server   # 另开终端：go run ./examples/http/nontls/client
```

### HTTP/1.x：流式响应、trailer 与 sendfile

除了 `Respond`/`WriteResponse` 一次性回复，`Context` 还实现了 `http.ResponseWriter`、
`http.Flusher` 和 `io.ReaderFrom`，可以像 `net/http` 的 handler 一样边写边发，也可以直接
交给 `net/http` 的 `ServeFile`、`ServeContent`、`FileServer` 等函数：

```go
func(c *fibhttp.Context, r *http.Request) {
    switch r.URL.Path {
    case "/events":
        c.Header().Set("Trailer", "X-Count")
        for i := 0; i < 10; i++ {
            fmt.Fprintf(c, "event %d\n", i)
            c.Flush() // 立即发出这一块，不等 handler 返回
        }
        c.Header().Set("X-Count", "10") // 作为 trailer 在 body 之后发送
    case "/download":
        http.ServeFile(c, r, "big.iso") // Range、If-Modified-Since 等由 net/http 处理，文件走 sendfile
    }
}
```

- 帧格式自动选择：handler 设置了 `Content-Length`，或写的内容不超过 4KB 且在返回前写完，
  就用 `Content-Length`；否则 HTTP/1.1 用 chunked（trailer 也靠它携带），HTTP/1.0 不支持
  chunked，body 以关闭连接结束。未设置 `Content-Type` 时像 `net/http` 一样嗅探。
- trailer：在 `Trailer` 头里声明名字，写完 body 后设置对应的值；也可以在任何时候设置带
  `http.TrailerPrefix` 前缀的键。`Response.Trailer` 同样可以随 `WriteResponse` 发送。
  HTTP/1.0 客户端收不到 trailer。
- `ReadFrom`（`io.Copy`、`http.ServeContent` 都会用到）遇到普通文件，或包着文件的
  `*io.LimitedReader` 时，通过 `Connection.SendFile` 发送；TLS 连接退化为读取后加密发送。
- handler 返回时响应自动结束（写出剩余数据、结束 chunk、发送 trailer），与 `net/http`
  一致；也可以提前调用 `Context.Finish()`。只有用过这些方法的响应才会被自动结束，所以
  没有开始写的响应仍然可以之后在其他 goroutine 中用 `WriteResponse` 异步回复。
- 同一个响应不能混用两种方式：开始用 `Write`/`WriteHeader` 之后，`WriteResponse` 返回
  `ErrResponseWritten`。
- HTTP/2 和 HTTP/3 上这些方法同样可用，但响应会先缓存，handler 返回后整体发送。

服务端还会：为 HTTP/1.1 缺少 `Host` 或有多个 `Host` 的请求返回 400；不支持的
`Transfer-Encoding` 返回 501；无法满足的 `Expect` 返回 417；同时带 `Content-Length`
和 chunked 的请求按 chunked 处理并在响应后关闭连接；HTTP/1.0 请求带
`Transfer-Encoding` 时返回 400；204 和 1xx 响应不带 `Content-Length`；自动添加 `Date`。
完整的 HTTP/1.x 支持情况、限制和一致性测试见
[`docs/http1.zh-CN.md`](../docs/http1.zh-CN.md)。

### HTTP/2

同一个 `ServerHandler` 同时服务 HTTP/1 和 HTTP/2，不需要额外配置：连接以 HTTP/2
preface 开头时按 HTTP/2 处理，否则按 HTTP/1。这覆盖了三种场景：

- TLS + ALPN（h2）：用 `fibhttp.ConfigureTLS` 让 TLS 配置在 ALPN 中优先提供 `h2`，
  浏览器、`net/http` 等客户端就会选择 HTTP/2；不支持 h2 的客户端继续走 HTTP/1.1。
- 明文 HTTP/2（h2c，prior knowledge）：客户端直接发送 preface 即可。
- 明文 HTTP/1.1 升级（`Upgrade: h2c` + `HTTP2-Settings`，`curl --http2 http://...` 用的
  就是这种方式）：服务端回复 101，随后在 stream 1 上用 HTTP/2 回复这个请求，连接之后
  按 HTTP/2 继续。TLS 连接只通过 ALPN 切换，不接受这种升级。

```go
tlsConfig = fibhttp.ConfigureTLS(tlsConfig) // ALPN: h2, http/1.1
server, err := fib.Bind(config, fibtls.NewServer(tlsConfig, fibhttp.NewHandler(handler)))
```

- Handler 不用改：每个 stream 就是一个普通的 `*http.Request`（`Proto` 为 `HTTP/2.0`），
  用 `Context.Respond`/`WriteResponse` 回复即可，响应会被编成该 stream 的 HEADERS/DATA
  帧。HTTP/2 连接上不要直接调用 `Context.Conn.Send`。
- 多路复用：一个连接上的多个 stream 并发进行，handler 可以在其他 goroutine 中稍后
  回复（异步响应），不同 stream 的响应互不阻塞。
- `Response.Close` 在 HTTP/2 上优雅关闭连接：发送 GOAWAY(NO_ERROR)，不再接受新
  stream，已在处理的 stream 完成后再关闭连接。
- 流控：遵守对端的连接级与 stream 级窗口，窗口不足的响应 body 暂存，等 WINDOW_UPDATE
  后继续发送；接收方向每个 stream 窗口 1MB、连接窗口 16MB，按消费量自动补充。
- 实现了 HPACK（含 Huffman 与动态表）、CONTINUATION、trailer、多个 cookie 字段合并、
  PING、SETTINGS、RST_STREAM、GOAWAY，以及 RFC 9113 的请求合法性校验（非法请求用
  RST_STREAM 拒绝，协议错误用 GOAWAY 关闭连接）。
- `Config.MaxConcurrentStreams` 限制单连接并发 stream 数（默认 250，超出的 stream 被
  REFUSED_STREAM 拒绝）；`MaxHeaderBytes`、`MaxBodyBytes` 同样作用于 HTTP/2，超限时
  返回 431/413。`Config.DisableHTTP2` 关闭 HTTP/2，只服务 HTTP/1。
- 通过 TLS 到达的请求（HTTP/1 和 HTTP/2）都会设置 `Request.TLS`，可以读取 ALPN 结果、
  对端证书等。

#### Server push

`Context` 实现了 `http.Pusher`：在回复之前调用 `Push`，服务端先发送 PUSH_PROMISE，
再用同一个 handler 处理被推送的请求，响应在它自己的 stream 上发送；`Push` 在被推送
请求的 handler 返回后才返回：

```go
func(c *fibhttp.Context, r *http.Request) {
    if r.URL.Path == "/index.html" {
        _ = c.Push("/style.css", nil) // 客户端不支持时返回 http.ErrNotSupported，忽略即可
    }
    _ = c.Respond(http.StatusOK, "text/html", page)
}
```

- target 是绝对路径，或 scheme 与当前请求相同的绝对 URL；`PushOptions` 可指定方法
  （GET 或 HEAD）和请求头，不能带 body 相关及连接相关的头。
- 遵守客户端的设置：`SETTINGS_ENABLE_PUSH=0` 时返回 `http.ErrNotSupported`，推送中的
  stream 数达到客户端的 `SETTINGS_MAX_CONCURRENT_STREAMS` 时返回 `ErrPushLimit`。
  客户端可以用 RST_STREAM 取消推送，不影响连接。
- HTTP/1、被推送的请求本身再调用 `Push` 时返回 `http.ErrNotSupported`。
- 主流浏览器和 Go 的 `net/http` 客户端都已关闭 push，本库的 client 同样不接收 push
  （SETTINGS_ENABLE_PUSH=0）；需要预加载时更推荐 103 Early Hints。

#### 1xx 中间响应

`Context.WriteInterim(status, header)` 在最终响应之前发送 1xx 中间响应，HTTP/1.1 和
HTTP/2 都支持，例如 103 Early Hints：

```go
_ = c.WriteInterim(http.StatusEarlyHints, http.Header{"Link": {"</style.css>; rel=preload"}})
_ = c.Respond(http.StatusOK, "text/html", page)
```

- 可以调用多次，必须在 `WriteResponse` 之前；不允许 101；HTTP/1.0 请求返回
  `http.ErrNotSupported`。
- 带 `Expect: 100-continue` 的请求会自动收到 100 Continue（HTTP/1.1 与 HTTP/2），
  因为 body 总是在 handler 运行前完整读取，客户端不必等待超时才发送 body。

#### 限制与待优化

HTTP/2 当前的限制（body 整体缓存、handler 同步执行、固定的窗口参数等）、有意未实现的
功能及原因、以及待优化项（安全加固、h2spec 一致性测试、流式 body、性能等）见
[`docs/http2.zh-CN.md`](../docs/http2.zh-CN.md)。

### 异步 HTTP client

`http.Client` 在 Engine 上发送 HTTP/1.0、HTTP/1.1 和 HTTP/2 请求，调用方不会阻塞。响应由 Engine 的
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

- 支持 `http://` 和 `https://`；https 使用 `ClientConfig.TLSConfig`（nil 表示默认
  配置，未设置 ServerName 时取 URL 的 host）。其他 scheme 返回 `ErrUnsupportedScheme`。
  http 与 https 即使地址相同也各自使用独立的连接池。
- HTTP/2：https 默认在 ALPN 中提供 `h2`、`http/1.1`（`TLSConfig.NextProtos` 已设置时
  以它为准），服务端选择 h2 时自动走 HTTP/2，一个连接并发承载多个请求（数量受服务端
  `MAX_CONCURRENT_STREAMS` 限制），对同一 host 只保持必要数量的连接。
  `ClientConfig.DisableHTTP2` 强制 HTTP/1.1；`ClientConfig.UnencryptedHTTP2` 让 http://
  以 prior knowledge 方式直接使用明文 HTTP/2（h2c）。HTTP/2 上超时或取消只会用
  RST_STREAM 结束对应 stream，不影响同连接上的其他请求；收到 GOAWAY 或
  REFUSED_STREAM 时，服务端未处理的请求会自动在新连接上重发。
- 每个 host:port 维护连接池：keep-alive 复用，`MaxConnsPerHost` 限制同时打开或正在
  建立的连接数，超出的请求排队；`MaxIdleConnsPerHost`、`IdleConnTimeout` 控制空闲
  连接的保留。HTTP/1.1 连接同时只跑一个请求，不做 pipelining。
- 支持 `Content-Length`、chunked（含 trailer）、以关闭连接为结束的 body，HEAD、
  204、304 不读 body，1xx 中间响应自动跳过。请求的 `ContentLength` 为 -1 时 body 以
  chunked 发送，可以携带 `req.Trailer`。
- HTTP/1.0：把请求的 `Proto`、`ProtoMajor`、`ProtoMinor` 设为 `HTTP/1.0`、1、0，就会以
  HTTP/1.0 发送（body 用 `Content-Length`，不带 trailer）；请求带
  `Connection: keep-alive` 且服务端同意时连接才会复用。`MaxResponseHeaderBytes`、
  `MaxResponseBodyBytes` 限制单个响应大小。
- `Timeout` 覆盖从 `Do` 到响应完整的全过程（排队、建连、发送、读取），超时错误满足
  `errors.Is(err, os.ErrDeadlineExceeded)`；取消请求的 context 同样会结束请求。被
  放弃的请求所在的 HTTP/1.1 连接会被关闭。
- 复用的空闲连接如果已被服务端关闭、且没有收到任何响应字节，GET/HEAD/OPTIONS/TRACE
  会在新连接上自动重试一次；其他方法直接返回错误。
- 回调可能在任意 goroutine 上执行：成功的响应在读取它的 worker 上回调，超时和取消在
  定时器 goroutine 上，连接失败在单独的 goroutine 上，`Do` 立即拒绝的请求在调用方
  goroutine 上。回调不要长时间阻塞；开启 `InlineHandlers` 时成功回调会在事件循环上执行。
- `Do` 会在调用方 goroutine 里读完 `req.Body`。
- 先 `client.Close()` 再关闭 Engine：`Close` 让排队中的请求以 `ErrClientClosed` 失败，
  已发出的请求照常完成；直接关闭 Engine 不会通知 client，已发出的请求只能等超时。

## HTTP/3 子 package

`http3` package 在 Engine 的 UDP socket 上提供 HTTP/3（RFC 9114），其下的 QUIC
（RFC 9000/9001/9002）和 QPACK（RFC 9204）都在本库内实现，TLS 1.3 握手使用标准库的
`crypto/tls.QUICConn`，不引入任何第三方依赖。Handler 与 `http` package 完全相同，
同一个 `fibhttp.Handler` 可以同时服务 HTTP/1.1、HTTP/2 和 HTTP/3：

```go
config := fib.DefaultConfig()
config.Network = "udp"
config.Addr = ":443"
server, err := fib.Bind(config, http3.NewHandler(tlsConfig, handler))
```

- 服务端就是一个 UDP Engine，handler 用 `http3.NewHandler`（或 `NewHandlerWithConfig`
  调整 `MaxHeaderBytes`、`MaxBodyBytes`、`MaxConcurrentStreams`、`MaxIdleTimeout`）。
  TLS 配置里的 ALPN 会自动加上 `h3`。
- Engine 按对端地址区分 UDP 连接，每个发来 QUIC Initial 的地址对应一个 QUIC 连接，挂在
  该地址的 `fib.Connection` 上；因此不支持连接迁移（NAT 重绑定后连接会失效），服务端在
  传输参数里声明 `disable_active_migration`。没有连接状态的短包头包会收到 stateless
  reset，让对端立即结束而不是等超时；不认识的 QUIC 版本收到 Version Negotiation。
- QUIC 的空闲超时（`MaxIdleTimeout`，默认 30 秒）应小于 Engine 的
  `Config.UDPIdleTimeout`（默认 60 秒），否则 Engine 会先把静默的对端连接关掉。
- Handler 不用改：请求的 `Proto` 为 `HTTP/3.0`，`Request.TLS` 带有 TLS 状态，用
  `Context.Respond`/`WriteResponse` 回复，响应被编成该 stream 的 HEADERS/DATA 帧；
  可以在其他 goroutine 中异步回复。`WriteInterim` 支持 1xx 中间响应（如 103 Early
  Hints），`Expect: 100-continue` 自动回复 100。`Push` 返回 `http.ErrNotSupported`：
  HTTP/3 的 push 需要客户端先发 MAX_PUSH_ID，浏览器都不这样做。
- `Response.Close` 在 HTTP/3 上优雅关闭：发送 GOAWAY，不再接受新请求，已在处理的请求
  的响应全部送达后再关闭连接。
- 请求 body 超过 `MaxBodyBytes` 返回 413，header 超过 `MaxHeaderBytes` 返回 431；
  非法请求（缺少伪头部、大写字段名、连接相关字段等）用 H3_MESSAGE_ERROR 重置该
  stream，协议错误（控制流不以 SETTINGS 开头、DATA 出现在 HEADERS 之前、引用动态表等）
  按 RFC 9114 的错误码关闭连接。
- 浏览器通过 HTTP/1.1 或 HTTP/2 响应里的 `Alt-Svc` 头发现 HTTP/3，`http3.AltSvc(port)`
  生成这个头的值；通常在同一端口号上同时跑 TCP 的 HTTPS 和 UDP 的 HTTP/3（见示例）。

QUIC 层的实现要点：

- 包保护支持 TLS 1.3 的三种套件：AES-128-GCM、AES-256-GCM 和
  ChaCha20-Poly1305（标准库没有导出 ChaCha20-Poly1305，本库按 RFC 8439 自行实现，
  并用 RFC 9001 附录的测试向量校验）；支持对端发起的密钥更新（key update）；
  客户端支持 Retry。不支持 0-RTT。
- 丢包检测与拥塞控制按 RFC 9002 实现：ACK 阈值和时间阈值判定丢包、PTO 探测、
  NewReno 拥塞控制；服务端在验证客户端地址前遵守 3 倍放大限制。发出的数据报固定为
  1200 字节（所有 IPv6 路径都能承载的大小），因此不需要 PMTU 探测。
- 流控：遵守对端的连接级和 stream 级限制；接收方向每个 stream 窗口 1MB、连接窗口
  16MB，按消费量自动补充；并发 stream 数用 MAX_STREAMS 动态放开。
- QPACK 两端都声明动态表容量为 0：编码只用静态表和字面量（字面量在更短时使用
  Huffman 编码），解码拒绝任何动态表引用，因此编码器流和解码器流无需内容。
- 已与 quic-go 做双向互通测试（包括 5% 随机丢包下的 20MB 双向传输），客户端也验证过
  Cloudflare、Google、nginx、Facebook、Varnish、quiche 的线上 HTTP/3 服务。

#### 限制与待优化

HTTP/3 当前的限制（按对端地址区分连接、不支持迁移、固定 1200 字节数据报、关闭时没有
closing/draining 期等）、有意未实现的功能及原因（0-RTT、QPACK 动态表、server push 等），
以及待优化项（主动 key update 与安全加固、pacing、PMTU 探测、拥塞控制、批量收发等）见
[`docs/http3.zh-CN.md`](../docs/http3.zh-CN.md)。

### 异步 HTTP/3 client

`http3.Client` 与 `http.Client` 的用法相同：

```go
client := http3.NewClient(engine, http3.DefaultClientConfig()) // engine 可以是任意运行中的 Engine
req, _ := http.NewRequest("GET", "https://example.com/", nil)
client.Do(req, func(resp *http.Response, err error) { /* 恰好回调一次 */ })
resp, err := client.Go(req).Wait()
```

- 只支持 `https://`（端口默认 443），其他 scheme 返回 `ErrUnsupportedScheme`。
  `ClientConfig.TLSConfig` 未设置 ServerName 时取 URL 的 host，ALPN 固定为 `h3`。
- 每个 host:port 一个 QUIC 连接，所有请求各占一个 stream 并发发送，数量受服务端
  MAX_STREAMS 限制，超出的请求排队，等服务端放开 stream 后自动发送。
- 收到 GOAWAY 时，服务端未处理的请求和排队中的请求自动在新连接上发送；被服务端以
  H3_REQUEST_REJECTED 重置的请求同样重发一次。
- `Timeout`、请求的 context 取消只重置对应 stream（H3_REQUEST_CANCELLED），不影响
  同连接上的其他请求；`HandshakeTimeout`、`MaxIdleTimeout`、`IdleConnTimeout` 分别
  限制握手、QUIC 空闲超时和连接复用的空闲时间；`MaxResponseHeaderBytes`、
  `MaxResponseBodyBytes` 限制单个响应大小。支持 trailer，1xx 中间响应自动跳过。
- 回调可能在任意 goroutine 上执行；先 `client.Close()` 再关闭 Engine。

## WebSocket 子 package

`websocket` package 实现 RFC 6455 Upgrade 握手、增量帧解析、
分片消息重组、客户端掩码校验、Ping/Pong、Close 握手、子协议协商、消息大小限制和
permessage-deflate 压缩（RFC 7692）：

```go
handler := websocket.NewHandler(websocket.HandlerFuncs{
    Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
        _ = c.WriteMessage(opcode, data)
    },
})
server, err := fib.Bind(config, handler)
```

收到的 Ping 默认自动回复同样 payload 的 Pong，收到的 Pong 默认丢弃。
`HandlerFuncs` 的 `Ping`、`Pong` 字段可以自定义这两种行为；自己实现 Handler 时，
额外实现 `PingHandler`（`OnPing`）或 `PongHandler`（`OnPong`）接口即可：

```go
handler := websocket.HandlerFuncs{
    Message: onMessage,
    // 设置 Ping 后不再自动回复，是否回 Pong 由它自己决定
    Ping: func(c *websocket.Connection, payload []byte) {
        _ = c.Pong(payload)
    },
    // 例如用于心跳检测、计算 RTT
    Pong: func(c *websocket.Connection, payload []byte) {
        lastPong.Store(time.Now().UnixNano())
    },
}
```

- 与 gorilla/websocket 的约定相同：自定义 Ping handler 会替换默认回复，需要回复时
  自己调用 `Connection.Pong`。
- payload 只在回调期间有效，需要保留时先复制；回调与 `OnMessage` 在同一个 worker
  上串行执行。server 和 client（`Dialer`）两端都支持。
- `Connection.Ping` 主动发送 Ping，payload 最多 125 字节。

运行 WebSocket echo 示例（`-compress` 开启压缩）：

```sh
cd go
go run ./examples/websocket/nontls/server   # 另开终端：go run ./examples/websocket/nontls/client
```

### permessage-deflate 压缩

`Config.EnableCompression`（server）和 `DialerConfig.EnableCompression`（client）
开启 permessage-deflate，默认关闭。协商成功后 Text/Binary 消息压缩发送，控制帧不压缩；
收到的压缩消息解压后再交给 `OnMessage`，未协商时收到 RSV1 帧按协议错误以 1002 关闭。

- 发送端总是不保留上下文（server 响应里始终带 `server_no_context_takeover`，client
  offer 里带 `client_no_context_takeover`），每条消息从 `sync.Pool` 借一个
  `flate.BestSpeed` 压缩器，连接本身不持有压缩器。
- 对端保留上下文时，连接保存最近 32 KiB 解压结果作为下一条消息的字典；对端声明
  no_context_takeover 时不保存。
- 对端限制窗口（`server_max_window_bits` / `client_max_window_bits` 为 8–15）时，
  消息按窗口大小分段、各段独立压缩后拼成一个 DEFLATE 流，保证回溯距离不超过窗口。
- `MaxMessageBytes` 限制的是解压后的大小，超过时以 1009 关闭；解压失败或解压后的
  Text 不是合法 UTF-8 时以 1007 关闭。

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

- 支持 `ws://` 和 `wss://`；wss 使用 `DialerConfig.TLSConfig`，规则同 HTTP client。
  其他 scheme 返回 `ErrUnsupportedScheme`。
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

## Examples

每种协议一个目录，每个目录下分 TLS 与非 TLS 两个子目录（UDP 只有非 TLS：本库
不内置 DTLS；HTTP/3 只有 TLS：QUIC 总是加密的），各有一个 echo server 和一个 echo client：

```text
examples/
├── tcp/        nontls/{server,client}  127.0.0.1:9000    tls/{server,client}  127.0.0.1:9443
├── udp/        nontls/{server,client}  127.0.0.1:9001
├── http/       nontls/{server,client}  127.0.0.1:8080    tls/{server,client}  127.0.0.1:8443 (HTTPS)
├── http3/                                                tls/{server,client}  127.0.0.1:8445 (UDP)
└── websocket/  nontls/{server,client}  127.0.0.1:8081    tls/{server,client}  127.0.0.1:8444 (wss)
```

先启动 server，再在另一个终端运行对应的 client，例如：

```sh
cd go
go run ./examples/tcp/tls/server
go run ./examples/tcp/tls/client -n 10
```

- client 默认发送 5 条消息（`-n` 修改），逐条打印回显后退出；server 按 Ctrl-C 退出。
  地址用 `-addr`（TCP、UDP）或 `-url`（HTTP、WebSocket）修改。
- TLS server 未指定 `-cert`/`-key` 时，会为 localhost 和 127.0.0.1 签发一张自签名
  证书，并把证书（不含私钥）写到临时目录的 `fib-example-cert.pem`；TLS client 默认
  信任这个文件（`-ca` 修改），因此会像正式部署一样校验服务端证书。`-insecure` 跳过校验。
- 不同协议的 TLS 写法：TCP、HTTP、WebSocket server 用 `fibtls.NewServer` 包装原本的
  handler，client 分别用 `fibtls.Dial`、`ClientConfig.TLSConfig`、
  `DialerConfig.TLSConfig`。HTTP/3 的 TLS 由 QUIC 自己完成，server 把 TLS 配置直接
  交给 `http3.NewHandler`，client 用 `http3.ClientConfig.TLSConfig`。
- HTTP/3 server 在同一端口号上同时监听 UDP（HTTP/3）和 TCP（HTTPS，HTTP/2 与
  HTTP/1.1），TCP 上的响应带 `Alt-Svc`，浏览器据此切换到 HTTP/3。
- WebSocket server 的 `-compress` 开启 permessage-deflate，CI 用它跑 Autobahn 测试。
- HTTP server 的 `-dir` 用 `net/http` 的 `FileServer` 在 `/files/` 下提供该目录的文件
  （支持 Range、条件请求，文件走 sendfile），例如 `go run ./examples/http/nontls/server -dir .`
  后 `curl -O http://127.0.0.1:8080/files/go.mod`。

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
