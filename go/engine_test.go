//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/go/taskpool"
)

func TestConcurrentBackpressuredEcho(t *testing.T) {
	for _, useWritev := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "writev"}[useWritev], func(t *testing.T) {
			config := DefaultConfig()
			config.Addr = "127.0.0.1:0"
			config.UseWritev = useWritev
			server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
				if err := c.Send(b); err != nil {
					c.Close()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- server.Run() }()
			defer func() {
				server.Stop()
				if err := <-runDone; err != nil {
					t.Error(err)
				}
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			}()
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(value byte) {
					defer wg.Done()
					payload := bytes.Repeat([]byte{value}, 1024*1024)
					conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
					if _, err = conn.Write(payload); err != nil {
						t.Error(err)
						return
					}
					received := make([]byte, len(payload))
					if _, err = io.ReadFull(conn, received); err != nil {
						t.Error(err)
						return
					}
					if !bytes.Equal(received, payload) {
						t.Error("echo mismatch")
					}
				}(byte(i))
			}
			wg.Wait()
		})
	}
}

func TestOnCloseReportsPeerEOF(t *testing.T) {
	closed := make(chan error, 1)
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != io.EOF {
			t.Fatalf("OnClose error = %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnClose")
	}
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
}

// TestReadsDeferredWhileOutputQueued covers the flush-before-read ordering in
// process: while Send output is still queued the worker must not read, and the
// deferred readiness must survive until the queue drains so the socket does
// not turn into a zombie with unread bytes and no further edge.
func TestReadsDeferredWhileOutputQueued(t *testing.T) {
	for _, halfClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "halfclose"}[halfClose], func(t *testing.T) {
			payload := bytes.Repeat([]byte{0xAB}, 32*1024*1024)
			second := make(chan []byte, 1)
			closed := make(chan error, 1)
			config := DefaultConfig()
			config.Addr = "127.0.0.1:0"
			// Disable the watermark so epoll keeps EPOLLIN registered and only
			// the process ordering stands between queued output and a read.
			config.WriteBufferHighWatermark = -1
			var first sync.Once
			var sender atomic.Pointer[Connection]
			server, err := Bind(config, HandlerFuncs{
				Data: func(c *Connection, data []byte) {
					started := false
					first.Do(func() {
						started = true
						sender.Store(c)
						// Send in chunks: Windows accepts one send of any size
						// while its send backlog is below SO_SNDBUF, so a single
						// call could leave nothing queued. Later chunks queue
						// once the backlog is full, on every platform.
						for offset := 0; offset < len(payload); offset += 1 << 20 {
							if err := c.SendOwned(payload[offset : offset+1<<20]); err != nil {
								t.Error(err)
								return
							}
						}
					})
					if !started {
						second <- append([]byte(nil), data...)
					}
				},
				Close: func(_ *Connection, err error) { closed <- err },
			})
			if err != nil {
				t.Fatal(err)
			}
			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- server.Run() }()
			defer func() {
				server.Stop()
				if err := <-runDone; err != nil {
					t.Error(err)
				}
				if err := server.Close(); err != nil {
					t.Error(err)
				}
			}()
			conn, err := net.DialTCP("tcp4", nil, addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			if _, err := conn.Write([]byte("start")); err != nil {
				t.Fatal(err)
			}
			// Let the server block on the peer's full receive window before
			// the second message arrives.
			time.Sleep(300 * time.Millisecond)
			if c := sender.Load(); c == nil || !c.hasQueuedOutput() {
				t.Skip("the kernel accepted the whole payload; no output is queued to defer reads behind")
			}
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			if halfClose {
				if err := conn.CloseWrite(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case data := <-second:
				t.Fatalf("read %q while output was still queued", data)
			case <-time.After(300 * time.Millisecond):
			}
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, received); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received, payload) {
				t.Fatal("payload mismatch")
			}
			select {
			case data := <-second:
				if !bytes.Equal(data, []byte("ping")) {
					t.Fatalf("second message = %q", data)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("deferred read never resumed after the queue drained")
			}
			if halfClose {
				select {
				case err := <-closed:
					if err != io.EOF {
						t.Fatalf("OnClose error = %v, want io.EOF", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for OnClose after half-close")
				}
			}
		})
	}
}

// TestConcurrentAcceptBurst covers the accept path when many connections arrive
// at once: every one must be accepted, tracked, and able to carry data.
func TestConcurrentAcceptBurst(t *testing.T) {
	const burst = 256
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp4", addr.String(), 20*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
			payload := []byte{value, value, value, value}
			if _, err = conn.Write(payload); err != nil {
				t.Error(err)
				return
			}
			echoed := make([]byte, len(payload))
			if _, err = io.ReadFull(conn, echoed); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(echoed, payload) {
				t.Error("echo mismatch")
			}
		}(byte(i))
	}
	wg.Wait()
}

// InlineHandlers moves handler execution onto the event loop. The connections
// still have to be served correctly and concurrently: the loop now interleaves
// their rounds itself instead of handing them to workers.
func TestInlineHandlersServeConcurrentConnections(t *testing.T) {
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	config.InlineHandlers = true
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Errorf("Run: %v", err)
		}
		if err := server.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	const connections, rounds = 16, 32
	payload := bytes.Repeat([]byte("inline"), 64)
	var wg sync.WaitGroup
	errs := make(chan error, connections)
	for i := 0; i < connections; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			reply := make([]byte, len(payload))
			for round := 0; round < rounds; round++ {
				if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
					errs <- err
					return
				}
				if _, err := conn.Write(payload); err != nil {
					errs <- err
					return
				}
				if _, err := io.ReadFull(conn, reply); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(reply, payload) {
					errs <- io.ErrUnexpectedEOF
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestMultipleListenersShareOneEngine covers a server carrying several
// listeners: every port must accept, and the connections from all of them must
// land in the one descriptor table, event loop and worker pool rather than
// needing a server each.
func TestMultipleListenersShareOneEngine(t *testing.T) {
	const listeners = 4
	config := DefaultConfig()
	config.Addr = "127.0.0.1:9999" // ignored once Addrs is set
	config.Addrs = make([]string, listeners)
	for i := range config.Addrs {
		config.Addrs[i] = "127.0.0.1:0"
	}
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.LocalAddrs()
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != listeners {
		t.Fatalf("LocalAddrs returned %d addresses, want %d", len(addrs), listeners)
	}
	seen := make(map[int]bool, listeners)
	for _, addr := range addrs {
		if addr.Port == 0 || addr.Port == 9999 || seen[addr.Port] {
			t.Fatalf("listener ports are not distinct ephemeral ports: %v", addrs)
		}
		seen[addr.Port] = true
	}
	first, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	if first.Port != addrs[0].Port {
		t.Fatalf("LocalAddr port = %d, want the first listener %d", first.Port, addrs[0].Port)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()

	var wg sync.WaitGroup
	for i, addr := range addrs {
		for round := 0; round < 4; round++ {
			wg.Add(1)
			go func(addr string, value byte) {
				defer wg.Done()
				conn, err := net.DialTimeout("tcp4", addr, 10*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				payload := bytes.Repeat([]byte{value}, 64)
				if _, err = conn.Write(payload); err != nil {
					t.Error(err)
					return
				}
				echoed := make([]byte, len(payload))
				if _, err = io.ReadFull(conn, echoed); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(echoed, payload) {
					t.Errorf("echo mismatch on listener %s", addr)
				}
			}(addr.String(), byte(i))
		}
	}
	wg.Wait()
}

// Network and Addr are read the way net.Listen reads them, so the same strings
// that name a listener there name one here.
func TestListenAddressFormsMatchNetListen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		network string
		addr    string
		wantIP  func(net.IP) bool
	}{
		{"tcp4 literal", "tcp4", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
		{"tcp literal", "tcp", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
		{"tcp6 literal", "tcp6", "[::1]:0", func(ip net.IP) bool { return ip.Equal(net.IPv6loopback) }},
		{"tcp wildcard", "tcp", ":0", func(ip net.IP) bool { return ip.IsUnspecified() }},
		{"empty network defaults to tcp", "", "127.0.0.1:0", func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Whatever this library does with the arguments, net.Listen has to
			// accept them too, or they are not the standard forms.
			reference, err := net.Listen(map[bool]string{true: "tcp", false: tc.network}[tc.network == ""], tc.addr)
			if err != nil {
				t.Skipf("net.Listen(%q, %q): %v", tc.network, tc.addr, err)
			}
			reference.Close()

			config := DefaultConfig()
			config.Network = tc.network
			config.Addr = tc.addr
			server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
				if err := c.Send(b); err != nil {
					c.Close()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- server.Run() }()
			defer func() {
				server.Stop()
				if err := <-runDone; err != nil {
					t.Errorf("Run: %v", err)
				}
				if err := server.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}()

			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			if addr.Port == 0 {
				t.Fatal("LocalAddr reports port 0, want the port the kernel chose")
			}
			if !tc.wantIP(addr.IP) {
				t.Fatalf("LocalAddr IP = %v, not the address asked for", addr.IP)
			}

			// A wildcard listener is reachable over loopback; a literal one is
			// reachable at itself. Dialing the reported address covers both.
			dialAddr := addr.String()
			if addr.IP.IsUnspecified() {
				dialAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(addr.Port))
			}
			conn, err := net.DialTimeout("tcp", dialAddr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			payload := []byte("listen")
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, reply); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reply, payload) {
				t.Fatalf("echo returned %q, want %q", reply, payload)
			}
		})
	}
}

func TestListenRejectsUnknownNetwork(t *testing.T) {
	config := DefaultConfig()
	config.Network = "unixgram"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{})
	if err == nil {
		server.Close()
		t.Fatal("Bind accepted network \"unixgram\", want an error")
	}
	var unknown net.UnknownNetworkError
	if !errors.As(err, &unknown) {
		t.Fatalf("Bind error = %v, want net.UnknownNetworkError", err)
	}
}

// countingPool runs tasks on a real pool and counts what it was handed, so a
// test can tell the engine used it.
type countingPool struct {
	*taskpool.TaskPool
	tasks atomic.Int64
}

func (p *countingPool) GoTask(task taskpool.Task) bool {
	p.tasks.Add(1)
	return p.TaskPool.GoTask(task)
}

func (p *countingPool) GoTasks(tasks []taskpool.Task) int {
	p.tasks.Add(int64(len(tasks)))
	return p.TaskPool.GoTasks(tasks)
}

// A pool the caller supplies has to carry the connections, make the built-in
// pool's settings irrelevant, and survive the engine: the caller owns it and
// may be sharing it.
func TestCustomTaskPoolRunsConnectionsAndOutlivesEngine(t *testing.T) {
	pool := &countingPool{TaskPool: taskpool.NewWithMode(taskpool.ModeCond, 4, 64)}
	defer pool.Stop()
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	// Invalid for the built-in pool, and ignored once a pool is supplied.
	config.WorkerCount = 0
	config.SetTaskPool(pool)
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	payload := []byte("custom pool")
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reply, payload) {
		t.Fatalf("echo = %q, want %q", reply, payload)
	}
	_ = conn.Close()
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
	if pool.tasks.Load() == 0 {
		t.Fatal("the engine never handed a task to the supplied pool")
	}
	done := make(chan struct{})
	if !pool.Go(func() { close(done) }) {
		t.Fatal("closing the engine stopped a pool it does not own")
	}
	<-done
}
