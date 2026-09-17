//go:build linux

package epoll

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
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

func TestPriorityDataUsesDedicatedCallback(t *testing.T) {
	regular := make(chan []byte, 1)
	priority := make(chan []byte, 1)
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{
		Data:         func(_ *Connection, data []byte) { regular <- append([]byte(nil), data...) },
		PriorityData: func(_ *Connection, data []byte) { priority <- append([]byte(nil), data...) },
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
	conn, err := net.DialTCP("tcp4", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var sendErr error
	if err := raw.Control(func(fd uintptr) {
		_, sendErr = syscall.SendmsgN(int(fd), []byte("!"), nil, nil, syscall.MSG_OOB)
	}); err != nil {
		t.Fatal(err)
	}
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	if _, err := conn.Write([]byte("normal")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-priority:
		if !bytes.Equal(data, []byte("!")) {
			t.Fatalf("priority data = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for priority data")
	}
	select {
	case data := <-regular:
		if !bytes.Equal(data, []byte("normal")) {
			t.Fatalf("regular data = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for regular data")
	}
	_ = conn.Close()
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
			server, err := Bind(config, HandlerFuncs{
				Data: func(c *Connection, data []byte) {
					started := false
					first.Do(func() {
						started = true
						if err := c.SendOwned(payload); err != nil {
							t.Error(err)
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

// TestDefaultBacklogMatchesKernelLimit keeps the accept queue as deep as the
// one net.Listen asks for, which is what every framework built on it gets. An
// overflowing accept queue does not refuse connections: the kernel drops the
// client's ACK, so a connection burst shows up only as clients sitting on
// SYN-ACK retransmission timers. Measured against a 3000-connection burst, the
// historical SOMAXCONN of 128 cost 1399 upgrades per second and a median of
// 1.06s, against 63064 per second and 34ms at the kernel's own limit.
func TestDefaultBacklogMatchesKernelLimit(t *testing.T) {
	want := syscall.SOMAXCONN
	if data, err := os.ReadFile("/proc/sys/net/core/somaxconn"); err == nil {
		if limit, parseErr := strconv.Atoi(strings.TrimSpace(string(data))); parseErr == nil && limit > 0 {
			want = limit
			if want > 1<<16-1 {
				want = 1<<16 - 1
			}
		}
	}
	if got := DefaultConfig().Backlog; got != want {
		t.Fatalf("default backlog = %d, want the kernel limit %d", got, want)
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

// TestMultipleListenersShareOneServer covers a server carrying several
// listeners: every port must accept, and the connections from all of them must
// land in the one descriptor table, event loop and worker pool rather than
// needing a server each.
func TestMultipleListenersShareOneServer(t *testing.T) {
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
	config.Network = "udp"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{})
	if err == nil {
		server.Close()
		t.Fatal("Bind accepted network \"udp\", want an error")
	}
	var unknown net.UnknownNetworkError
	if !errors.As(err, &unknown) {
		t.Fatalf("Bind error = %v, want net.UnknownNetworkError", err)
	}
}
