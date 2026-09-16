//go:build linux

package epoll

import (
	"bytes"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestConcurrentBackpressuredEcho(t *testing.T) {
	for _, useWritev := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "writev"}[useWritev], func(t *testing.T) {
			config := DefaultConfig()
			config.BindAddress = "127.0.0.1"
			config.Port = 0
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
	config.BindAddress = "127.0.0.1"
	config.Port = 0
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
	config.BindAddress = "127.0.0.1"
	config.Port = 0
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
			config.BindAddress = "127.0.0.1"
			config.Port = 0
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
