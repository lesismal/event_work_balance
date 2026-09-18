//go:build linux || darwin

package fib

import (
	"bytes"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Descriptors are recycled by the kernel, and the connection table is indexed
// by descriptor. A new connection landing on a closed one's descriptor must be
// reached by its own events, and must not inherit anything from its predecessor.
func TestRecycledDescriptorGetsFreshConnection(t *testing.T) {
	var mu sync.Mutex
	tokensByFD := map[int][]uint64{}
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			tokensByFD[c.FD()] = append(tokensByFD[c.FD()], c.token)
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})

	// Serially open, use and close connections. Closing each before opening the
	// next makes the kernel hand the same descriptor back repeatedly.
	payload := []byte("recycled")
	reply := make([]byte, len(payload))
	for i := 0; i < 24; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(reply, payload) {
			t.Fatalf("round %d: echo mismatch", i)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		// Give the loop a moment to process the close before the next dial, so
		// the descriptor is actually free to be reused.
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	reused := false
	for fd, tokens := range tokensByFD {
		if len(tokens) > 1 {
			reused = true
		}
		seen := map[uint64]bool{}
		for _, token := range tokens {
			if seen[token] {
				t.Fatalf("descriptor %d handed out token %d twice", fd, token)
			}
			seen[token] = true
			if int(uint32(token)) != fd {
				t.Fatalf("token %d does not encode descriptor %d", token, fd)
			}
		}
	}
	if !reused {
		t.Skip("kernel never recycled a descriptor; nothing was exercised")
	}
}

// A stale token must not resolve, even to a live connection on the same
// descriptor. This is what the generation half of the token buys.
//
// The lookups run inside OnOpen because the connection table belongs to the
// event loop; checking it from the test goroutine would be the race it is
// meant to rule out.
func TestConnectionForRejectsStaleToken(t *testing.T) {
	type failure struct{ msg string }
	checked := make(chan failure, 1)
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) {
		// Reach the server through the connection rather than through a
		// variable the test goroutine is still assigning.
		server := c.engine
		report := func(msg string) {
			select {
			case checked <- failure{msg}:
			default:
			}
		}
		switch {
		case server.connectionFor(c.token) != c:
			report("connectionFor(live token) did not return the live connection")
		// Same descriptor, different generation.
		case server.connectionFor(c.token^(1<<32)) != nil:
			report("connectionFor(stale token) resolved to a connection")
		// A descriptor past the end of the table must not panic.
		case server.connectionFor(uint64(uint32(len(server.connections)+100))) != nil:
			report("connectionFor(out-of-range descriptor) resolved to a connection")
		default:
			report("")
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case got := <-checked:
		if got.msg != "" {
			t.Fatal(got.msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the connection")
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

// newOfflineConnection builds a connection that is not attached to a socket,
// for exercising queue bookkeeping directly.
func newOfflineConnection(e *Engine) *Connection {
	c := &Connection{engine: e}
	c.token = 1
	c.fd.Store(-1)
	return c
}
