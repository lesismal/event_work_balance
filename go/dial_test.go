//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dialResult is what one Dial's done callback reported.
type dialResult struct {
	c   *Connection
	err error
}

func echoHandler() Handler {
	return HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}}
}

// A dialed connection joins the engine the way an accepted one does: OnOpen
// runs before the dialer hears of it, and its replies arrive through the same
// handler and workers.
func TestDialedConnectionsEcho(t *testing.T) {
	_, serverAddr := startEchoServer(t, DefaultConfig(), echoHandler())

	const conns = 64
	payload := []byte("dialed through the event loop")
	var mu sync.Mutex
	opened := map[*Connection]bool{}
	received := map[*Connection][]byte{}
	echoed := make(chan *Connection, conns)
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			opened[c] = true
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			mu.Lock()
			received[c] = append(received[c], b...)
			done := len(received[c]) == len(payload)
			mu.Unlock()
			if done {
				echoed <- c
			}
		},
	})

	results := make(chan dialResult, conns)
	for i := 0; i < conns; i++ {
		err := client.Dial("tcp4", serverAddr, 5*time.Second, func(c *Connection, err error) {
			if err == nil {
				mu.Lock()
				if !opened[c] {
					t.Error("done ran before OnOpen")
				}
				mu.Unlock()
				err = c.Send(payload)
			}
			results <- dialResult{c, err}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < conns; i++ {
		if r := <-results; r.err != nil {
			t.Fatalf("dial %d: %v", i, r.err)
		}
	}
	timeout := time.After(10 * time.Second)
	for i := 0; i < conns; i++ {
		select {
		case c := <-echoed:
			mu.Lock()
			got := received[c]
			mu.Unlock()
			if !bytes.Equal(got, payload) {
				t.Fatalf("echo mismatch: %q", got)
			}
		case <-timeout:
			t.Fatalf("only %d of %d connections were echoed", i, conns)
		}
	}
}

// A host name goes through the resolver on a goroutine of its own, and must
// still end up on the event loop.
func TestDialResolvesHostName(t *testing.T) {
	_, serverAddr := startEchoServer(t, DefaultConfig(), echoHandler())
	_, port, _ := net.SplitHostPort(serverAddr)
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", net.JoinHostPort("localhost", port), 5*time.Second, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if r.err != nil {
			t.Fatal(r.err)
		}
		r.c.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("dial never reported")
	}
}

// A refused connect reaches the dialer as an error, and the handler never hears
// of the connection.
func TestDialRefusedReportsError(t *testing.T) {
	// A port that was just listened on and closed has nobody behind it.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()

	var handlerCalls int
	var mu sync.Mutex
	count := func(*Connection) { mu.Lock(); handlerCalls++; mu.Unlock() }
	client, _ := startEchoServer(t, DefaultConfig(), HandlerFuncs{
		Open:  count,
		Close: func(c *Connection, _ error) { count(c) },
	})
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", addr, 5*time.Second, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	r := <-results
	if r.err == nil || r.c != nil {
		t.Fatalf("dial to a closed port: connection %v, error %v", r.c, r.err)
	}
	var opErr *net.OpError
	if !errors.As(r.err, &opErr) || opErr.Op != "dial" {
		t.Fatalf("error is not a dial *net.OpError: %#v", r.err)
	}
	mu.Lock()
	defer mu.Unlock()
	if handlerCalls != 0 {
		t.Fatalf("handler called %d times for a refused dial", handlerCalls)
	}
}

// blackholeAddr names an address from TEST-NET-1, which is never routed, so a
// connect to it hangs until something gives up. Where a network refuses it
// outright instead, the tests that need a hanging connect are skipped.
const blackholeAddr = "192.0.2.1:81"

func TestDialTimeout(t *testing.T) {
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	results := make(chan dialResult, 1)
	start := time.Now()
	if err := client.Dial("tcp4", blackholeAddr, 200*time.Millisecond, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	r := <-results
	if !errors.Is(r.err, os.ErrDeadlineExceeded) {
		t.Skipf("connect to %s did not hang: %v", blackholeAddr, r.err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	var netErr net.Error
	if !errors.As(r.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("timeout does not report Timeout(): %v", r.err)
	}
}

// Closing the engine settles a connect still in flight rather than leaving
// its dialer waiting forever.
func TestCloseFailsPendingDial(t *testing.T) {
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	client, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	results := make(chan dialResult, 1)
	if err := client.Dial("tcp4", blackholeAddr, 0, func(c *Connection, err error) {
		results <- dialResult{c, err}
	}); err != nil {
		t.Fatal(err)
	}
	// Give the loop time to start the connect.
	time.Sleep(100 * time.Millisecond)
	select {
	case r := <-results:
		t.Skipf("connect to %s did not hang: %v", blackholeAddr, r.err)
	default:
	}
	client.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if !errors.Is(r.err, net.ErrClosed) {
			t.Fatalf("pending dial reported %v, want net.ErrClosed", r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the engine never settled the pending dial")
	}
}

func TestDialRejectsWhatCannotStart(t *testing.T) {
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	never := func(*Connection, error) { t.Error("done called for a dial that could not start") }
	for _, tc := range []struct{ network, addr string }{
		{"udp", "127.0.0.1:80"},
		{"udp", "localhost:80"},
		{"tcp", "127.0.0.1"},
		{"tcp", "127.0.0.1:99999"},
		{"tcp4", "[::1]:80"},
	} {
		if err := client.Dial(tc.network, tc.addr, 0, never); err == nil {
			t.Errorf("Dial(%q, %q) started", tc.network, tc.addr)
		}
	}

	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	closed, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed.Stop()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Dial("tcp4", "127.0.0.1:"+strconv.Itoa(80), 0, never); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Dial on a closed engine: %v, want net.ErrClosed", err)
	}
}

// A dialed connection with a handler of its own gets every callback there, and
// the engine's handler hears nothing of it. An engine made by NewEngine has no
// listener at all.
func TestDialWithHandlerOnListenerlessEngine(t *testing.T) {
	_, addr := startEchoServer(t, DefaultConfig(), echoHandler())
	var engineCalls atomic.Int64
	client, err := NewEngine(DefaultConfig(), HandlerFuncs{
		Open: func(*Connection) { engineCalls.Add(1) },
		Data: func(*Connection, []byte) { engineCalls.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.LocalAddr(); err == nil {
		t.Fatal("LocalAddr on an engine without listeners reported an address")
	}
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run() }()
	defer func() {
		client.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = client.Close()
	}()

	opened := make(chan struct{})
	echoed := make(chan []byte, 1)
	closed := make(chan error, 1)
	own := HandlerFuncs{
		Open:  func(*Connection) { close(opened) },
		Data:  func(_ *Connection, b []byte) { echoed <- append([]byte(nil), b...) },
		Close: func(_ *Connection, err error) { closed <- err },
	}
	err = client.DialWithHandler("tcp", addr, 5*time.Second, own, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		_ = c.Send([]byte("own handler"))
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-echoed:
		if string(got) != "own handler" {
			t.Fatalf("echo = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no echo reached the dial's own handler")
	}
	<-opened
	if n := engineCalls.Load(); n != 0 {
		t.Fatalf("the engine's handler was called %d times for a connection dialed with its own", n)
	}
}
