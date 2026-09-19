//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// startUDPServer runs an engine on a UDP socket and returns it with its
// address.
func startUDPServer(t *testing.T, config Config, handler Handler) (*Engine, *net.UDPAddr) {
	t.Helper()
	config.Network = "udp"
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server, addr
}

// Every datagram reaches OnData whole and alone, and every Send leaves as one
// datagram, up to the largest macOS sends by default (its UDP send buffer is 9216 bytes).
func TestUDPServerEchoKeepsDatagramBoundaries(t *testing.T) {
	_, addr := startUDPServer(t, DefaultConfig(), echoHandler())
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, maxDatagramSize)
	for _, size := range []int{1, 100, 1400, 9000} {
		payload := bytes.Repeat([]byte{byte(size)}, size)
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:n], payload) {
			t.Fatalf("%d-byte datagram came back as %d bytes", size, n)
		}
	}
}

// Each peer address is a connection of its own, which knows the peer's
// address and outlives any single datagram.
func TestUDPPeersAreSeparateConnections(t *testing.T) {
	var mu sync.Mutex
	opened := map[*Connection]string{}
	_, addr := startUDPServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			opened[c] = c.RemoteAddr().String()
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if !c.IsUDP() {
				t.Error("UDP peer does not report IsUDP")
			}
			_ = c.Send(append([]byte(c.RemoteAddr().String()+" "), b...))
		},
	})
	const peers = 8
	for i := 0; i < peers; i++ {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		for round := 0; round < 3; round++ {
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 128)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatal(err)
			}
			if want := conn.LocalAddr().String() + " ping"; string(buf[:n]) != want {
				t.Fatalf("reply %q, want %q", buf[:n], want)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != peers {
		t.Fatalf("%d connections opened for %d peers", len(opened), peers)
	}
}

// A peer that goes quiet is closed after the idle timeout, and one that sends
// again afterwards gets a fresh connection.
func TestUDPIdlePeerTimesOut(t *testing.T) {
	config := DefaultConfig()
	config.UDPIdleTimeout = 100 * time.Millisecond
	opened := make(chan *Connection, 4)
	closed := make(chan error, 4)
	_, addr := startUDPServer(t, config, HandlerFuncs{
		Open:  func(c *Connection) { opened <- c },
		Close: func(_ *Connection, err error) { closed <- err },
	})
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for round := 0; round < 2; round++ {
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		select {
		case <-opened:
		case <-time.After(5 * time.Second):
			t.Fatal("no connection opened")
		}
		select {
		case err := <-closed:
			if !errors.Is(err, ErrUDPIdleTimeout) {
				t.Fatalf("closed with %v, want ErrUDPIdleTimeout", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("idle peer was not closed")
		}
	}
}

// A dialed UDP connection talks to its one peer, and SendParts joins its
// parts into a single datagram.
func TestDialUDP(t *testing.T) {
	_, addr := startUDPServer(t, DefaultConfig(), echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	received := make(chan []byte, 8)
	handler := HandlerFuncs{Data: func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) }}
	dialed := make(chan *Connection, 1)
	err := client.DialWithHandler("udp", addr.String(), time.Second, handler, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		dialed <- c
	})
	if err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if !c.IsUDP() || c.RemoteAddr().String() != addr.String() {
		t.Fatalf("dialed connection: udp %v, remote %v", c.IsUDP(), c.RemoteAddr())
	}
	for _, msg := range []string{"one", "two", "three"} {
		if err := c.SendParts([]byte(msg), []byte("!")); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-received:
			if string(got) != msg+"!" {
				t.Fatalf("echo %q, want %q", got, msg+"!")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no echo")
		}
	}
	c.Close()
}
