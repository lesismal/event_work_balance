//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// unixSocketPath returns a path for a Unix socket in a directory of its own.
// t.TempDir can run past the 104 bytes macOS allows a socket path, so the
// directory is made directly under the system temporary directory.
func unixSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "fib")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func startUnixServer(t *testing.T, path string, handler Handler) *Engine {
	t.Helper()
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	server, err := Bind(config, handler)
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
		_ = server.Close()
	})
	return server
}

// A Unix socket serves like TCP: large echoes run through the same queueing
// and backpressure, the peer's address is a *net.UnixAddr, and the socket file
// is gone once the engine closes.
func TestUnixSocketEcho(t *testing.T) {
	path := unixSocketPath(t)
	var remote sync.Map
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	server, err := Bind(config, HandlerFuncs{
		Open: func(c *Connection) { remote.Store(c, c.RemoteAddr()) },
		Data: func(c *Connection, b []byte) {
			if c.Send(b) != nil {
				c.Close()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()

	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 || addrs[0].Network() != "unix" || addrs[0].String() != path {
		t.Fatalf("ListenAddrs = %v, want unix %s", addrs, path)
	}
	if _, err := server.LocalAddr(); err == nil {
		t.Error("LocalAddr reported a TCP address for a Unix listener")
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			conn, err := net.DialTimeout("unix", path, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
			payload := bytes.Repeat([]byte{value}, 1<<20)
			go func() { _, _ = conn.Write(payload) }()
			received := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, received); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(received, payload) {
				t.Error("echo mismatch")
			}
		}(byte(i))
	}
	wg.Wait()
	remote.Range(func(_, addr any) bool {
		if _, ok := addr.(*net.UnixAddr); !ok {
			t.Errorf("RemoteAddr = %T, want *net.UnixAddr", addr)
		}
		return true
	})

	server.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket file left behind: %v", err)
	}
}

// Binding a path that already exists fails, as it does for net.Listen, rather
// than taking the path from whatever owns it.
func TestUnixSocketPathInUse(t *testing.T) {
	path := unixSocketPath(t)
	startUnixServer(t, path, nil)
	config := DefaultConfig()
	config.Network = "unix"
	config.Addr = path
	if second, err := Bind(config, nil); err == nil {
		second.Close()
		t.Fatal("a second engine bound a path already in use")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the failed Bind removed the first engine's socket: %v", err)
	}
}

// An engine dials a Unix socket like a TCP address, and a path nothing
// listens on fails the dial.
func TestDialUnix(t *testing.T) {
	path := unixSocketPath(t)
	startUnixServer(t, path, echoHandler())
	client, _ := startEchoServer(t, DefaultConfig(), nil)

	received := make(chan []byte, 16)
	handler := HandlerFuncs{Data: func(_ *Connection, b []byte) { received <- append([]byte(nil), b...) }}
	dialed := make(chan dialResult, 1)
	err := client.DialWithHandler("unix", path, 5*time.Second, handler, func(c *Connection, err error) {
		dialed <- dialResult{c, err}
	})
	if err != nil {
		t.Fatal(err)
	}
	r := <-dialed
	if r.err != nil {
		t.Fatal(r.err)
	}
	for i := 0; i < 3; i++ {
		msg := "unix " + strconv.Itoa(i)
		if err := r.c.Send([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		var got []byte
		for len(got) < len(msg) {
			select {
			case b := <-received:
				got = append(got, b...)
			case <-time.After(5 * time.Second):
				t.Fatal("no echo")
			}
		}
		if string(got) != msg {
			t.Fatalf("echo %q, want %q", got, msg)
		}
	}
	r.c.Close()

	failed := make(chan error, 1)
	missing := filepath.Join(filepath.Dir(path), "missing.sock")
	err = client.Dial("unix", missing, time.Second, func(c *Connection, err error) { failed <- err })
	if err == nil {
		err = <-failed
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Addr == nil || opErr.Addr.String() != missing {
		t.Fatalf("dial to a missing socket: %v, want a *net.OpError naming it", err)
	}
}

// On Linux a leading '@' names an abstract socket, which has no file.
func TestUnixAbstractSocket(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract Unix sockets are Linux-only")
	}
	name := "@fib-test-" + strconv.Itoa(os.Getpid())
	startUnixServer(t, name, echoHandler())
	conn, err := net.DialTimeout("unix", name, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("abstract")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "abstract" {
		t.Fatalf("echo %q, %v", buf, err)
	}
}
