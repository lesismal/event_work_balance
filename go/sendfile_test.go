//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeTestFile writes size pseudo-random bytes to a new file and returns its
// path with its contents.
func writeTestFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func startServer(t *testing.T, network, addr string, handler Handler) string {
	t.Helper()
	config := DefaultConfig()
	config.Network = network
	config.Addr = addr
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.ListenAddrs()
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
	return addrs[0].String()
}

// sendFileHandler answers any byte with head, the file's range and tail, from
// the handler itself (queued behind the corked round) or from a goroutine of
// its own (straight to the socket). The caller's file is closed as soon as
// SendFile returns, which the connection's own descriptor has to survive.
func sendFileHandler(t *testing.T, path string, offset, count int64, fromGoroutine bool) Handler {
	reply := func(c *Connection) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			c.Close()
			return
		}
		_ = c.Send([]byte("head"))
		if err := c.SendFile(f, offset, count); err != nil {
			t.Error(err)
		}
		_ = f.Close()
		_ = c.Send([]byte("tail"))
	}
	return HandlerFuncs{Data: func(c *Connection, _ []byte) {
		if fromGoroutine {
			go reply(c)
		} else {
			reply(c)
		}
	}}
}

func TestSendFile(t *testing.T) {
	const size = 8 << 20
	path, data := writeTestFile(t, size)
	cases := []struct {
		name          string
		offset, count int64
	}{
		{"whole", 0, size},
		{"middle", 12345, size / 2},
		{"small", 7, 100},
	}
	networks := []struct{ network, addr string }{{"tcp", "127.0.0.1:0"}, {"unix", ""}}
	for _, nw := range networks {
		for _, fromGoroutine := range []bool{false, true} {
			for _, tc := range cases {
				name := nw.network + "/" + map[bool]string{false: "handler", true: "goroutine"}[fromGoroutine] + "/" + tc.name
				t.Run(name, func(t *testing.T) {
					addr := nw.addr
					if nw.network == "unix" {
						addr = unixSocketPath(t)
					}
					addr = startServer(t, nw.network, addr, sendFileHandler(t, path, tc.offset, tc.count, fromGoroutine))
					conn, err := net.DialTimeout(nw.network, addr, 5*time.Second)
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
					if _, err = conn.Write([]byte{1}); err != nil {
						t.Fatal(err)
					}
					// Let the socket fill up before reading, so that the file
					// has to wait for the peer part way.
					time.Sleep(50 * time.Millisecond)
					want := append(append([]byte("head"), data[tc.offset:tc.offset+tc.count]...), "tail"...)
					got := make([]byte, len(want))
					if _, err = io.ReadFull(conn, got); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, want) {
						t.Fatal("received bytes differ from the file")
					}
				})
			}
		}
	}
}

// A file shorter than the range cannot deliver what was promised, so the
// connection is closed after what the file did hold.
func TestSendFileShortFileClosesConnection(t *testing.T) {
	path, data := writeTestFile(t, 1000)
	addr := startServer(t, "tcp", "127.0.0.1:0", sendFileHandler(t, path, 0, 5000, false))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err = conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil && !isConnReset(err) {
		t.Fatal(err)
	}
	want := append([]byte("head"), data...)
	if len(got) > len(want) || !bytes.Equal(got, want[:len(got)]) {
		t.Fatalf("received %d bytes, not a prefix of the file", len(got))
	}
	if bytes.HasSuffix(got, []byte("tail")) {
		t.Fatal("bytes after the short file were sent")
	}
}

func isConnReset(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func TestSendFileRejectsBadRangeAndDatagrams(t *testing.T) {
	path, _ := writeTestFile(t, 10)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c := &Connection{}
	if err := c.SendFile(f, -1, 1); err == nil {
		t.Fatal("negative offset accepted")
	}
	c.udp = &udpState{}
	if err := c.SendFile(f, 0, 1); err != ErrSendFileDatagram {
		t.Fatalf("UDP SendFile = %v", err)
	}
}
