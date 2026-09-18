//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
)

func TestServerHandshakeEchoPingAndClose(t *testing.T) {
	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			if err := c.WriteMessage(opcode, payload); err != nil {
				t.Error(err)
			}
		},
	})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, handler)
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
		<-runDone
		_ = server.Close()
	}()

	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /chat HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err = io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha1.Sum([]byte(key + websocketGUID))
	wantAccept := base64.StdEncoding.EncodeToString(sum[:])
	if response.StatusCode != stdhttp.StatusSwitchingProtocols ||
		response.Header.Get("Sec-WebSocket-Accept") != wantAccept {
		t.Fatalf("invalid handshake: status=%d accept=%q", response.StatusCode, response.Header.Get("Sec-WebSocket-Accept"))
	}

	frames := append(clientFrame(Text, true, []byte("hello")), clientFrame(Ping, true, []byte("p"))...)
	frames = append(frames, clientFrame(Close, true, []byte{0x03, 0xe8})...)
	if _, err = conn.Write(frames); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []Opcode{Text, Pong, Close} {
		opcode, _, err := readServerFrame(reader)
		if err != nil {
			t.Fatal(err)
		}
		if opcode != expected {
			t.Fatalf("opcode = %d, want %d", opcode, expected)
		}
	}
}

func BenchmarkMinimalServerHandshake(b *testing.B) {
	handler := NewHandler(HandlerFuncs{Message: func(*Connection, Opcode, []byte) {}})
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkHandshakeRequest)))
	for i := 0; i < b.N; i++ {
		key, _, remainder, complete, err := handler.validateMinimalHandshake(benchmarkHandshakeRequest)
		if err != nil || !complete || len(key) != 24 || len(remainder) != 0 {
			b.Fatalf("minimal handshake: key=%q remainder=%d complete=%v err=%v", key, len(remainder), complete, err)
		}
	}
}

func readServerFrame(reader *bufio.Reader) (Opcode, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, nil, err
	}
	length := uint64(header[1] & 0x7f)
	if length == 126 {
		size := make([]byte, 2)
		if _, err := io.ReadFull(reader, size); err != nil {
			return 0, nil, err
		}
		length = uint64(size[0])<<8 | uint64(size[1])
	} else if length == 127 {
		size := make([]byte, 8)
		if _, err := io.ReadFull(reader, size); err != nil {
			return 0, nil, err
		}
		length = 0
		for _, value := range size {
			length = length<<8 | uint64(value)
		}
	}
	payload := make([]byte, int(length))
	_, err := io.ReadFull(reader, payload)
	return Opcode(header[0] & 0xf), payload, err
}

// dialUpgraded opens a connection and completes the WebSocket handshake.
func dialUpgraded(tb testing.TB, addr string) (net.Conn, *bufio.Reader) {
	tb.Helper()
	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		tb.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /chat HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err = io.WriteString(conn, request); err != nil {
		tb.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		tb.Fatal(err)
	}
	if response.StatusCode != stdhttp.StatusSwitchingProtocols {
		tb.Fatalf("handshake status = %d, want %d", response.StatusCode, stdhttp.StatusSwitchingProtocols)
	}
	return conn, reader
}

// BenchmarkServerEcho1KiB drives the server the way a load generator does:
// several connections each keeping messages in flight, so one read usually
// carries more than one frame and one round answers more than one message. It
// covers parsing and the reply write path together, which per-frame benchmarks
// cannot. Client and server share the process, so treat the result as a
// relative measure rather than an absolute server cost.
func BenchmarkServerEcho1KiB(b *testing.B) {
	const connections = 8

	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			_ = c.WriteMessage(opcode, payload)
		},
	})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, handler)
	if err != nil {
		b.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		b.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()

	frame := clientFrame(Binary, true, make([]byte, 1024))
	perConnection := (b.N + connections - 1) / connections
	conns := make([]net.Conn, connections)
	readers := make([]*bufio.Reader, connections)
	for i := range conns {
		conns[i], readers[i] = dialUpgraded(b, addr.String())
		defer conns[i].Close()
	}

	b.SetBytes(1024)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < connections; i++ {
		wg.Add(2)
		go func(conn net.Conn) {
			defer wg.Done()
			for sent := 0; sent < perConnection; sent++ {
				if _, err := conn.Write(frame); err != nil {
					b.Error(err)
					return
				}
			}
		}(conns[i])
		go func(reader *bufio.Reader) {
			defer wg.Done()
			for received := 0; received < perConnection; received++ {
				if _, _, err := readServerFrame(reader); err != nil {
					b.Error(err)
					return
				}
			}
		}(readers[i])
	}
	wg.Wait()
	b.StopTimer()
}

// dialWebSocket opens a connection and completes the handshake, returning the
// connection and the buffered reader the handshake response was read through.
// Anything the server sends afterwards has to be read through that reader,
// since it may already hold the first bytes of it.
func dialWebSocket(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /chat HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err = io.WriteString(conn, request); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if response.StatusCode != stdhttp.StatusSwitchingProtocols {
		conn.Close()
		t.Fatalf("handshake status = %d, want %d", response.StatusCode, stdhttp.StatusSwitchingProtocols)
	}
	return conn, reader
}

// A peer that only ever sends must not be able to make the server read without
// limit. Its replies cannot leave, so they queue; once that queue crosses the
// write watermark the server has to stop reading this connection and let the
// backlog push back through TCP to the peer itself. Reading on regardless is
// what turns one silent peer into unbounded memory on the server.
//
// The pause is only correct if it is a pause, so the test carries on past it:
// once the peer starts reading, the server has to pick its connection back up
// on its own.
func TestReadsPauseForPeerThatNeverReadsAndResumeWhenItDoes(t *testing.T) {
	const (
		watermark   = 64 << 10
		payloadSize = 8 << 10
		// Far more than the watermark and both kernel socket buffers together,
		// so reaching it means nothing ever stopped the server reading.
		maxWrite = 64 << 20
	)
	var received atomic.Int64
	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			received.Add(int64(len(payload)))
			if err := c.WriteMessage(opcode, payload); err != nil {
				_ = c.Close(CloseInternalError, "write failed")
			}
		},
	})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	config.WriteBufferHighWatermark = watermark
	// Leave the server-wide budget off, so the per-connection watermark is the
	// only thing that can stop these reads.
	config.MaxPendingBytes = 0
	server, err := fib.Bind(config, handler)
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
		<-runDone
		_ = server.Close()
	}()

	conn, reader := dialWebSocket(t, addr.String())
	defer conn.Close()

	// Send, and never read. Every reply the server produces stays in its queue
	// and in the socket buffers between the two.
	frame := clientFrame(Binary, true, bytes.Repeat([]byte{'x'}, payloadSize))
	written := 0
	blocked := false
	for written < maxWrite {
		if err = conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		n, writeErr := conn.Write(frame)
		written += n
		if writeErr == nil {
			continue
		}
		var netErr net.Error
		if errors.As(writeErr, &netErr) && netErr.Timeout() {
			blocked = true
			break
		}
		t.Fatalf("after %d bytes: %v", written, writeErr)
	}
	if !blocked {
		t.Fatalf("wrote %d bytes without ever blocking: the server never stopped reading", written)
	}
	if received.Load() == 0 {
		t.Fatal("the server read nothing at all; the test never exercised the pause")
	}

	// Blocked on write means the pushback reached this end, so the server is
	// not reading this connection any more. It must stay that way while the
	// peer keeps ignoring its replies.
	paused := received.Load()
	time.Sleep(500 * time.Millisecond)
	if grown := received.Load(); grown != paused {
		t.Fatalf("server read %d more bytes while the peer was not reading, want the reads to stay paused",
			grown-paused)
	}

	// Now drain. The replies leave, the queue falls back under the watermark,
	// and the server has to resume reading what the peer already sent.
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		buf := make([]byte, 32<<10)
		for {
			select {
			case <-drainDone:
				return
			default:
			}
			if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				return
			}
			if _, err := reader.Read(buf); err != nil {
				return
			}
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for received.Load() == paused && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	conn.Close()
	<-drainDone
	if resumed := received.Load(); resumed == paused {
		t.Fatalf("server read nothing more after the peer drained %d bytes of replies; the pause never lifted", paused)
	}
}
