//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
)

// startClientEngine runs an engine with no listener, for clients only.
func startClientEngine(t *testing.T) *fib.Engine {
	t.Helper()
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = engine.Close()
	})
	return engine
}

// startWebSocketServer runs a fib WebSocket server and returns its ws:// URL.
func startWebSocketServer(t *testing.T, config Config, handler Handler) string {
	t.Helper()
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	server, err := fib.Bind(engineConfig, NewHandlerWithConfig(config, handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return "ws://" + addr.String() + "/ws"
}

// clientRecorder collects what a client handler is told.
type clientRecorder struct {
	messages chan Event
	opened   chan *stdhttp.Request
	closed   chan closeReport
}

type closeReport struct {
	code   uint16
	reason string
}

func newClientRecorder() *clientRecorder {
	return &clientRecorder{messages: make(chan Event, 64), opened: make(chan *stdhttp.Request, 1),
		closed: make(chan closeReport, 1)}
}

func (r *clientRecorder) handler() HandlerFuncs {
	return HandlerFuncs{
		Open: func(_ *Connection, req *stdhttp.Request) { r.opened <- req },
		Message: func(_ *Connection, opcode Opcode, payload []byte) {
			r.messages <- Event{Opcode: opcode, Payload: append([]byte(nil), payload...)}
		},
		Close: func(_ *Connection, code uint16, reason string, _ error) { r.closed <- closeReport{code, reason} },
	}
}

func (r *clientRecorder) next(t *testing.T) Event {
	t.Helper()
	select {
	case event := <-r.messages:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return Event{}
	}
}

func echoServerHandler() HandlerFuncs {
	return HandlerFuncs{Message: func(c *Connection, opcode Opcode, payload []byte) {
		if err := c.WriteMessage(opcode, payload); err != nil {
			_ = c.Close(CloseInternalError, "write failed")
		}
	}}
}

func TestClientEchoesThroughServer(t *testing.T) {
	url := startWebSocketServer(t, DefaultConfig(), echoServerHandler())
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	recorder := newClientRecorder()
	conn, resp, err := dialer.Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusSwitchingProtocols {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if req := <-recorder.opened; req.URL.Path != "/ws" {
		t.Fatalf("OnOpen got request for %q", req.URL.Path)
	}

	// Sizes that take each of the three payload-length encodings.
	for _, size := range []int{5, 1000, 100 << 10} {
		payload := bytes.Repeat([]byte{byte(size)}, size)
		if err := conn.WriteBinary(payload); err != nil {
			t.Fatal(err)
		}
		if event := recorder.next(t); event.Opcode != Binary || !bytes.Equal(event.Payload, payload) {
			t.Fatalf("%d-byte echo came back as %d bytes", size, len(event.Payload))
		}
	}
	if err := conn.WriteText("héllo"); err != nil {
		t.Fatal(err)
	}
	if event := recorder.next(t); event.Opcode != Text || string(event.Payload) != "héllo" {
		t.Fatalf("text echo = %v %q", event.Opcode, event.Payload)
	}

	if err := conn.Close(CloseNormal, "bye"); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-recorder.closed:
		if report.code != CloseNormal || report.reason != "bye" {
			t.Fatalf("OnClose = %d %q", report.code, report.reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose never ran")
	}
}

// The server's close reaches the client handler with the server's code, and
// the client answers it.
func TestClientSeesServerClose(t *testing.T) {
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{Message: func(c *Connection, _ Opcode, _ []byte) {
		_ = c.Close(ClosePolicyViolation, "go away")
	}})
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	recorder := newClientRecorder()
	conn, _, err := dialer.Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.WriteText("trigger")
	select {
	case report := <-recorder.closed:
		if report.code != ClosePolicyViolation || report.reason != "go away" {
			t.Fatalf("OnClose = %d %q", report.code, report.reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose never ran")
	}
}

func TestClientNegotiatesSubprotocol(t *testing.T) {
	config := DefaultConfig()
	config.Subprotocols = []string{"chat.v2", "chat.v1"}
	url := startWebSocketServer(t, config, echoServerHandler())
	dialerConfig := DefaultDialerConfig()
	dialerConfig.Subprotocols = []string{"chat.v1", "other"}
	conn, _, err := NewDialer(startClientEngine(t), dialerConfig).Go(url, nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := conn.Subprotocol(); got != "chat.v1" {
		t.Fatalf("subprotocol %q, want chat.v1", got)
	}
}

// rawServer runs serve on each accepted connection, for peers the package's
// own server would never be.
func rawServer(t *testing.T, serve func(net.Conn, *bufio.Reader, *stdhttp.Request)) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				req, err := stdhttp.ReadRequest(reader)
				if err != nil {
					return
				}
				serve(conn, reader, req)
			}()
		}
	}()
	return "ws://" + listener.Addr().String() + "/"
}

func acceptFor(key string) string {
	sum := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func upgradeResponse(req *stdhttp.Request) string {
	return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptFor(req.Header.Get("Sec-Websocket-Key")) + "\r\n\r\n"
}

// Every frame a client sends is masked, and a server frame that arrives in the
// same read as the handshake response is still delivered.
func TestClientMasksFramesAndReadsCoalescedFirstFrame(t *testing.T) {
	type observed struct {
		masked  bool
		payload string
	}
	frames := make(chan observed, 1)
	url := rawServer(t, func(conn net.Conn, reader *bufio.Reader, req *stdhttp.Request) {
		first, _ := MarshalFrame(Text, []byte("welcome"))
		_, _ = io.WriteString(conn, upgradeResponse(req)+string(first))
		var head [2]byte
		if _, err := io.ReadFull(reader, head[:]); err != nil {
			return
		}
		length := int(head[1] & 0x7f)
		var mask [4]byte
		masked := head[1]&0x80 != 0
		if masked {
			_, _ = io.ReadFull(reader, mask[:])
		}
		payload := make([]byte, length)
		_, _ = io.ReadFull(reader, payload)
		if masked {
			for i := range payload {
				payload[i] ^= mask[i&3]
			}
		}
		frames <- observed{masked: masked, payload: string(payload)}
	})
	recorder := newClientRecorder()
	conn, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if event := recorder.next(t); string(event.Payload) != "welcome" {
		t.Fatalf("first frame %q", event.Payload)
	}
	if err := conn.WriteText("masked?"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-frames:
		if !got.masked || got.payload != "masked?" {
			t.Fatalf("server saw masked=%v payload %q", got.masked, got.payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never saw the frame")
	}
}

// A masked frame from the server is a protocol error the client must answer
// by closing with 1002.
func TestClientRejectsMaskedServerFrame(t *testing.T) {
	url := rawServer(t, func(conn net.Conn, reader *bufio.Reader, req *stdhttp.Request) {
		// A masked text frame: a client's frame, sent the wrong way.
		frame := []byte{0x81, 0x80 | 2, 1, 2, 3, 4, 'h' ^ 1, 'i' ^ 2}
		_, _ = io.WriteString(conn, upgradeResponse(req)+string(frame))
		_, _ = io.Copy(io.Discard, reader)
	})
	recorder := newClientRecorder()
	_, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-recorder.closed:
		if report.code != CloseProtocolError {
			t.Fatalf("close code %d, want %d", report.code, CloseProtocolError)
		}
	case event := <-recorder.messages:
		t.Fatalf("a masked server frame was delivered: %q", event.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("the client never closed")
	}
}

func TestClientHandshakeFailures(t *testing.T) {
	cases := []struct {
		name   string
		reply  func(*stdhttp.Request) string
		status int
	}{
		{"refused", func(*stdhttp.Request) string {
			return "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"
		}, 403},
		{"wrong accept", func(*stdhttp.Request) string {
			return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
				"Sec-WebSocket-Accept: " + acceptFor("not the key") + "\r\n\r\n"
		}, 101},
		{"unoffered subprotocol", func(req *stdhttp.Request) string {
			return strings.TrimSuffix(upgradeResponse(req), "\r\n") + "Sec-WebSocket-Protocol: surprise\r\n\r\n"
		}, 101},
		{"closed early", nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := rawServer(t, func(conn net.Conn, _ *bufio.Reader, req *stdhttp.Request) {
				if tc.reply != nil {
					_, _ = io.WriteString(conn, tc.reply(req))
				}
			})
			var opened bool
			handler := HandlerFuncs{Open: func(*Connection, *stdhttp.Request) { opened = true }}
			conn, resp, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, handler).Wait()
			if !errors.Is(err, ErrBadHandshake) || conn != nil {
				t.Fatalf("err = %v, conn = %v; want ErrBadHandshake and no connection", err, conn)
			}
			if tc.status != 0 && (resp == nil || resp.StatusCode != tc.status) {
				t.Fatalf("response %v, want status %d", resp, tc.status)
			}
			if opened {
				t.Fatal("OnOpen ran for a failed handshake")
			}
		})
	}
}

func TestClientHandshakeTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	url := rawServer(t, func(net.Conn, *bufio.Reader, *stdhttp.Request) { <-release })
	config := DefaultDialerConfig()
	config.HandshakeTimeout = 200 * time.Millisecond
	_, _, err := NewDialer(startClientEngine(t), config).Go(url, nil, nil).Wait()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("err = %v, want a deadline error", err)
	}
}

func TestClientRejectsBadURLsAndHeaders(t *testing.T) {
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	if _, _, err := dialer.Go("wss://example.com/", nil, nil).Wait(); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("wss: err = %v, want ErrUnsupportedScheme", err)
	}
	header := stdhttp.Header{"Sec-Websocket-Key": {"mine"}}
	if _, _, err := dialer.Go("ws://127.0.0.1:1/", header, nil).Wait(); err == nil {
		t.Fatal("a caller-supplied Sec-WebSocket-Key was accepted")
	}
}

// Extra headers reach the server, and many clients can dial at once.
func TestClientConcurrentDialsWithHeaders(t *testing.T) {
	var mu sync.Mutex
	origins := map[string]bool{}
	config := DefaultConfig()
	config.CheckOrigin = func(r *stdhttp.Request) bool {
		mu.Lock()
		origins[r.Header.Get("Origin")] = true
		mu.Unlock()
		return true
	}
	url := startWebSocketServer(t, config, echoServerHandler())
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	const clients = 32
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recorder := newClientRecorder()
			header := stdhttp.Header{"Origin": {"http://client.test"}}
			conn, _, err := dialer.Go(url, header, recorder.handler()).Wait()
			if err != nil {
				t.Error(err)
				return
			}
			payload := make([]byte, 8)
			binary.BigEndian.PutUint64(payload, uint64(i))
			if err := conn.WriteBinary(payload); err != nil {
				t.Error(err)
				return
			}
			select {
			case event := <-recorder.messages:
				if !bytes.Equal(event.Payload, payload) {
					t.Errorf("client %d got another client's echo", i)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("client %d: no echo", i)
			}
			_ = conn.Close(CloseNormal, "")
		}(i)
	}
	wg.Wait()
	if !origins["http://client.test"] || len(origins) != 1 {
		t.Fatalf("server saw origins %v", origins)
	}
}
