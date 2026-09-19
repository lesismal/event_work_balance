//go:build linux || darwin || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/http/internal/hpack"
	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// serve runs an engine serving handler and returns its address.
func serve(t *testing.T, handler fib.Handler) string {
	t.Helper()
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
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return addr.String()
}

// echoHandler answers with the request's protocol, method, path and body.
func echoHandler() Handler {
	return HandlerFunc(func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		header := stdhttp.Header{"Content-Type": {"text/plain"}, "X-Proto": {r.Proto}}
		if cookie := r.Header.Get("Cookie"); cookie != "" {
			header.Set("X-Cookie", cookie)
		}
		if r.Trailer != nil {
			header.Set("X-Trailer", r.Trailer.Get("X-Sum"))
		}
		reply := fmt.Sprintf("%s %s %s", r.Method, r.URL.Path, body)
		if n, err := strconv.Atoi(r.URL.Query().Get("size")); err == nil {
			reply = strings.Repeat("x", n)
		}
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Header: header, Body: []byte(reply)})
	})
}

func TestH2ServerOverTLSWithNetHTTPClient(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(echoHandler())))
	client := &stdhttp.Client{
		Timeout:   10 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: clientConfig, ForceAttemptHTTP2: true},
	}
	defer client.CloseIdleConnections()

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := strings.Repeat(strconv.Itoa(i%10), i*1000)
			resp, err := client.Post(fmt.Sprintf("https://%s/post/%d", addr, i), "text/plain", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 || resp.Header.Get("X-Proto") != "HTTP/2.0" {
				errs <- fmt.Errorf("proto %s / %s", resp.Proto, resp.Header.Get("X-Proto"))
				return
			}
			if want := fmt.Sprintf("POST /post/%d %s", i, body); string(got) != want {
				errs <- fmt.Errorf("request %d: body of %d bytes, want %d", i, len(got), len(want))
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// A response far beyond the default windows exercises flow control.
	resp, err := client.Get(fmt.Sprintf("https://%s/big?size=%d", addr, 3<<20))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(got) != 3<<20 {
		t.Fatalf("big body %d bytes", len(got))
	}

	// HEAD reports the length and sends no body.
	resp, err = client.Head(fmt.Sprintf("https://%s/head", addr))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.ContentLength != int64(len("HEAD /head ")) {
		t.Fatalf("HEAD Content-Length %d", resp.ContentLength)
	}
}

func TestH2ServerStillServesHTTP1OverTLS(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(echoHandler())))
	clientConfig.NextProtos = []string{"http/1.1"}
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{TLSClientConfig: clientConfig}}
	defer client.CloseIdleConnections()
	resp, err := client.Get("https://" + addr + "/one")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("proto %s", resp.Proto)
	}
}

// TestH2ServerSniffsSplitHTTP1 checks that a request whose first bytes could
// still be the HTTP/2 preface is served as HTTP/1 once they turn out not to be.
func TestH2ServerSniffsSplitHTTP1(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for _, part := range []string{"P", "O", "ST /split HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n\r\nhi"} {
		if _, err = io.WriteString(conn, part); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf[:n], []byte("POST /split hi")) {
		t.Fatalf("response %q", buf[:n])
	}
}

// h2TestConn is a minimal HTTP/2 client speaking raw frames, for exercising
// what net/http's client never does.
type h2TestConn struct {
	t   *testing.T
	c   net.Conn
	enc *hpack.Encoder
	dec *hpack.Decoder
	in  []byte
}

func dialH2(t *testing.T, addr string, settings ...[2]uint32) *h2TestConn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	tc := &h2TestConn{t: t, c: c, enc: hpack.NewEncoder(), dec: hpack.NewDecoder(hpack.DefaultTableSize)}
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil, settings...)...))
	return tc
}

func (tc *h2TestConn) write(b []byte) {
	tc.t.Helper()
	if _, err := tc.c.Write(b); err != nil {
		tc.t.Fatal(err)
	}
}

// read returns the next frame, or fails the test on timeout or close.
func (tc *h2TestConn) read() h2Frame {
	tc.t.Helper()
	for {
		f, n, err := h2ReadFrame(tc.in, h2MaxFrameSizeLimit)
		if err != nil {
			tc.t.Fatal(err)
		}
		if n > 0 {
			f.payload = append([]byte(nil), f.payload...)
			tc.in = tc.in[n:]
			return f
		}
		buf := make([]byte, 32<<10)
		m, err := tc.c.Read(buf)
		if err != nil {
			tc.t.Fatalf("read: %v", err)
		}
		tc.in = append(tc.in, buf[:m]...)
	}
}

// readUntil skips frames until one of type typ arrives.
func (tc *h2TestConn) readUntil(typ h2FrameType) h2Frame {
	tc.t.Helper()
	for {
		if f := tc.read(); f.typ == typ {
			return f
		}
	}
}

func (tc *h2TestConn) headers(id uint32, endStream bool, fields ...string) {
	block := tc.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		block = tc.enc.AppendField(block, fields[i], fields[i+1], false)
	}
	tc.write(h2AppendHeaderBlock(nil, id, block, endStream, 16384))
}

func (tc *h2TestConn) data(id uint32, endStream bool, payload []byte) {
	var flags uint8
	if endStream {
		flags = h2FlagEndStream
	}
	tc.write(append(h2AppendFrameHeader(nil, h2FrameData, flags, id, len(payload)), payload...))
}

// response reads one stream's response headers and body.
func (tc *h2TestConn) response(id uint32) (map[string]string, []byte) {
	tc.t.Helper()
	header := map[string]string{}
	var body []byte
	for {
		f := tc.read()
		if f.streamID != id {
			continue
		}
		switch f.typ {
		case h2FrameHeaders:
			if err := tc.dec.Decode(f.payload, func(hf hpack.HeaderField) error {
				header[hf.Name] = hf.Value
				return nil
			}); err != nil {
				tc.t.Fatal(err)
			}
		case h2FrameData:
			body = append(body, f.payload...)
		case h2FrameRSTStream:
			tc.t.Fatalf("stream %d reset: %v", id, H2ErrorCode(binary.BigEndian.Uint32(f.payload)))
		}
		if f.has(h2FlagEndStream) && (f.typ == h2FrameHeaders || f.typ == h2FrameData) {
			return header, body
		}
	}
}

func get(path string) []string {
	return []string{":method", "GET", ":scheme", "http", ":authority", "test", ":path", path}
}

func TestH2ServerRespectsClientFlowControl(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 10})
	tc.headers(1, true, get("/?size=100")...)
	var body []byte
	for len(body) < 10 {
		f := tc.read()
		if f.typ == h2FrameData {
			body = append(body, f.payload...)
		}
	}
	if len(body) != 10 {
		t.Fatalf("sent %d bytes into a window of 10", len(body))
	}
	tc.write(h2AppendWindowUpdate(nil, 1, 1000))
	_, rest := tc.response(1)
	if len(body)+len(rest) != 100 {
		t.Fatalf("body %d bytes, want 100", len(body)+len(rest))
	}
}

func TestH2ServerMultiplexesAndHandlesContinuationCookiesAndTrailers(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr)
	// Stream 1: a header block split across HEADERS and CONTINUATION.
	block := tc.enc.Begin(nil)
	for _, kv := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":authority", "t"}, {":path", "/cont"},
		{"cookie", "a=1"}, {"cookie", "b=2"}} {
		block = tc.enc.AppendField(block, kv[0], kv[1], false)
	}
	tc.write(h2AppendHeaderBlock(nil, 1, block, true, 5))
	// Stream 3: a body in two DATA frames, then trailers.
	tc.headers(3, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/post")
	tc.data(3, false, []byte("hel"))
	tc.data(3, false, []byte("lo"))
	tc.headers(3, true, "x-sum", "42")

	header, body := tc.response(1)
	if string(body) != "GET /cont " || header["x-cookie"] != "a=1; b=2" {
		t.Fatalf("stream 1: %v %q", header, body)
	}
	header, body = tc.response(3)
	if string(body) != "POST /post hello" || header["x-trailer"] != "42" || header[":status"] != "200" {
		t.Fatalf("stream 3: %v %q", header, body)
	}

	// PING is answered in kind.
	tc.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), "pingpong"...))
	if f := tc.readUntil(h2FramePing); !f.has(h2FlagAck) || string(f.payload) != "pingpong" {
		t.Fatalf("PING reply %+v", f)
	}
}

func TestH2ServerRejectsOversizedBody(t *testing.T) {
	config := DefaultConfig()
	config.MaxBodyBytes = 10
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/")
	tc.data(1, false, make([]byte, 11))
	header, _ := tc.response(1)
	if header[":status"] != "413" {
		t.Fatalf("status %s", header[":status"])
	}
	// The connection carries on.
	tc.headers(3, true, get("/after")...)
	if _, body := tc.response(3); string(body) != "GET /after " {
		t.Fatalf("body %q", body)
	}
}

func TestH2ServerResetsMalformedRequestAndRefusesExcessStreams(t *testing.T) {
	config := DefaultConfig()
	config.MaxConcurrentStreams = 1
	release := make(chan struct{})
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/slow" {
			go func() {
				<-release
				_ = c.Respond(200, "", []byte("slow"))
			}()
			return
		}
		_ = c.Respond(200, "", nil)
	})))
	tc := dialH2(t, addr)
	// Missing :path.
	tc.headers(1, true, ":method", "GET", ":scheme", "http")
	if f := tc.readUntil(h2FrameRSTStream); f.streamID != 1 || H2ErrorCode(binary.BigEndian.Uint32(f.payload)) != H2ProtocolError {
		t.Fatalf("got %+v", f)
	}
	tc.headers(3, true, get("/slow")...)
	tc.headers(5, true, get("/fast")...)
	if f := tc.readUntil(h2FrameRSTStream); f.streamID != 5 || H2ErrorCode(binary.BigEndian.Uint32(f.payload)) != H2RefusedStream {
		t.Fatalf("got %+v", f)
	}
	close(release)
	if _, body := tc.response(3); string(body) != "slow" {
		t.Fatalf("body %q", body)
	}
}

func TestH2ServerConnectionErrorsSendGoAway(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr)
	// DATA on stream 0 is a connection error.
	tc.data(0, false, []byte("x"))
	f := tc.readUntil(h2FrameGoAway)
	if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); code != H2ProtocolError {
		t.Fatalf("GOAWAY %v", code)
	}
	buf := make([]byte, 64)
	for {
		if _, err := tc.c.Read(buf); err != nil {
			break
		}
	}
}

// TestH2ServerDisabled checks that DisableHTTP2 leaves the preface to HTTP/1,
// which rejects it.
func TestH2ServerDisabled(t *testing.T) {
	config := DefaultConfig()
	config.DisableHTTP2 = true
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.WriteString(conn, h2Preface); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := conn.Read(buf)
	if !bytes.HasPrefix(buf[:n], []byte("HTTP/1.1 ")) {
		t.Fatalf("response %q", buf[:n])
	}
}

func TestConfigureTLS(t *testing.T) {
	config := ConfigureTLS(&stdtls.Config{NextProtos: []string{"http/1.1", "acme-tls/1"}})
	if strings.Join(config.NextProtos, ",") != "h2,http/1.1,acme-tls/1" {
		t.Fatalf("NextProtos %v", config.NextProtos)
	}
}
