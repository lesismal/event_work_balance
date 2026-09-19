//go:build linux || darwin || windows

package http

// HTTP/1.0 and HTTP/1.1 conformance. The fib server is checked against
// net/http's client, against raw connections for what net/http would never
// send, and against curl when it is installed; the fib client against
// net/http's server and raw servers. Every test's name starts with
// TestHTTP1Conformance so that CI can run the suite on its own.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// ---------------------------------------------------------------------------
// Helpers

// serveHTTP1 runs a fib HTTP server and returns its host:port.
func serveHTTP1(t *testing.T, handler HandlerFunc) string {
	t.Helper()
	return serve(t, NewHandler(handler))
}

// stdClient is a net/http client that keeps to HTTP/1.1 and counts the
// connections it opens.
func stdClient(t *testing.T) (*stdhttp.Client, *atomic.Int64) {
	t.Helper()
	var dials atomic.Int64
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &stdhttp.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return dialer.DialContext(ctx, network, addr)
		},
		ExpectContinueTimeout: 5 * time.Second,
		DisableCompression:    true,
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &stdhttp.Client{Transport: transport, Timeout: 20 * time.Second}, &dials
}

// rawConn is a hand-driven client connection for requests net/http will not
// send.
type rawConn struct {
	t *testing.T
	net.Conn
	r *bufio.Reader
}

func dialRaw(t *testing.T, addr string) *rawConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	return &rawConn{t: t, Conn: conn, r: bufio.NewReader(conn)}
}

func (c *rawConn) send(s string) {
	c.t.Helper()
	if _, err := io.WriteString(c.Conn, s); err != nil {
		c.t.Fatal(err)
	}
}

// response reads one response to a request with method, body included.
func (c *rawConn) response(method string) (*stdhttp.Response, string) {
	c.t.Helper()
	resp, err := stdhttp.ReadResponse(c.r, &stdhttp.Request{Method: method})
	if err != nil {
		c.t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	return resp, string(body)
}

// head reads a response's status line and header as text, leaving the body.
func (c *rawConn) head() string {
	c.t.Helper()
	var head strings.Builder
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			c.t.Fatalf("reading header: %v (so far %q)", err, head.String())
		}
		head.WriteString(line)
		if line == "\r\n" {
			return head.String()
		}
	}
}

// closed reports whether the server has closed the connection.
func (c *rawConn) closed() bool {
	c.t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := c.r.Read(make([]byte, 1))
	return n == 0 && err != nil
}

func do(t *testing.T, client *stdhttp.Client, req *stdhttp.Request) (*stdhttp.Response, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

func randomFile(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), "file.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

// trailerBody is a request body that sets its trailer once it has been read
// to the end, as a streaming sender would.
type trailerBody struct {
	r       io.Reader
	trailer stdhttp.Header
	sum     string
}

func (b *trailerBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.trailer.Set("X-Sum", b.sum)
	}
	return n, err
}

// ---------------------------------------------------------------------------
// Server: HTTP/1.0

func TestHTTP1ConformanceServerHTTP10CloseByDefault(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(200, "text/plain", []byte(r.Proto))
	})
	conn := dialRaw(t, addr)
	conn.send("GET /a HTTP/1.0\r\n\r\n")
	resp, body := conn.response("GET")
	if resp.Proto != "HTTP/1.0" || body != "HTTP/1.0" || resp.ContentLength != 8 {
		t.Fatalf("proto %s, body %q, length %d", resp.Proto, body, resp.ContentLength)
	}
	if resp.Header.Get("Connection") != "" {
		t.Fatalf("Connection: %q", resp.Header.Get("Connection"))
	}
	if !conn.closed() {
		t.Fatal("connection kept open for an HTTP/1.0 request without keep-alive")
	}
}

func TestHTTP1ConformanceServerHTTP10KeepAlive(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(200, "text/plain", append([]byte(r.URL.Path), body...))
	})
	conn := dialRaw(t, addr)
	conn.send("GET /one HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	resp, body := conn.response("GET")
	if body != "/one" || !strings.EqualFold(resp.Header.Get("Connection"), "keep-alive") || resp.Close {
		t.Fatalf("body %q, Connection %q", body, resp.Header.Get("Connection"))
	}
	conn.send("POST /two HTTP/1.0\r\nConnection: keep-alive\r\nContent-Length: 3\r\n\r\nabc")
	if _, body = conn.response("POST"); body != "/twoabc" {
		t.Fatalf("second body %q", body)
	}
	conn.send("GET /three HTTP/1.0\r\n\r\n")
	if _, body = conn.response("GET"); body != "/three" {
		t.Fatalf("third body %q", body)
	}
	if !conn.closed() {
		t.Fatal("connection kept open after a request without keep-alive")
	}
}

// HTTP/1.0 has no chunks, so a streamed response of unknown length ends
// with the connection, and trailers are dropped.
func TestHTTP1ConformanceServerHTTP10StreamedBody(t *testing.T) {
	part := strings.Repeat("s", writerBufferSize)
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		c.Header().Set("Trailer", "X-After")
		for i := 0; i < 4; i++ {
			_, _ = c.WriteString(part)
			c.Flush()
		}
		c.Header().Set("X-After", "dropped")
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	head := conn.head()
	if !strings.HasPrefix(head, "HTTP/1.0 200 OK\r\n") || strings.Contains(head, "chunked") ||
		strings.Contains(head, "Content-Length") || strings.Contains(head, "Trailer") {
		t.Fatalf("head %q", head)
	}
	body, err := io.ReadAll(conn.r)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != strings.Repeat(part, 4) {
		t.Fatalf("body of %d bytes", len(body))
	}
}

func TestHTTP1ConformanceServerHTTP10RejectsTransferEncoding(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) { _ = c.Respond(200, "", nil) })
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n")
	if resp, _ := conn.response("POST"); resp.StatusCode != 400 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if !conn.closed() {
		t.Fatal("connection kept open after a malformed request")
	}
}

// Interim responses do not exist in HTTP/1.0.
func TestHTTP1ConformanceServerHTTP10NoInterim(t *testing.T) {
	result := make(chan error, 1)
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		result <- c.WriteInterim(103, stdhttp.Header{"Link": {"</a>"}})
		_ = c.Respond(200, "", nil)
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.0\r\n\r\n")
	if resp, _ := conn.response("GET"); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if err := <-result; !errors.Is(err, stdhttp.ErrNotSupported) {
		t.Fatalf("WriteInterim = %v", err)
	}
}

// ---------------------------------------------------------------------------
// Server: HTTP/1.1 framing, keep-alive and pipelining

func TestHTTP1ConformanceServerKeepAliveWithNetHTTP(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(200, "text/plain", append([]byte(r.Method+" "), body...))
	})
	client, dials := stdClient(t)
	for i := 0; i < 5; i++ {
		req, _ := stdhttp.NewRequest("POST", "http://"+addr+"/", strings.NewReader(strconv.Itoa(i)))
		resp, body := do(t, client, req)
		if resp.StatusCode != 200 || body != "POST "+strconv.Itoa(i) || resp.ProtoMinor != 1 {
			t.Fatalf("status %d body %q", resp.StatusCode, body)
		}
		if resp.Header.Get("Date") == "" {
			t.Fatal("no Date header")
		}
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("%d connections for keep-alive requests", n)
	}
}

func TestHTTP1ConformanceServerPipelining(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/stream" {
			// A streamed response must end before the next one starts.
			for i := 0; i < 3; i++ {
				_, _ = fmt.Fprintf(c, "%s-%d;", strings.Repeat("p", writerBufferSize), i)
				c.Flush()
			}
			return
		}
		_ = c.Respond(200, "text/plain", append([]byte(r.URL.Path), body...))
	})
	conn := dialRaw(t, addr)
	conn.send("GET /a HTTP/1.1\r\nHost: x\r\n\r\n" +
		"POST /b HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\nbody" +
		"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n" +
		"POST /c HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n" +
		"GET /d HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n" +
		"GET /never HTTP/1.1\r\nHost: x\r\n\r\n")
	for _, want := range []string{"/a", "/bbody", "stream", "/chi", "/d"} {
		method := "GET"
		resp, body := conn.response(method)
		if want == "stream" {
			if len(resp.TransferEncoding) != 1 || !strings.HasSuffix(body, "-2;") {
				t.Fatalf("streamed response: %v, %d bytes", resp.TransferEncoding, len(body))
			}
			continue
		}
		if body != want {
			t.Fatalf("body %q, want %q", body, want)
		}
	}
	if !conn.closed() {
		t.Fatal("request after Connection: close was served")
	}
}

func TestHTTP1ConformanceServerClientClose(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) { _ = c.Respond(200, "", []byte("x")) })
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
	resp, _ := conn.response("GET")
	if !resp.Close || !conn.closed() {
		t.Fatal("connection not closed")
	}
}

// A handler asking for Connection: close in its header ends the connection.
func TestHTTP1ConformanceServerHandlerClose(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/stream" {
			c.Header().Set("Connection", "close")
			_, _ = c.WriteString("streamed")
			return
		}
		_ = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"Connection": {"close"}}})
	})
	for _, path := range []string{"/", "/stream"} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n")
		resp, _ := conn.response("GET")
		if !resp.Close || !conn.closed() {
			t.Fatalf("%s: connection not closed", path)
		}
	}
}

func TestHTTP1ConformanceServerChunkedRequestWithTrailers(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		_ = c.Respond(200, "text/plain", fmt.Appendf(nil, "%v %d %t %s", r.TransferEncoding, len(body),
			hex.EncodeToString(sum[:]) == r.Trailer.Get("X-Sum"), r.Trailer.Get("X-Other")))
	})
	client, _ := stdClient(t)
	payload := bytes.Repeat([]byte("chunk"), 50000)
	sum := sha256.Sum256(payload)
	trailer := stdhttp.Header{"X-Sum": nil}
	req, _ := stdhttp.NewRequest("POST", "http://"+addr+"/", &trailerBody{
		r: bytes.NewReader(payload), trailer: trailer, sum: hex.EncodeToString(sum[:])})
	req.ContentLength = -1
	req.Trailer = trailer
	if _, body := do(t, client, req); body != fmt.Sprintf("[chunked] %d true ", len(payload)) {
		t.Fatalf("body %q", body)
	}

	// Chunk extensions and several trailer fields, raw.
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\nTrailer: X-Sum, X-Other\r\n\r\n" +
		"3;ext=1\r\nabc\r\n0\r\nX-Sum: nope\r\nX-Other: yes\r\n\r\n")
	if _, body := conn.response("POST"); body != "[chunked] 3 false yes" {
		t.Fatalf("raw body %q", body)
	}
}

// A request framed both ways is read as chunked, and its connection is not
// trusted with another request.
func TestHTTP1ConformanceServerContentLengthAndChunked(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(200, "", body)
	})
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"3\r\nabc\r\n0\r\n\r\n")
	resp, body := conn.response("POST")
	if body != "abc" || !resp.Close || !conn.closed() {
		t.Fatalf("body %q, close %v", body, resp.Close)
	}
}

func TestHTTP1ConformanceServerStreamedResponseWithTrailers(t *testing.T) {
	release := make(chan struct{})
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		c.Header().Set("Trailer", "X-Checksum")
		c.Header().Set("Content-Type", "application/octet-stream")
		_, _ = c.WriteString("first;")
		c.Flush()
		if r.URL.Query().Has("wait") {
			select {
			case <-release:
			case <-time.After(10 * time.Second):
			}
		}
		_, _ = c.WriteString(strings.Repeat("y", 3*writerBufferSize))
		c.Header().Set("X-Checksum", "abc123")
		c.Header().Set(stdhttp.TrailerPrefix+"X-Late", "late")
	})
	client, _ := stdClient(t)
	req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/", nil)
	resp, body := do(t, client, req)
	if len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked" || resp.ContentLength != -1 {
		t.Fatalf("framing %v %d", resp.TransferEncoding, resp.ContentLength)
	}
	if body != "first;"+strings.Repeat("y", 3*writerBufferSize) {
		t.Fatalf("body of %d bytes", len(body))
	}
	if resp.Trailer.Get("X-Checksum") != "abc123" || resp.Trailer.Get("X-Late") != "late" {
		t.Fatalf("trailer %v", resp.Trailer)
	}

	// The first chunk arrives while the handler is still running: Flush puts
	// it on the wire rather than leaving it for when the handler returns.
	conn := dialRaw(t, addr)
	conn.send("GET /?wait HTTP/1.1\r\nHost: x\r\n\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	head := conn.head()
	if !strings.Contains(head, "Transfer-Encoding: chunked\r\n") || !strings.Contains(head, "Trailer: X-Checksum\r\n") {
		t.Fatalf("head %q", head)
	}
	line, _ := conn.r.ReadString('\n')
	chunk := make([]byte, 6+2)
	_, _ = io.ReadFull(conn.r, chunk)
	if line != "6\r\n" || string(chunk) != "first;\r\n" {
		t.Fatalf("first chunk %q %q", line, chunk)
	}
	close(release)
}

func TestHTTP1ConformanceServerResponseTrailer(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		_ = c.WriteResponse(Response{StatusCode: 200, Body: []byte("payload"),
			Trailer: stdhttp.Header{"X-Digest": {"d1"}, "Content-Length": {"forbidden"}}})
	})
	client, _ := stdClient(t)
	req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/", nil)
	resp, body := do(t, client, req)
	if body != "payload" || resp.Trailer.Get("X-Digest") != "d1" || resp.Trailer.Get("Content-Length") != "" {
		t.Fatalf("body %q trailer %v", body, resp.Trailer)
	}
	// HTTP/1.0 gets the body by length, without trailers.
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.0\r\n\r\n")
	resp, body = conn.response("GET")
	if body != "payload" || resp.ContentLength != 7 || len(resp.TransferEncoding) != 0 {
		t.Fatalf("HTTP/1.0: body %q length %d", body, resp.ContentLength)
	}
}

func TestHTTP1ConformanceServerWriterFraming(t *testing.T) {
	writeErr := make(chan error, 1)
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/small":
			_, _ = c.WriteString("<html><body>small</body></html>")
		case "/declared":
			c.Header().Set("Content-Length", strconv.Itoa(3*writerBufferSize))
			for i := 0; i < 3; i++ {
				_, _ = c.WriteString(strings.Repeat("d", writerBufferSize))
				c.Flush()
			}
			_, err := c.WriteString("extra")
			writeErr <- err
		case "/short":
			c.Header().Set("Content-Length", "100")
			_, _ = c.WriteString("only this")
			c.Flush()
		case "/status":
			c.WriteHeader(stdhttp.StatusCreated)
		case "/nobody":
			c.WriteHeader(stdhttp.StatusNoContent)
			_, err := c.WriteString("x")
			writeErr <- err
		}
	})
	client, _ := stdClient(t)
	get := func(path string) (*stdhttp.Response, string, error) {
		req, _ := stdhttp.NewRequest("GET", "http://"+addr+path, nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp, string(body), err
	}
	resp, body, err := get("/small")
	if err != nil || resp.ContentLength != int64(len(body)) || resp.Header.Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("small: %v length %d type %q", err, resp.ContentLength, resp.Header.Get("Content-Type"))
	}
	resp, body, err = get("/declared")
	if err != nil || resp.ContentLength != 3*writerBufferSize || len(body) != 3*writerBufferSize ||
		len(resp.TransferEncoding) != 0 {
		t.Fatalf("declared: %v length %d body %d", err, resp.ContentLength, len(body))
	}
	if err := <-writeErr; !errors.Is(err, stdhttp.ErrContentLength) {
		t.Fatalf("write past Content-Length = %v", err)
	}
	if _, _, err = get("/short"); err == nil {
		t.Fatal("a body shorter than its Content-Length read as complete")
	}
	resp, body, err = get("/status")
	if err != nil || resp.StatusCode != 201 || resp.ContentLength != 0 || body != "" {
		t.Fatalf("status only: %v %d %d", err, resp.StatusCode, resp.ContentLength)
	}
	resp, _, err = get("/nobody")
	if err != nil || resp.StatusCode != 204 || resp.Header.Get("Content-Length") != "" {
		t.Fatalf("204: %v %v", err, resp.Header)
	}
	if err := <-writeErr; !errors.Is(err, stdhttp.ErrBodyNotAllowed) {
		t.Fatalf("write on 204 = %v", err)
	}
}

// Responses that cannot have a body carry no Content-Length, except a 304
// repeating one its handler gave, and a HEAD response has the length of the
// GET it stands for.
func TestHTTP1ConformanceServerBodilessResponses(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/204":
			_ = c.WriteResponse(Response{StatusCode: 204, Body: []byte("dropped")})
		case "/304":
			_ = c.WriteResponse(Response{StatusCode: 304, Header: stdhttp.Header{"Content-Length": {"42"}}})
		case "/head-declared":
			_ = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"Content-Length": {"42"}}})
		default:
			_ = c.Respond(200, "text/plain", []byte("twelve bytes"))
		}
	})
	conn := dialRaw(t, addr)
	conn.send("GET /204 HTTP/1.1\r\nHost: x\r\n\r\n")
	if head := conn.head(); strings.Contains(head, "Content-Length") || !strings.HasPrefix(head, "HTTP/1.1 204") {
		t.Fatalf("204 head %q", head)
	}
	conn.send("GET /304 HTTP/1.1\r\nHost: x\r\n\r\n")
	if head := conn.head(); !strings.Contains(head, "Content-Length: 42\r\n") {
		t.Fatalf("304 head %q", head)
	}
	conn.send("HEAD / HTTP/1.1\r\nHost: x\r\n\r\n")
	if head := conn.head(); !strings.Contains(head, "Content-Length: 12\r\n") {
		t.Fatalf("HEAD head %q", head)
	}
	conn.send("HEAD /head-declared HTTP/1.1\r\nHost: x\r\n\r\n")
	if head := conn.head(); !strings.Contains(head, "Content-Length: 42\r\n") {
		t.Fatalf("HEAD declared head %q", head)
	}
	// The connection is still in step: nothing was sent after the headers.
	conn.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, body := conn.response("GET"); body != "twelve bytes" {
		t.Fatalf("body after bodiless responses %q", body)
	}
}

func TestHTTP1ConformanceServerExpectContinue(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(200, "", body)
	})
	client, _ := stdClient(t)
	var got100 atomic.Bool
	trace := &httptrace.ClientTrace{Got100Continue: func() { got100.Store(true) }}
	req, _ := stdhttp.NewRequest("PUT", "http://"+addr+"/", strings.NewReader("expected"))
	req.Header.Set("Expect", "100-continue")
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	if _, body := do(t, client, req); body != "expected" || !got100.Load() {
		t.Fatalf("body %q, 100 Continue %v", body, got100.Load())
	}

	conn := dialRaw(t, addr)
	conn.send("PUT / HTTP/1.1\r\nHost: x\r\nExpect: something-else\r\nContent-Length: 1\r\n\r\nx")
	if resp, _ := conn.response("PUT"); resp.StatusCode != stdhttp.StatusExpectationFailed {
		t.Fatalf("unknown expectation: status %d", resp.StatusCode)
	}
}

func TestHTTP1ConformanceServerInterimResponses(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		c.Header().Set("Link", "</style.css>; rel=preload")
		c.WriteHeader(stdhttp.StatusEarlyHints)
		c.Header().Del("Link")
		_, _ = c.WriteString("final")
	})
	client, _ := stdClient(t)
	var hints []string
	trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
		hints = append(hints, fmt.Sprintf("%d %s", code, header.Get("Link")))
		return nil
	}}
	req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/", nil)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, body := do(t, client, req)
	if body != "final" || len(hints) != 1 || hints[0] != "103 </style.css>; rel=preload" || resp.Header.Get("Link") != "" {
		t.Fatalf("body %q hints %q", body, hints)
	}
}

func TestHTTP1ConformanceServerRejectsBadRequests(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) { _ = c.Respond(200, "", []byte("ok")) })
	cases := []struct {
		name, request string
		status        int
	}{
		{"missing host", "GET / HTTP/1.1\r\n\r\n", 400},
		{"two hosts", "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", 400},
		{"unknown transfer coding", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", 501},
		{"bad chunk size", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", 400},
		{"bad content length", "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1, 2\r\n\r\n", 400},
		{"bad request line", "GET\r\n\r\n", 400},
		{"space before colon", "GET / HTTP/1.1\r\nHost : x\r\n\r\n", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialRaw(t, addr)
			conn.send(tc.request)
			resp, _ := conn.response("GET")
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
			if !conn.closed() {
				t.Fatal("connection kept open after a refused request")
			}
		})
	}
	// HTTP/1.0 needs no Host.
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.0\r\n\r\n")
	if resp, _ := conn.response("GET"); resp.StatusCode != 200 {
		t.Fatalf("HTTP/1.0 without Host: status %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Server: files, through net/http's own helpers and the sendfile path

func fileHandler(path string) HandlerFunc {
	return func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/copy":
			// io.Copy from an *os.File, with no length announced: chunked,
			// each chunk sent from the file.
			f, err := os.Open(path)
			if err != nil {
				c.WriteHeader(500)
				return
			}
			defer f.Close()
			c.Header().Set("Content-Type", "application/octet-stream")
			_, _ = c.WriteString("prefix:")
			_, _ = io.Copy(c, f)
		case "/section":
			f, err := os.Open(path)
			if err != nil {
				c.WriteHeader(500)
				return
			}
			defer f.Close()
			_, _ = f.Seek(1000, io.SeekStart)
			c.Header().Set("Content-Length", strconv.Itoa(100000))
			_, _ = io.CopyN(c, f, 100000)
		default:
			stdhttp.ServeFile(c, r, path)
		}
	}
}

func TestHTTP1ConformanceServerFiles(t *testing.T) {
	path, data := randomFile(t, 3<<20+123)
	addr := serveHTTP1(t, fileHandler(path))
	client, _ := stdClient(t)
	get := func(path string, header ...string) (*stdhttp.Response, string) {
		req, _ := stdhttp.NewRequest("GET", "http://"+addr+path, nil)
		for i := 0; i+1 < len(header); i += 2 {
			req.Header.Set(header[i], header[i+1])
		}
		return do(t, client, req)
	}
	resp, body := get("/file")
	if resp.StatusCode != 200 || body != string(data) || resp.ContentLength != int64(len(data)) {
		t.Fatalf("whole file: status %d, %d bytes", resp.StatusCode, len(body))
	}
	resp, body = get("/file", "Range", "bytes=100-199")
	if resp.StatusCode != 206 || body != string(data[100:200]) ||
		resp.Header.Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(data)) {
		t.Fatalf("range: status %d, %d bytes", resp.StatusCode, len(body))
	}
	resp, body = get("/file", "Range", fmt.Sprintf("bytes=%d-", len(data)-70000))
	if resp.StatusCode != 206 || body != string(data[len(data)-70000:]) {
		t.Fatalf("tail range: status %d, %d bytes", resp.StatusCode, len(body))
	}
	resp, body = get("/file", "Range", "bytes=0-9,20-29")
	if resp.StatusCode != 206 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/byteranges") ||
		!strings.Contains(body, string(data[20:30])) {
		t.Fatalf("multipart range: status %d", resp.StatusCode)
	}
	lastModified := resp.Header.Get("Last-Modified")
	resp, body = get("/file", "If-Modified-Since", lastModified)
	if resp.StatusCode != 304 || body != "" {
		t.Fatalf("conditional: status %d", resp.StatusCode)
	}
	resp, _ = get("/file", "Range", fmt.Sprintf("bytes=%d-", len(data)+10))
	if resp.StatusCode != 416 {
		t.Fatalf("unsatisfiable range: status %d", resp.StatusCode)
	}
	resp, body = get("/copy")
	if len(resp.TransferEncoding) != 1 || body != "prefix:"+string(data) {
		t.Fatalf("io.Copy: %v, %d bytes", resp.TransferEncoding, len(body))
	}
	resp, body = get("/section")
	if resp.ContentLength != 100000 || body != string(data[1000:101000]) {
		t.Fatalf("section: length %d, %d bytes", resp.ContentLength, len(body))
	}
	req, _ := stdhttp.NewRequest("HEAD", "http://"+addr+"/file", nil)
	if resp, body = do(t, client, req); resp.ContentLength != int64(len(data)) || body != "" {
		t.Fatalf("HEAD: length %d", resp.ContentLength)
	}

	// HTTP/1.0, where io.Copy's unknown length ends with the connection.
	conn := dialRaw(t, addr)
	conn.send("GET /file HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	if resp, body = conn.response("GET"); body != string(data) || resp.Close {
		t.Fatalf("HTTP/1.0 file: %d bytes, close %v", len(body), resp.Close)
	}
	conn.send("GET /copy HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
	if resp, body = conn.response("GET"); body != "prefix:"+string(data) || !resp.Close {
		t.Fatalf("HTTP/1.0 copy: %d bytes, close %v", len(body), resp.Close)
	}
}

// Over TLS the file cannot go to the socket directly, and is read and
// encrypted instead.
func TestHTTP1ConformanceServerFilesOverTLS(t *testing.T) {
	path, data := randomFile(t, 1<<20+7)
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultConfig()
	config.DisableHTTP2 = true
	addr := serve(t, fibtls.NewServer(serverConfig, NewHandlerWithConfig(config, fileHandler(path))))
	client := &stdhttp.Client{Timeout: 20 * time.Second, Transport: &stdhttp.Transport{TLSClientConfig: clientConfig}}
	for _, target := range []string{"/file", "/copy"} {
		req, _ := stdhttp.NewRequest("GET", "https://"+addr+target, nil)
		resp, body := do(t, client, req)
		want := string(data)
		if target == "/copy" {
			want = "prefix:" + want
		}
		if resp.ProtoMajor != 1 || body != want {
			t.Fatalf("%s: %s, %d bytes", target, resp.Proto, len(body))
		}
	}
}

// ---------------------------------------------------------------------------
// Server: curl as a third-party client

func requireCurl(t *testing.T) string {
	t.Helper()
	curl, err := exec.LookPath("curl")
	if err != nil {
		if os.Getenv("FIB_REQUIRE_CURL") != "" {
			t.Fatal("curl is required but not installed")
		}
		t.Skip("curl not installed")
	}
	return curl
}

func runCurl(t *testing.T, curl string, args ...string) (stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := exec.Command(curl, append([]string{"--silent", "--show-error", "--max-time", "30"}, args...)...)
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		t.Fatalf("curl %v: %v\n%s", args, err, errOut.String())
	}
	return out.String(), errOut.String()
}

func TestHTTP1ConformanceServerWithCurl(t *testing.T) {
	curl := requireCurl(t)
	path, data := randomFile(t, 2<<20+99)
	files := fileHandler(path)
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/echo":
			body, _ := io.ReadAll(r.Body)
			sum := sha256.Sum256(body)
			_ = c.Respond(200, "text/plain", fmt.Appendf(nil, "%s %v %d %x", r.Proto, r.TransferEncoding, len(body), sum))
		case "/stream":
			c.Header().Set("Trailer", "X-Done")
			for i := 0; i < 5; i++ {
				_, _ = c.WriteString(strings.Repeat("z", writerBufferSize))
				c.Flush()
			}
			c.Header().Set("X-Done", "yes")
		default:
			files(c, r)
		}
	})
	base := "http://" + addr

	t.Run("http1.0", func(t *testing.T) {
		out, _ := runCurl(t, curl, "--http1.0", "--include", base+"/echo")
		if !strings.HasPrefix(out, "HTTP/1.0 200 OK") || !strings.HasSuffix(out, "HTTP/1.0 [] 0 "+hex.EncodeToString(sha256Of(nil))) {
			t.Fatalf("output %q", out)
		}
	})
	t.Run("keep-alive", func(t *testing.T) {
		_, stderr := runCurl(t, curl, "--verbose", "--output", os.DevNull, "--output", os.DevNull, base+"/echo", base+"/echo")
		if !strings.Contains(strings.ToLower(stderr), "existing connection") {
			t.Fatalf("second request did not reuse the connection:\n%s", stderr)
		}
	})
	t.Run("chunked-upload", func(t *testing.T) {
		out, _ := runCurl(t, curl, "--header", "Transfer-Encoding: chunked", "--data-binary", "@"+path, base+"/echo")
		if want := fmt.Sprintf("HTTP/1.1 [chunked] %d %x", len(data), sha256Of(data)); out != want {
			t.Fatalf("output %q, want %q", out, want)
		}
	})
	t.Run("download", func(t *testing.T) {
		for _, target := range []string{"/file", "/copy"} {
			dst := filepath.Join(t.TempDir(), "download")
			runCurl(t, curl, "--output", dst, base+target)
			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			want := data
			if target == "/copy" {
				want = append([]byte("prefix:"), data...)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("%s: %d bytes differ from the file", target, len(got))
			}
		}
	})
	t.Run("range", func(t *testing.T) {
		out, _ := runCurl(t, curl, "--range", "10-19", base+"/file")
		if out != string(data[10:20]) {
			t.Fatal("range differs from the file")
		}
	})
	t.Run("http1.0-download", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "download")
		runCurl(t, curl, "--http1.0", "--output", dst, base+"/copy")
		got, _ := os.ReadFile(dst)
		if !bytes.Equal(got, append([]byte("prefix:"), data...)) {
			t.Fatalf("%d bytes differ from the file", len(got))
		}
	})
	t.Run("raw-chunked-trailers", func(t *testing.T) {
		out, _ := runCurl(t, curl, "--raw", base+"/stream")
		if !strings.HasSuffix(out, "0\r\nX-Done: yes\r\n\r\n") || !strings.HasPrefix(out, "1000\r\n") {
			t.Fatalf("raw output ends %q", out[max(len(out)-40, 0):])
		}
	})
}

func sha256Of(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

// ---------------------------------------------------------------------------
// Client

func TestHTTP1ConformanceClientKeepAlive(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	for i := 0; i < 4; i++ {
		resp, err := client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if body := readBody(t, resp); body != "HTTP/1.1" {
			t.Fatalf("body %q", body)
		}
	}
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("%d connections", n)
	}
}

func TestHTTP1ConformanceClientHTTP10(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "%s %s %v %d %s", r.Proto, r.Method, r.TransferEncoding, r.ContentLength, body)
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	send := func(method string, body io.Reader, keepAlive bool) string {
		req := mustRequest(t, method, server.URL, body)
		req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/1.0", 1, 0
		if body != nil {
			// No length: HTTP/1.1 would chunk it, HTTP/1.0 cannot.
			req.ContentLength = -1
			req.Trailer = stdhttp.Header{"X-Dropped": {"x"}}
		}
		if keepAlive {
			req.Header.Set("Connection", "keep-alive")
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if resp.ProtoMinor != 0 {
			t.Fatalf("response %s to an HTTP/1.0 request", resp.Proto)
		}
		return readBody(t, resp)
	}
	if got := send("GET", nil, false); got != "HTTP/1.0 GET [] 0 " {
		t.Fatalf("GET: %q", got)
	}
	if got := send("POST", strings.NewReader("hello"), false); got != "HTTP/1.0 POST [] 5 hello" {
		t.Fatalf("POST: %q", got)
	}
	if n := conns.n.Load(); n != 2 {
		t.Fatalf("%d connections for two HTTP/1.0 requests without keep-alive", n)
	}
	for i := 0; i < 3; i++ {
		if got := send("GET", nil, true); got != "HTTP/1.0 GET [] 0 " {
			t.Fatalf("keep-alive GET: %q", got)
		}
	}
	if n := conns.n.Load(); n != 3 {
		t.Fatalf("%d connections, want one more for the keep-alive requests", n)
	}
}

func TestHTTP1ConformanceClientChunkedResponseWithTrailers(t *testing.T) {
	part := strings.Repeat("c", 10000)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Trailer", "X-Sum")
		for i := 0; i < 20; i++ {
			_, _ = io.WriteString(w, part)
			w.(stdhttp.Flusher).Flush()
		}
		w.Header().Set("X-Sum", "s1")
		w.Header().Set(stdhttp.TrailerPrefix+"X-Late", "s2")
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	resp, err := client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != strings.Repeat(part, 20) {
		t.Fatalf("body of %d bytes", len(body))
	}
	if len(resp.TransferEncoding) != 1 || resp.Trailer.Get("X-Sum") != "s1" || resp.Trailer.Get("X-Late") != "s2" {
		t.Fatalf("framing %v trailer %v", resp.TransferEncoding, resp.Trailer)
	}
}

func TestHTTP1ConformanceClientChunkedRequestWithTrailers(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "%v %d %s", r.TransferEncoding, len(body), r.Trailer.Get("X-Sum"))
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	req := mustRequest(t, "POST", server.URL, strings.NewReader(strings.Repeat("u", 70000)))
	req.ContentLength = -1
	req.Trailer = stdhttp.Header{"X-Sum": {"sum"}}
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "[chunked] 70000 sum" {
		t.Fatalf("body %q", body)
	}
}

func TestHTTP1ConformanceClientBodilessResponses(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/204":
			w.WriteHeader(204)
		case "/304":
			w.WriteHeader(304)
		default:
			w.Header().Set("Content-Length", "11")
			_, _ = io.WriteString(w, "eleven byte")
		}
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	for _, tc := range []struct{ method, path, body string }{
		{"HEAD", "/", ""}, {"GET", "/204", ""}, {"GET", "/304", ""}, {"GET", "/", "eleven byte"},
	} {
		resp, err := client.Go(mustRequest(t, tc.method, server.URL+tc.path, nil)).Wait()
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if body := readBody(t, resp); body != tc.body {
			t.Fatalf("%s %s: body %q", tc.method, tc.path, body)
		}
	}
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("%d connections: bodiless responses put the connection out of step", n)
	}
}

func TestHTTP1ConformanceClientInterimAndExpectContinue(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Link", "</a>")
		w.WriteHeader(stdhttp.StatusEarlyHints)
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	req := mustRequest(t, "PUT", server.URL, strings.NewReader("continued"))
	req.Header.Set("Expect", "100-continue")
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || readBody(t, resp) != "continued" {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestHTTP1ConformanceClientConnectionClose(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Connection", "close")
		_, _ = io.WriteString(w, "bye")
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	t.Cleanup(server.Close)
	client := newTestClient(t, DefaultClientConfig())
	for i := 0; i < 2; i++ {
		resp, err := client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if !resp.Close || readBody(t, resp) != "bye" {
			t.Fatal("Connection: close not reported")
		}
	}
	if n := conns.n.Load(); n != 2 {
		t.Fatalf("%d connections: a closed connection was reused", n)
	}
}

func TestHTTP1ConformanceClientMalformedResponses(t *testing.T) {
	cases := []struct {
		name, response string
		want           error
	}{
		{"short body", "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort", io.ErrUnexpectedEOF},
		{"unknown transfer coding", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\nxx", ErrMalformedResponse},
		{"bad chunk", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", ErrMalformedResponse},
		{"bad status line", "HTTP/1.1 abc\r\n\r\n", ErrMalformedResponse},
		{"truncated chunks", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab", io.ErrUnexpectedEOF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url := rawServer(t, func(conn net.Conn, _ int) {
				_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
				_, _ = io.WriteString(conn, tc.response)
			})
			client := newTestClient(t, DefaultClientConfig())
			_, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v, want %v", err, tc.want)
			}
		})
	}
}

func TestHTTP1ConformanceClientHTTP10Responses(t *testing.T) {
	url := rawServer(t, func(conn net.Conn, i int) {
		reader := bufio.NewReader(conn)
		for {
			req, err := stdhttp.ReadRequest(reader)
			if err != nil {
				return
			}
			if req.URL.Path == "/keep" {
				_, _ = io.WriteString(conn, "HTTP/1.0 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\nkept")
				continue
			}
			_, _ = io.WriteString(conn, "HTTP/1.0 200 OK\r\n\r\nuntil close")
			return
		}
	})
	client := newTestClient(t, DefaultClientConfig())
	for _, tc := range []struct{ path, body string }{{"/keep", "kept"}, {"/keep", "kept"}, {"/close", "until close"}} {
		resp, err := client.Go(mustRequest(t, "GET", url+tc.path, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if body := readBody(t, resp); body != tc.body || resp.ProtoMinor != 0 {
			t.Fatalf("%s: %s body %q", tc.path, resp.Proto, body)
		}
	}
}

// ---------------------------------------------------------------------------
// fib client against fib server

func TestHTTP1ConformanceFibClientAndServer(t *testing.T) {
	path, data := randomFile(t, 1<<20+5)
	files := fileHandler(path)
	addr := serveHTTP1(t, func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/echo" {
			body, _ := io.ReadAll(r.Body)
			c.Header().Set("Trailer", "X-Len")
			_, _ = fmt.Fprintf(c, "%s %v %s ", r.Proto, r.TransferEncoding, r.Trailer.Get("X-Req"))
			_, _ = c.Write(bytes.Repeat(body, 2000))
			c.Header().Set("X-Len", strconv.Itoa(len(body)))
			return
		}
		files(c, r)
	})
	client := newTestClient(t, DefaultClientConfig())
	req := mustRequest(t, "POST", "http://"+addr+"/echo", strings.NewReader("abc"))
	req.ContentLength = -1
	req.Trailer = stdhttp.Header{"X-Req": {"r"}}
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "HTTP/1.1 [chunked] r "+strings.Repeat("abc", 2000) ||
		resp.Trailer.Get("X-Len") != "3" {
		t.Fatalf("echo: body %d bytes, trailer %v", len(body), resp.Trailer)
	}
	for _, target := range []string{"/file", "/copy"} {
		resp, err = client.Go(mustRequest(t, "GET", "http://"+addr+target, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		want := string(data)
		if target == "/copy" {
			want = "prefix:" + want
		}
		if body := readBody(t, resp); body != want {
			t.Fatalf("%s: %d bytes differ", target, len(body))
		}
	}
	req = mustRequest(t, "GET", "http://"+addr+"/copy", nil)
	req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/1.0", 1, 0
	if resp, err = client.Go(req).Wait(); err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "prefix:"+string(data) || resp.ProtoMinor != 0 {
		t.Fatalf("HTTP/1.0 copy: %s, %d bytes", resp.Proto, len(body))
	}
}
