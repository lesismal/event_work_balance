//go:build linux || darwin || windows

package http

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

func newTestClient(t *testing.T, config ClientConfig) *Client {
	t.Helper()
	client := NewClient(startClientEngine(t), config)
	t.Cleanup(client.Close)
	return client
}

func mustRequest(t *testing.T, method, url string, body io.Reader) *stdhttp.Request {
	t.Helper()
	req, err := stdhttp.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func readBody(t *testing.T, resp *stdhttp.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// connCounter counts the distinct connections a test server sees.
type connCounter struct{ n atomic.Int64 }

func (c *connCounter) hook(_ net.Conn, state stdhttp.ConnState) {
	if state == stdhttp.StateNew {
		c.n.Add(1)
	}
}

func TestClientGetKeepsConnectionAlive(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("X-Path", r.URL.Path)
		fmt.Fprintf(w, "hello %s", r.URL.Query().Get("n"))
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	defer server.Close()
	client := newTestClient(t, DefaultClientConfig())

	for i := 0; i < 5; i++ {
		resp, err := client.Go(mustRequest(t, "GET", fmt.Sprintf("%s/p?n=%d", server.URL, i), nil)).Wait()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.StatusCode != 200 || resp.Header.Get("X-Path") != "/p" {
			t.Fatalf("request %d: status %d, X-Path %q", i, resp.StatusCode, resp.Header.Get("X-Path"))
		}
		if got, want := readBody(t, resp), fmt.Sprintf("hello %d", i); got != want {
			t.Fatalf("request %d: body %q, want %q", i, got, want)
		}
	}
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("five sequential requests used %d connections, want 1", n)
	}
}

func TestClientDoCallbackAndPostBody(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(stdhttp.StatusCreated)
		_, _ = w.Write(bytes.ToUpper(body))
	}))
	defer server.Close()
	client := newTestClient(t, DefaultClientConfig())

	done := make(chan struct{})
	var resp *stdhttp.Response
	var err error
	client.Do(mustRequest(t, "POST", server.URL, strings.NewReader("payload")), func(r *stdhttp.Response, e error) {
		resp, err = r, e
		close(done)
	})
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusCreated {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := readBody(t, resp); got != "PAYLOAD" {
		t.Fatalf("body %q", got)
	}
}

func TestClientChunkedResponse(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Trailer", "X-Sum")
		flusher := w.(stdhttp.Flusher)
		for i := 0; i < 50; i++ {
			fmt.Fprintf(w, "chunk-%02d;", i)
			flusher.Flush()
		}
		w.Header().Set("X-Sum", "50")
	}))
	defer server.Close()
	client := newTestClient(t, DefaultClientConfig())

	for round := 0; round < 2; round++ {
		resp, err := client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		body := readBody(t, resp)
		if !strings.HasPrefix(body, "chunk-00;") || !strings.HasSuffix(body, "chunk-49;") || len(body) != 50*9 {
			t.Fatalf("round %d: body %q", round, body)
		}
		if got := resp.Trailer.Get("X-Sum"); got != "50" {
			t.Fatalf("round %d: trailer X-Sum = %q", round, got)
		}
	}
}

func TestClientHeadIgnoresContentLength(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "1000")
		if r.Method != stdhttp.MethodHead {
			_, _ = w.Write(bytes.Repeat([]byte{'x'}, 1000))
		}
	}))
	defer server.Close()
	client := newTestClient(t, DefaultClientConfig())

	resp, err := client.Go(mustRequest(t, "HEAD", server.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.ContentLength != 1000 || readBody(t, resp) != "" {
		t.Fatalf("HEAD: ContentLength %d", resp.ContentLength)
	}
	// The connection must still be in step for the next request.
	resp, err = client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if n := len(readBody(t, resp)); n != 1000 {
		t.Fatalf("GET after HEAD: body %d bytes", n)
	}
}

// rawServer answers each connection with serve, for responses net/http would
// not produce.
func rawServer(t *testing.T, serve func(net.Conn, int)) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for i := 0; ; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(i int) {
				defer conn.Close()
				serve(conn, i)
			}(i)
		}
	}()
	return "http://" + listener.Addr().String()
}

func TestClientBodyDelimitedByClose(t *testing.T) {
	url := rawServer(t, func(conn net.Conn, _ int) {
		_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\nuntil the end")
	})
	client := newTestClient(t, DefaultClientConfig())
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "until the end" {
		t.Fatalf("body %q", got)
	}
}

func TestClientSkipsInterimResponses(t *testing.T) {
	url := rawServer(t, func(conn net.Conn, _ int) {
		_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </a>\r\n\r\n"+
			"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nfinal")
	})
	client := newTestClient(t, DefaultClientConfig())
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || readBody(t, resp) != "final" {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// A kept connection the server has closed without the client noticing yet is
// the ordinary way a reused connection fails. A GET on it must be sent again
// on a fresh one rather than fail.
func TestClientRetriesIdempotentRequestOnStaleConnection(t *testing.T) {
	var accepted atomic.Int64
	url := rawServer(t, func(conn net.Conn, _ int) {
		accepted.Add(1)
		reader := bufio.NewReader(conn)
		for served := 0; ; served++ {
			if _, err := stdhttp.ReadRequest(reader); err != nil {
				return
			}
			if served == 1 {
				// Take the second request and hang up without a word, as a
				// server does when its keep-alive timer beats the request.
				return
			}
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}
	})
	client := newTestClient(t, DefaultClientConfig())
	for i := 0; i < 2; i++ {
		resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("request %d: body %q", i, got)
		}
	}
	if n := accepted.Load(); n != 2 {
		t.Fatalf("server accepted %d connections, want 2: one reused and dropped, one for the retry", n)
	}

	// A POST is not safe to repeat, so the same failure reaches the caller.
	// A client of its own makes the first POST open a connection and the
	// second reuse it.
	client = newTestClient(t, DefaultClientConfig())
	resp, err := client.Go(mustRequest(t, "POST", url, strings.NewReader("x"))).Wait()
	if err != nil {
		t.Fatalf("POST on a fresh connection: %v", err)
	}
	readBody(t, resp)
	_, err = client.Go(mustRequest(t, "POST", url, strings.NewReader("x"))).Wait()
	if err == nil {
		t.Fatal("a POST dropped on a reused connection was retried")
	}
}

func TestClientTimeoutAndCancel(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	url := rawServer(t, func(conn net.Conn, _ int) {
		_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
		<-release
	})

	config := DefaultClientConfig()
	config.Timeout = 200 * time.Millisecond
	client := newTestClient(t, config)
	start := time.Now()
	_, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("err = %v, want a deadline error", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	req := mustRequest(t, "GET", url, nil).WithContext(ctx)
	future := client.Go(req)
	time.AfterFunc(50*time.Millisecond, cancel)
	if _, err := future.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestClientDialFailureReachesCallback(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	client := newTestClient(t, DefaultClientConfig())
	_, err = client.Go(mustRequest(t, "GET", "http://"+addr+"/", nil)).Wait()
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" {
		t.Fatalf("err = %v, want a dial *net.OpError", err)
	}
}

func TestClientRejectsHTTPS(t *testing.T) {
	client := newTestClient(t, DefaultClientConfig())
	_, err := client.Go(mustRequest(t, "GET", "https://example.com/", nil)).Wait()
	if !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("err = %v, want ErrUnsupportedScheme", err)
	}
}

// Many concurrent requests share at most MaxConnsPerHost connections; the rest
// wait their turn instead of failing or dialing past the cap.
func TestClientConcurrentRequestsRespectConnectionCap(t *testing.T) {
	const (
		requests = 200
		maxConns = 8
	)
	var conns connCounter
	var active, peak atomic.Int64
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		_, _ = io.WriteString(w, r.URL.Query().Get("i"))
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	defer server.Close()

	config := DefaultClientConfig()
	config.MaxConnsPerHost = maxConns
	config.MaxIdleConnsPerHost = maxConns
	client := newTestClient(t, config)
	futures := make([]*Future, requests)
	for i := range futures {
		futures[i] = client.Go(mustRequest(t, "GET", fmt.Sprintf("%s/?i=%d", server.URL, i), nil))
	}
	for i, f := range futures {
		resp, err := f.Wait()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if got := readBody(t, resp); got != fmt.Sprint(i) {
			t.Fatalf("request %d got the response %q", i, got)
		}
	}
	if n := conns.n.Load(); n > maxConns {
		t.Fatalf("server saw %d connections, want at most %d", n, maxConns)
	}
	if p := peak.Load(); p > maxConns {
		t.Fatalf("%d requests were in flight at once, want at most %d", p, maxConns)
	}
}

// A client can share an engine with a fib server, and even call that server:
// the client's connections carry their own handler, so neither side sees the
// other's traffic.
func TestClientSharesEngineWithServer(t *testing.T) {
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(200, "text/plain", []byte("served "+r.URL.Path))
	})))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	}()
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(engine, DefaultClientConfig())
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Go(mustRequest(t, "GET", fmt.Sprintf("http://%s/%d", addr, i), nil)).Wait()
			if err != nil {
				t.Error(err)
				return
			}
			if got, want := readBody(t, resp), fmt.Sprintf("served /%d", i); got != want {
				t.Errorf("body %q, want %q", got, want)
			}
		}(i)
	}
	wg.Wait()
}

func TestClientCloseFailsWaitingRequests(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	url := rawServer(t, func(conn net.Conn, _ int) {
		_, _ = stdhttp.ReadRequest(bufio.NewReader(conn))
		<-release
	})
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	client := NewClient(startClientEngine(t), config)
	first := client.Go(mustRequest(t, "GET", url, nil))
	second := client.Go(mustRequest(t, "GET", url, nil))
	time.Sleep(100 * time.Millisecond)
	client.Close()
	if _, err := second.Wait(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("waiting request: err = %v, want ErrClientClosed", err)
	}
	select {
	case <-first.Done():
		t.Fatal("the request already on a connection was cut off by Close")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := client.Go(mustRequest(t, "GET", url, nil)).Wait(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("request after Close: err = %v, want ErrClientClosed", err)
	}
}
