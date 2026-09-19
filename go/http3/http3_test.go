//go:build linux || darwin || windows

package http3

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/internal/tlstest"
)

func runEngine(t *testing.T, engine *fib.Engine) {
	t.Helper()
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = engine.Close()
	})
}

// startServer serves handler over HTTP/3 on a UDP port of its own and
// returns the base URL.
func startServer(t *testing.T, config Config, handler fibhttp.HandlerFunc) string {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	config.TLSConfig = serverTLS
	fc := fib.DefaultConfig()
	fc.Network = "udp"
	fc.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(fc, NewHandlerWithConfig(config, handler))
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("https://localhost:%d", addr.Port)
}

func newClient(t *testing.T, configure func(*ClientConfig)) *Client {
	t.Helper()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	config := DefaultClientConfig()
	config.TLSConfig = clientTLS
	// localhost resolves to 127.0.0.1 here and certificates name it.
	config.TLSConfig.ServerName = "localhost"
	if configure != nil {
		configure(&config)
	}
	client := NewClient(engine, config)
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

func echo(c *fibhttp.Context, r *stdhttp.Request) {
	body, _ := io.ReadAll(r.Body)
	header := stdhttp.Header{"X-Proto": {r.Proto}, "Content-Type": {"text/plain"}}
	if r.TLS != nil {
		header.Set("X-Alpn", r.TLS.NegotiatedProtocol)
	}
	_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header,
		Body: []byte(fmt.Sprintf("%s %s %s", r.Method, r.URL.RequestURI(), body))})
}

func TestRequestResponse(t *testing.T) {
	url := startServer(t, Config{}, echo)
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url+"/echo?x=1", strings.NewReader("hello"))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "POST /echo?x=1 hello" {
		t.Fatalf("body %q", got)
	}
	if resp.ProtoMajor != 3 || resp.Header.Get("X-Proto") != "HTTP/3.0" || resp.Header.Get("X-Alpn") != "h3" {
		t.Fatalf("proto %s, server saw %s over %q", resp.Proto, resp.Header.Get("X-Proto"), resp.Header.Get("X-Alpn"))
	}
	if resp.ContentLength != int64(len("POST /echo?x=1 hello")) || resp.TLS == nil {
		t.Fatalf("content length %d, TLS %v", resp.ContentLength, resp.TLS)
	}
}

func TestManyConcurrentRequests(t *testing.T) {
	url := startServer(t, Config{MaxConcurrentStreams: 10}, echo)
	client := newClient(t, nil)
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := strings.Repeat("b", i)
			resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, fmt.Sprintf("%s/p?i=%d", url, i), strings.NewReader(body))).Wait()
			if err != nil {
				t.Error(err)
				return
			}
			if got, want := readBody(t, resp), fmt.Sprintf("POST /p?i=%d %s", i, body); got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		}(i)
	}
	wg.Wait()
	client.mu.Lock()
	conns := len(client.conns)
	client.mu.Unlock()
	if conns != 1 {
		t.Fatalf("%d connections, want 1", conns)
	}
}

func TestLargeBodies(t *testing.T) {
	url := startServer(t, Config{MaxBodyBytes: 32 << 20}, func(c *fibhttp.Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, nil)
	payload := make([]byte, 8<<20)
	_, _ = rand.Read(payload)
	start := time.Now()
	resp, err := client.Go(mustRequest(t, stdhttp.MethodPut, url+"/big", bytes.NewReader(payload))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(payload) {
		t.Fatalf("got %d bytes back, want %d", len(got), len(payload))
	}
	t.Logf("8 MiB each way in %v", time.Since(start))
}

func TestRequestTooLarge(t *testing.T) {
	url := startServer(t, Config{MaxBodyBytes: 1 << 10}, echo)
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url, strings.NewReader(strings.Repeat("x", 4<<10)))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// The connection is still good.
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, url+"/after", nil)).Wait()
	if err != nil || readBody(t, resp) != "GET /after " {
		t.Fatalf("after: %v", err)
	}
}

func TestHeadAndNoContent(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		if r.URL.Path == "/empty" {
			_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusNoContent})
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("body"))
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodHead, url+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "" || resp.ContentLength != 4 {
		t.Fatalf("HEAD: body %q, length %d", got, resp.ContentLength)
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, url+"/empty", nil)).Wait()
	if err != nil || resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("204: %v %v", resp, err)
	}
}

func TestInterimResponse(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		if err := c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"</style.css>; rel=preload"}}); err != nil {
			t.Error(err)
		}
		if err := c.Push("/style.css", nil); !errors.Is(err, stdhttp.ErrNotSupported) {
			t.Errorf("push: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("final"))
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusOK || readBody(t, resp) != "final" {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestResponseHeaders(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusTeapot, Header: stdhttp.Header{
			"Set-Cookie": {"a=1", "b=2"},
			"X-Echo":     {r.Header.Get("X-Custom")},
			"X-Cookie":   {r.Header.Get("Cookie")},
			"Connection": {"close"},
		}})
	})
	client := newClient(t, nil)
	req := mustRequest(t, stdhttp.MethodGet, url+"/", nil)
	req.Header.Set("X-Custom", "value with spaces")
	req.AddCookie(&stdhttp.Cookie{Name: "c", Value: "3"})
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusTeapot || len(resp.Header["Set-Cookie"]) != 2 ||
		resp.Header.Get("X-Echo") != "value with spaces" || resp.Header.Get("X-Cookie") != "c=3" ||
		resp.Header.Get("Connection") != "" {
		t.Fatalf("status %d header %v", resp.StatusCode, resp.Header)
	}
}

func TestAsyncHandler(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		go func() {
			time.Sleep(10 * time.Millisecond)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("later"))
		}()
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait()
	if err != nil || readBody(t, resp) != "later" {
		t.Fatalf("%v", err)
	}
}

func TestTimeoutAndCancel(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		go func() {
			<-release
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
		}()
	})
	client := newClient(t, func(config *ClientConfig) { config.Timeout = 200 * time.Millisecond })
	_, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/slow", nil)).Wait()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req := mustRequest(t, stdhttp.MethodGet, url+"/slow", nil).WithContext(ctx)
	f := client.Go(req)
	time.AfterFunc(50*time.Millisecond, cancel)
	if _, err := f.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestResponseCloseRetiresConnection(t *testing.T) {
	var mu sync.Mutex
	remotes := map[string]bool{}
	url := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		mu.Unlock()
		_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Body: []byte("ok"), Close: r.URL.Path == "/close"})
	})
	client := newClient(t, nil)
	for _, path := range []string{"/a", "/close", "/b"} {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+path, nil)).Wait()
		if err != nil || readBody(t, resp) != "ok" {
			t.Fatalf("%s: %v", path, err)
		}
		// Give the GOAWAY a moment to arrive before the next request.
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 2 {
		t.Fatalf("%d connections, want 2: %v", len(remotes), remotes)
	}
}

func TestUnsupportedScheme(t *testing.T) {
	client := newClient(t, nil)
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, "http://localhost/", nil)).Wait(); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("got %v", err)
	}
}

func TestClientClose(t *testing.T) {
	url := startServer(t, Config{}, echo)
	client := newClient(t, nil)
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	client.Close()
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestDialFailure(t *testing.T) {
	client := newClient(t, func(config *ClientConfig) { config.HandshakeTimeout = 300 * time.Millisecond })
	// Nothing listens there, so the handshake cannot finish.
	_, err := client.Go(mustRequest(t, stdhttp.MethodGet, "https://127.0.0.1:1/", nil)).Wait()
	if err == nil {
		t.Fatal("request to nowhere succeeded")
	}
}
