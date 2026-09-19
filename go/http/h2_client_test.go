//go:build linux || darwin || windows

package http

import (
	"context"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// newH2TestServer starts net/http's own HTTP/2 server over TLS.
func newH2TestServer(t *testing.T, handler stdhttp.HandlerFunc) (*httptest.Server, *connCounter, *stdtls.Config) {
	t.Helper()
	counter := &connCounter{}
	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.Config.ConnState = counter.hook
	ts.StartTLS()
	t.Cleanup(ts.Close)
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	return ts, counter, &stdtls.Config{RootCAs: pool}
}

func TestClientHTTP2MultiplexesOnOneConnection(t *testing.T) {
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Proto", r.Proto)
		fmt.Fprintf(w, "%s %s %s", r.Method, r.URL.RequestURI(), body)
	})
	config := DefaultClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := strings.Repeat("b", i)
			resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, fmt.Sprintf("%s/p?i=%d", ts.URL, i), strings.NewReader(body))).Wait()
			if err != nil {
				t.Error(err)
				return
			}
			got := readBody(t, resp)
			if resp.ProtoMajor != 2 || resp.Header.Get("X-Proto") != "HTTP/2.0" {
				t.Errorf("proto %s / %s", resp.Proto, resp.Header.Get("X-Proto"))
			}
			if want := fmt.Sprintf("POST /p?i=%d %s", i, body); got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		}(i)
	}
	wg.Wait()
	if got := counter.n.Load(); got != 1 {
		t.Fatalf("%d connections, want 1", got)
	}
}

func TestClientHTTP2LargeBodiesBothWays(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if size, err := strconv.Atoi(r.URL.Query().Get("size")); err == nil {
			_, _ = w.Write([]byte(strings.Repeat("d", size)))
			return
		}
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%d", len(body))
	})
	config := DefaultClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	upload := strings.Repeat("u", 5<<20)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodPut, ts.URL+"/up", strings.NewReader(upload))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != strconv.Itoa(len(upload)) {
		t.Fatalf("server read %s bytes", got)
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, fmt.Sprintf("%s/down?size=%d", ts.URL, 5<<20), nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); len(got) != 5<<20 {
		t.Fatalf("downloaded %d bytes", len(got))
	}
}

func TestClientHTTP2CancelKeepsConnection(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/slow" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = io.WriteString(w, "fast")
	})
	config := DefaultClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	ctx, cancel := context.WithCancel(context.Background())
	req := mustRequest(t, stdhttp.MethodGet, ts.URL+"/slow", nil).WithContext(ctx)
	future := client.Go(req)
	time.Sleep(50 * time.Millisecond)
	cancel()
	if _, err := future.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, ts.URL+"/fast", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "fast" {
		t.Fatalf("body %q", got)
	}
	if got := counter.n.Load(); got != 1 {
		t.Fatalf("%d connections, want 1", got)
	}
}

func TestClientDisableHTTP2(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.WriteString(w, r.Proto)
	})
	config := DefaultClientConfig()
	config.TLSConfig = tlsConfig
	config.DisableHTTP2 = true
	client := newTestClient(t, config)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, ts.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "HTTP/1.1" || resp.ProtoMajor != 1 {
		t.Fatalf("spoke %q / %s", got, resp.Proto)
	}
}

// TestClientHTTP2ToFibServer runs both ends of HTTP/2 on fib, over TLS and
// over h2c.
func TestClientHTTP2ToFibServer(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	tlsAddr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(echoHandler())))
	plainAddr := serve(t, NewHandler(echoHandler()))

	config := DefaultClientConfig()
	config.TLSConfig = clientConfig
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	for _, url := range []string{"https://" + tlsAddr, "http://" + plainAddr} {
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, fmt.Sprintf("%s/x/%d", url, i), strings.NewReader("hi"))).Wait()
				if err != nil {
					t.Error(err)
					return
				}
				got := readBody(t, resp)
				if resp.ProtoMajor != 2 || got != fmt.Sprintf("POST /x/%d hi", i) {
					t.Errorf("%s: %s %q", url, resp.Proto, got)
				}
			}(i)
		}
		wg.Wait()
		// A large response exercises the server's flow control against the
		// client's windows.
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, fmt.Sprintf("%s/big?size=%d", url, 3<<20), nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); len(got) != 3<<20 {
			t.Fatalf("%s: body %d bytes", url, len(got))
		}
	}
}
