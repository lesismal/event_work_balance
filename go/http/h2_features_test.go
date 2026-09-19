//go:build linux || darwin || windows

package http

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// pushHandler pushes /style.css and /app.js ahead of /index.html, and
// records what each Push returned.
type pushHandler struct {
	mu   sync.Mutex
	errs []error
}

func (h *pushHandler) ServeHTTP(c *Context, r *stdhttp.Request) {
	switch r.URL.Path {
	case "/index.html":
		for _, target := range []string{"/style.css", "/app.js"} {
			err := c.Push(target, &stdhttp.PushOptions{Header: stdhttp.Header{"Accept": {"*/*"}}})
			h.mu.Lock()
			h.errs = append(h.errs, err)
			h.mu.Unlock()
		}
		_ = c.Respond(200, "text/html", []byte("<html>"))
	case "/style.css", "/app.js":
		// A pushed request cannot push in turn.
		if err := c.Push("/nested", nil); !errors.Is(err, stdhttp.ErrNotSupported) {
			_ = c.Respond(500, "", []byte(fmt.Sprint("nested push: ", err)))
			return
		}
		fallthrough
	default:
		_ = c.Respond(200, "text/plain", []byte("pushed "+r.Method+" "+r.URL.Path+" "+r.Header.Get("Accept")))
	}
}

func (h *pushHandler) results() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.errs...)
}

func TestH2ServerPush(t *testing.T) {
	handler := &pushHandler{}
	addr := serve(t, NewHandler(handler))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/index.html")...)
	header, body := tc.response(1)
	if header[":status"] != "200" || string(body) != "<html>" {
		t.Fatalf("index: %v %q", header, body)
	}
	parent := tc.stream(1)
	if len(parent.promises) != 2 || parent.promises[0] != 2 || parent.promises[1] != 4 {
		t.Fatalf("promises %v", parent.promises)
	}
	for i, path := range []string{"/style.css", "/app.js"} {
		id := parent.promises[i]
		promised := tc.stream(id).promised
		if promised[":method"] != "GET" || promised[":path"] != path || promised[":scheme"] != "http" ||
			promised[":authority"] != "test" || promised["accept"] != "*/*" {
			t.Fatalf("promise %d: %v", id, promised)
		}
		header, body := tc.response(id)
		if want := "pushed GET " + path + " */*"; header[":status"] != "200" || string(body) != want {
			t.Fatalf("pushed %d: %v %q", id, header, body)
		}
	}
	for _, err := range handler.results() {
		if err != nil {
			t.Fatal(err)
		}
	}

	// The client may cancel a push, which leaves the connection alone.
	tc.headers(3, true, get("/index.html")...)
	tc.readUntil(h2FramePushPromise)
	tc.write(h2AppendRSTStream(nil, 6, H2Cancel))
	if _, body := tc.response(3); string(body) != "<html>" {
		t.Fatalf("body %q", body)
	}
	tc.headers(5, true, get("/after")...)
	if _, body := tc.response(5); !strings.HasPrefix(string(body), "pushed GET /after") {
		t.Fatalf("body %q", body)
	}
}

func TestH2ServerPushRefusedByClientSettings(t *testing.T) {
	for _, tt := range []struct {
		name    string
		setting [2]uint32
		want    error
	}{
		{"push disabled", [2]uint32{uint32(h2SettingEnablePush), 0}, stdhttp.ErrNotSupported},
		{"no pushed streams allowed", [2]uint32{uint32(h2SettingMaxConcurrentStreams), 0}, ErrPushLimit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handler := &pushHandler{}
			addr := serve(t, NewHandler(handler))
			tc := dialH2(t, addr, tt.setting)
			tc.headers(1, true, get("/index.html")...)
			if _, body := tc.response(1); string(body) != "<html>" {
				t.Fatalf("body %q", body)
			}
			if len(tc.stream(1).promises) != 0 {
				t.Fatalf("promised %v", tc.stream(1).promises)
			}
			for _, err := range handler.results() {
				if !errors.Is(err, tt.want) {
					t.Fatalf("Push = %v, want %v", err, tt.want)
				}
			}
		})
	}
}

func TestPushOnHTTP1IsNotSupported(t *testing.T) {
	handler := &pushHandler{}
	addr := serve(t, NewHandler(handler))
	resp, err := stdhttp.Get("http://" + addr + "/index.html")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, err := range handler.results() {
		if !errors.Is(err, stdhttp.ErrNotSupported) {
			t.Fatalf("Push = %v", err)
		}
	}
}

// earlyHints answers with 103 Early Hints before its response.
func earlyHints() Handler {
	return HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if err := c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"</style.css>; rel=preload"}}); err != nil {
			_ = c.Respond(500, "", []byte(err.Error()))
			return
		}
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(200, "text/plain", body)
	})
}

// TestInterimResponsesAndContinue sends through net/http's client, over
// HTTP/1.1 and HTTP/2: 103 Early Hints reach it ahead of the response, and a
// request expecting 100-continue gets its 100 at once instead of waiting out
// the client's ExpectContinueTimeout.
func TestInterimResponsesAndContinue(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	plain := serve(t, NewHandler(earlyHints()))
	secure := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(earlyHints())))
	for _, tt := range []struct{ url, proto string }{
		{"http://" + plain, "HTTP/1.1"},
		{"https://" + secure, "HTTP/2.0"},
	} {
		t.Run(tt.proto, func(t *testing.T) {
			client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{
				TLSClientConfig: clientConfig, ForceAttemptHTTP2: true, ExpectContinueTimeout: 5 * time.Second,
			}}
			defer client.CloseIdleConnections()
			var mu sync.Mutex
			var interim []string
			trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
				mu.Lock()
				defer mu.Unlock()
				interim = append(interim, fmt.Sprintf("%d %s", code, header.Get("Link")))
				return nil
			}}
			req, _ := stdhttp.NewRequest(stdhttp.MethodPost, tt.url+"/", strings.NewReader("payload"))
			req.Header.Set("Expect", "100-continue")
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
			start := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("took %v: 100 Continue was not sent", elapsed)
			}
			if resp.Proto != tt.proto || string(body) != "payload" {
				t.Fatalf("%s %q", resp.Proto, body)
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.Join(interim, ",") != "100 ,103 </style.css>; rel=preload" {
				t.Fatalf("interim responses %q", interim)
			}
		})
	}
}

func TestInterimResponseRules(t *testing.T) {
	results := make(chan []error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		var errs []error
		errs = append(errs, c.WriteInterim(stdhttp.StatusSwitchingProtocols, nil))
		// HTTP/1.0 knows nothing of interim responses.
		errs = append(errs, c.WriteInterim(stdhttp.StatusEarlyHints, nil))
		_ = c.Respond(200, "", nil)
		errs = append(errs, c.WriteInterim(stdhttp.StatusEarlyHints, nil))
		results <- errs
	})))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.WriteString(conn, "GET / HTTP/1.0\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	errs := <-results
	if errs[0] == nil || !errors.Is(errs[1], stdhttp.ErrNotSupported) || errs[2] == nil {
		t.Fatalf("WriteInterim errors %v", errs)
	}
}

// TestH2CUpgrade switches an HTTP/1.1 connection to HTTP/2 with Upgrade:
// h2c, as curl --http2 does for http:// URLs.
func TestH2CUpgrade(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	settings := h2AppendSettings(nil, [2]uint32{uint32(h2SettingInitialWindowSize), 1 << 20})[h2FrameHeaderLen:]
	request := "POST /upgrade HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\n" +
		"HTTP2-Settings: " + base64.RawURLEncoding.EncodeToString(settings) + "\r\n\r\nbody"
	tc := newH2TestConn(t, c)
	tc.write([]byte(request))
	const switching = "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n"
	head := make([]byte, len(switching))
	if _, err = io.ReadFull(c, head); err != nil || string(head) != switching {
		t.Fatalf("got %q, %v", head, err)
	}
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	header, body := tc.response(1)
	if header[":status"] != "200" || header["x-proto"] != "HTTP/2.0" || string(body) != "POST /upgrade body" {
		t.Fatalf("stream 1: %v %q", header, body)
	}
	// The connection carries on as HTTP/2.
	tc.headers(3, true, get("/next")...)
	if _, body := tc.response(3); string(body) != "GET /next " {
		t.Fatalf("stream 3: %q", body)
	}
}

func TestH2CUpgradeIgnoredWhenDisabled(t *testing.T) {
	config := DefaultConfig()
	config.DisableHTTP2 = true
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://"+addr+"/plain", nil)
	req.Header.Set("Connection", "Upgrade, HTTP2-Settings")
	req.Header.Set("Upgrade", "h2c")
	req.Header.Set("HTTP2-Settings", "")
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.ProtoMajor != 1 {
		t.Fatalf("%d %s", resp.StatusCode, resp.Proto)
	}
}

// TestH2ServerResponseCloseGoesAwayGracefully checks that Response.Close on
// HTTP/2 sends GOAWAY, lets the open streams finish, and then closes.
func TestH2ServerResponseCloseGoesAwayGracefully(t *testing.T) {
	release := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/slow":
			go func() {
				<-release
				_ = c.Respond(200, "", []byte("slow"))
			}()
		case "/close":
			_ = c.WriteResponse(Response{StatusCode: 200, Body: []byte("bye"), Close: true})
		default:
			_ = c.Respond(200, "", []byte("other"))
		}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/slow")...)
	tc.headers(3, true, get("/close")...)
	f := tc.readUntil(h2FrameGoAway)
	if last, code := binary.BigEndian.Uint32(f.payload), H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); last != 3 || code != H2NoError {
		t.Fatalf("GOAWAY last %d code %v", last, code)
	}
	// A stream opened after GOAWAY is not served.
	tc.headers(5, true, get("/late")...)
	close(release)
	if _, body := tc.response(1); string(body) != "slow" {
		t.Fatalf("body %q", body)
	}
	if st := tc.stream(5); st.header != nil {
		t.Fatalf("stream 5 answered: %v", st.header)
	}
	// Then the connection closes: EOF, or a reset if the late stream was
	// still unread when it did.
	for {
		if _, err := tc.c.Read(make([]byte, 64)); err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				t.Fatal("connection still open")
			}
			break
		}
	}
}

// TestRequestTLS checks that requests over TLS carry the connection state,
// over both protocols.
func TestRequestTLS(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		proto := "none"
		if r.TLS != nil {
			proto = r.TLS.NegotiatedProtocol
		}
		_ = c.Respond(200, "", []byte(proto))
	}))))
	for _, protos := range [][]string{{"h2"}, {"http/1.1"}} {
		config := clientConfig.Clone()
		config.NextProtos = protos
		client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{TLSClientConfig: config, ForceAttemptHTTP2: protos[0] == "h2"}}
		resp, err := client.Get("https://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		client.CloseIdleConnections()
		if string(body) != protos[0] {
			t.Fatalf("TLS.NegotiatedProtocol %q, want %q", body, protos[0])
		}
	}
	// And a cleartext request carries none.
	plain := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(200, "", []byte(fmt.Sprint(r.TLS != nil)))
	})))
	resp, err := stdhttp.Get("http://" + plain + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "false" {
		t.Fatalf("cleartext request has TLS")
	}
}

// TestH2ServerCleartextWithNetHTTPClient serves h2c, HTTP/2 without TLS, to
// net/http's client speaking it with prior knowledge.
func TestH2ServerCleartextWithNetHTTPClient(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			resp, err := client.Post(fmt.Sprintf("http://%s/h2c/%d", addr, i), "text/plain", strings.NewReader("body"))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 || string(got) != fmt.Sprintf("POST /h2c/%d body", i) {
				t.Errorf("%s %q", resp.Proto, got)
			}
		})
	}
	wg.Wait()
}

// TestClientUnencryptedHTTP2ToNetHTTPServer runs the client's h2c against
// net/http's own h2c server.
func TestClientUnencryptedHTTP2ToNetHTTPServer(t *testing.T) {
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	ts.Config.Protocols = new(stdhttp.Protocols)
	ts.Config.Protocols.SetHTTP1(true)
	ts.Config.Protocols.SetUnencryptedHTTP2(true)
	ts.Start()
	t.Cleanup(ts.Close)
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, ts.URL+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "HTTP/2.0" || resp.ProtoMajor != 2 {
		t.Fatalf("spoke %q / %s", got, resp.Proto)
	}
}
