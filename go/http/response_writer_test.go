//go:build linux || darwin || windows

package http

import (
	"io"
	stdhttp "net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// recordingWriter keeps the reader io.Copy hands to ReadFrom.
type recordingWriter struct{ src io.Reader }

func (w *recordingWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *recordingWriter) ReadFrom(r io.Reader) (int64, error) {
	w.src = r
	return io.Copy(io.Discard, r)
}

// ReadFrom reaches the file behind every reader net/http and io.Copy pass
// it: the *os.File itself, the *io.LimitedReader http.ServeContent passes, and
// the wrapper os.File.WriteTo passes when it cannot send to the writer itself.
func TestSendableFileOf(t *testing.T) {
	path, data := randomFile(t, 5000)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, _ = f.Seek(100, io.SeekStart)
	file, ok := sendableFileOf(f)
	if !ok || file.offset != 100 || file.size != int64(len(data)-100) {
		t.Fatalf("*os.File: %v %+v", ok, file)
	}
	limited := &io.LimitedReader{R: f, N: 50}
	if file, ok = sendableFileOf(limited); !ok || file.size != 50 {
		t.Fatalf("*io.LimitedReader: %v %+v", ok, file)
	}
	file.advance()
	if pos, _ := f.Seek(0, io.SeekCurrent); pos != 150 || limited.N != 0 {
		t.Fatalf("advance left the file at %d, limit %d", pos, limited.N)
	}
	var w recordingWriter
	_, _ = f.Seek(0, io.SeekStart)
	if _, err = io.Copy(&w, f); err != nil {
		t.Fatal(err)
	}
	if _, ok = sendableFileOf(w.src); !ok {
		t.Fatalf("io.Copy from a file passed %T, which ReadFrom does not recognise", w.src)
	}
	if _, ok = sendableFileOf(strings.NewReader("x")); ok {
		t.Fatal("a strings.Reader taken for a file")
	}
}

// On HTTP/2 the ResponseWriter methods hold the response and send it whole,
// trailers included, and files are served through them too.
func TestResponseWriterOverHTTP2(t *testing.T) {
	path, data := randomFile(t, 300000)
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/file":
			stdhttp.ServeFile(c, r, path)
		case "/whole":
			_ = c.WriteResponse(Response{StatusCode: 200, Body: []byte("whole"),
				Trailer: stdhttp.Header{"X-Whole": {"w"}, "Host": {"forbidden"}}})
		case "/empty":
			_ = c.WriteResponse(Response{StatusCode: 200, Trailer: stdhttp.Header{"X-Empty": {"e"}}})
		default:
			c.Header().Set("Trailer", "X-Sum")
			for i := 0; i < 10; i++ {
				_, _ = c.WriteString(strings.Repeat("h", 10000))
				c.Flush()
			}
			c.Header().Set("X-Sum", "done")
			c.Header().Set(stdhttp.TrailerPrefix+"X-Late", "late")
		}
	}))))
	client := &stdhttp.Client{Timeout: 10 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: clientConfig, ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()
	get := func(path string) (*stdhttp.Response, string) {
		t.Helper()
		req, _ := stdhttp.NewRequest("GET", "https://"+addr+path, nil)
		resp, body := do(t, client, req)
		if resp.ProtoMajor != 2 {
			t.Fatalf("%s: %s", path, resp.Proto)
		}
		return resp, body
	}
	resp, body := get("/stream")
	if body != strings.Repeat("h", 100000) || resp.Trailer.Get("X-Sum") != "done" || resp.Trailer.Get("X-Late") != "late" {
		t.Fatalf("stream: %d bytes, trailer %v", len(body), resp.Trailer)
	}
	resp, body = get("/whole")
	if body != "whole" || resp.Trailer.Get("X-Whole") != "w" || resp.Trailer.Get("Host") != "" {
		t.Fatalf("whole: %q, trailer %v", body, resp.Trailer)
	}
	resp, body = get("/empty")
	if body != "" || resp.Trailer.Get("X-Empty") != "e" {
		t.Fatalf("empty: %q, trailer %v", body, resp.Trailer)
	}
	if _, body = get("/file"); body != string(data) {
		t.Fatalf("file: %d bytes differ", len(body))
	}
	// Several streams with trailers, so their blocks share HPACK's table.
	for i := 0; i < 3; i++ {
		if resp, _ = get("/stream"); resp.Trailer.Get("X-Sum") != "done" {
			t.Fatalf("stream %d: trailer %v", i, resp.Trailer)
		}
	}
}
