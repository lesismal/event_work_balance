//go:build linux

package http

import (
	"bufio"
	"bytes"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
)

func TestServerHandlerKeepAliveAndClose(t *testing.T) {
	handler := NewHandler(HandlerFunc(func(c *Context, request *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(request.URL.Path))
	}))
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
	if _, err = io.WriteString(conn, "GET /one HTTP/1.1\r\nHost: test\r\n\r\nGET /two HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	for _, path := range []string{"/one", "/two"} {
		request := &stdhttp.Request{Method: stdhttp.MethodGet}
		response, err := stdhttp.ReadResponse(reader, request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if string(body) != path {
			t.Fatalf("body = %q, want %q", body, path)
		}
	}
}

func TestMarshalHeadResponse(t *testing.T) {
	request := &stdhttp.Request{Method: stdhttp.MethodHead, ProtoMajor: 1, ProtoMinor: 1}
	data, err := marshalResponse(request, Response{StatusCode: 200, Body: []byte("hello")}, false)
	if err != nil {
		t.Fatal(err)
	}
	response, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(data)), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.ContentLength != 5 {
		t.Fatalf("Content-Length = %d", response.ContentLength)
	}
	body, _ := io.ReadAll(response.Body)
	if len(body) != 0 {
		t.Fatalf("HEAD body = %q", body)
	}
}
