//go:build linux || darwin || windows

// Command server is an HTTP/3 echo server: it answers each request with its
// protocol, method, path and body.
//
//	go run ./examples/http3/tls/server
//
// HTTP/3 runs over UDP. The same handler also serves HTTPS on the TCP port of
// the same number, whose responses carry Alt-Svc, which is how a browser
// learns that it can switch to HTTP/3.
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/http3"
	fibtls "github.com/lesismal/fib/go/tls"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8445", "listen address, UDP for HTTP/3 and TCP for HTTPS")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		example.Fatal(err)
	}
	portNum, _ := strconv.Atoi(port)
	handler := echo(http3.AltSvc(portNum))

	// HTTP/3: a UDP engine whose handler is http3's. The http.Handler is the
	// same kind the HTTP/1 and HTTP/2 servers take.
	udpConfig := fib.DefaultConfig()
	udpConfig.Network = "udp"
	udpConfig.Addr = *addr
	udp, err := fib.Bind(udpConfig, http3.NewHandler(tlsConfig, handler))
	if err != nil {
		example.Fatal(err)
	}
	go func() {
		if err := udp.Run(); err != nil {
			example.Fatal(err)
		}
	}()
	defer udp.Close()
	defer udp.Stop()

	// HTTPS over TCP, for clients that do not know yet that HTTP/3 is here.
	tcpConfig := fib.DefaultConfig()
	tcpConfig.Addr = *addr
	tcp, err := fib.Bind(tcpConfig, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(handler)))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(tcp, fmt.Sprintf("HTTP/3 echo server listening on https://%s (UDP), HTTPS on the same TCP port", *addr))
}

func echo(altSvc string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		reply := fmt.Sprintf("%s %s %s %s", r.Proto, r.Method, r.URL.Path, body)
		header := stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}}
		if r.ProtoMajor < 3 {
			header.Set("Alt-Svc", altSvc)
		}
		if err := c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header, Body: []byte(reply)}); err != nil {
			c.Conn.Close()
		}
	}
}
