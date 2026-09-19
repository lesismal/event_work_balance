//go:build linux || darwin || windows

// Command server is an HTTPS echo server: it answers each request with its
// method, path and body.
//
//	go run ./examples/http/tls/server
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
	fibtls "github.com/lesismal/fib/go/tls"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8443", "listen address")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	config := fib.DefaultConfig()
	config.Addr = *addr
	// The HTTP handler is the same one the plain server uses: fibtls.NewServer
	// decrypts in front of it and encrypts what it sends.
	engine, err := fib.Bind(config, fibtls.NewServer(tlsConfig, fibhttp.NewHandler(echo())))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("HTTPS echo server listening on https://%s", *addr))
}

func echo() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		reply := fmt.Sprintf("%s %s %s", r.Method, r.URL.Path, body)
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
			c.Conn.Close()
		}
	}
}
