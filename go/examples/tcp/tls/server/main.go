//go:build linux || darwin || windows

// Command server is a TCP echo server over TLS.
//
//	go run ./examples/tcp/tls/server
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"flag"
	"fmt"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibtls "github.com/lesismal/fib/go/tls"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "listen address")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	echo := fib.HandlerFuncs{
		// The handler is the same as without TLS: fibtls.NewServer hands it
		// plaintext, and its Send encrypts.
		Data: func(c *fib.Connection, data []byte) {
			if c.Send(data) != nil {
				c.Close()
			}
		},
	}
	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, fibtls.NewServer(tlsConfig, echo))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("TCP+TLS echo server listening on %s", *addr))
}
