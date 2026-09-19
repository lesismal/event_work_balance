//go:build linux || darwin || windows

// Command server is a WebSocket echo server over TLS (wss://).
//
//	go run ./examples/websocket/tls/server
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
package main

import (
	"flag"
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibtls "github.com/lesismal/fib/go/tls"
	"github.com/lesismal/fib/go/websocket"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8444", "listen address")
	compress := flag.Bool("compress", false, "accept permessage-deflate compression")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	config := fib.DefaultConfig()
	config.Addr = *addr
	// The WebSocket handler is the same one the plain server uses:
	// fibtls.NewServer decrypts in front of it and encrypts what it sends.
	engine, err := fib.Bind(config, fibtls.NewServer(tlsConfig, echo(*compress)))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("WebSocket echo server listening on wss://%s", *addr))
}

func echo(compress bool) fib.Handler {
	config := websocket.DefaultConfig()
	config.EnableCompression = compress
	return websocket.NewHandlerWithConfig(config, websocket.HandlerFuncs{
		Open: func(_ *websocket.Connection, r *stdhttp.Request) {
			fmt.Printf("%s: open %s\n", r.RemoteAddr, r.URL.Path)
		},
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if err := c.WriteMessage(opcode, data); err != nil {
				_ = c.Close(websocket.CloseInternalError, "write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			fmt.Printf("closed: code=%d reason=%q error=%v\n", code, reason, err)
		},
	})
}
