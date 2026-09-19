//go:build linux || darwin || windows

// Command server is a WebSocket echo server. CI runs the Autobahn test suite
// against it.
//
//	go run ./examples/websocket/nontls/server
package main

import (
	"flag"
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/example"
	"github.com/lesismal/fib/go/websocket"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8081", "listen address")
	compress := flag.Bool("compress", false, "accept permessage-deflate compression")
	flag.Parse()

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, echo(*compress))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("WebSocket echo server listening on ws://%s", *addr))
}

func echo(compress bool) fib.Handler {
	config := websocket.DefaultConfig()
	config.EnableCompression = compress
	return websocket.NewHandlerWithConfig(config, websocket.HandlerFuncs{
		Open: func(_ *websocket.Connection, r *stdhttp.Request) {
			fmt.Printf("%s: open %s\n", r.RemoteAddr, r.URL.Path)
		},
		// A message arrives whole, reassembled from its frames.
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
