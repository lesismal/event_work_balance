//go:build linux || darwin || windows

// Command server is a TCP echo server.
//
//	go run ./examples/tcp/nontls/server
package main

import (
	"flag"
	"fmt"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/example"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	writev := flag.Bool("writev", true, "batch queued output with writev")
	flag.Parse()

	config := fib.DefaultConfig()
	config.Addr = *addr
	config.UseWritev = *writev
	engine, err := fib.Bind(config, fib.HandlerFuncs{
		// TCP is a byte stream: data is whatever arrived in one read, not
		// necessarily one of the client's writes. An echo does not care.
		Data: func(c *fib.Connection, data []byte) {
			if c.Send(data) != nil {
				c.Close()
			}
		},
	})
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("TCP echo server listening on %s", *addr))
}
