//go:build linux || darwin || windows

// Command server is a UDP echo server.
//
//	go run ./examples/udp/nontls/server
package main

import (
	"flag"
	"fmt"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/example"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "listen address")
	idle := flag.Duration("idle", time.Minute, "close a peer after this long without a datagram")
	flag.Parse()

	config := fib.DefaultConfig()
	config.Network = "udp"
	config.Addr = *addr
	config.UDPIdleTimeout = *idle
	engine, err := fib.Bind(config, fib.HandlerFuncs{
		// Every peer address is a connection of its own: OnOpen runs on its
		// first datagram and OnClose once it has been idle too long.
		Open: func(c *fib.Connection) { fmt.Printf("peer %s: open\n", c.RemoteAddr()) },
		// data is exactly one datagram, and Send sends exactly one.
		Data: func(c *fib.Connection, data []byte) {
			if err := c.Send(data); err != nil {
				fmt.Printf("peer %s: echo dropped: %v\n", c.RemoteAddr(), err)
			}
		},
		Close: func(c *fib.Connection, err error) { fmt.Printf("peer %s: closed: %v\n", c.RemoteAddr(), err) },
	})
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("UDP echo server listening on %s", *addr))
}
