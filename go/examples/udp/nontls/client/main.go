//go:build linux || darwin || windows

// Command client sends datagrams to the UDP echo server and prints the
// echoes.
//
//	go run ./examples/udp/nontls/client
package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/example"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "server address")
	count := flag.Int("n", 5, "datagrams to send")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()

	datagrams := make(chan []byte, 64)
	closed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		Data:  func(_ *fib.Connection, data []byte) { datagrams <- append([]byte(nil), data...) },
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}
	dialed := make(chan error, 1)
	var conn *fib.Connection
	// A UDP dial connects the socket to the server, so the connection
	// exchanges datagrams with it alone.
	err := engine.DialWithHandler("udp", *addr, 3*time.Second, handler, func(c *fib.Connection, err error) {
		conn = c
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		example.Fatal(err)
	}

	for i := 1; i <= *count; i++ {
		if err := conn.Send([]byte(example.Message(i))); err != nil {
			example.Fatal(err)
		}
		// UDP keeps message boundaries, so one datagram is one whole echo.
		select {
		case echo := <-datagrams:
			fmt.Printf("echo: %s\n", echo)
		case err := <-closed:
			// Nothing listening at the server's port shows up here as a
			// refusal.
			example.Fatal(fmt.Errorf("connection closed: %v", err))
		case <-time.After(3 * time.Second):
			example.Fatal(errors.New("no echo: UDP does not retransmit, is the server running?"))
		}
	}
	conn.Close()
}
