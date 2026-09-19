//go:build linux || darwin || windows

// Command client sends messages to the TCP echo server and prints the echoes.
//
//	go run ./examples/tcp/nontls/client
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
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	count := flag.Int("n", 5, "messages to send")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()

	chunks := make(chan []byte, 64)
	closed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		// data is only valid during the call, so it is copied before it
		// leaves the handler.
		Data:  func(_ *fib.Connection, data []byte) { chunks <- append([]byte(nil), data...) },
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}
	dialed := make(chan error, 1)
	var conn *fib.Connection
	err := engine.DialWithHandler("tcp", *addr, 3*time.Second, handler, func(c *fib.Connection, err error) {
		conn = c
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		example.Fatal(err)
	}
	fmt.Printf("connected to %s\n", conn.RemoteAddr())

	for i := 1; i <= *count; i++ {
		msg := example.Message(i)
		if err := conn.Send([]byte(msg)); err != nil {
			example.Fatal(err)
		}
		// A stream may deliver the echo in several pieces.
		var echo []byte
		for len(echo) < len(msg) {
			select {
			case chunk := <-chunks:
				echo = append(echo, chunk...)
			case err := <-closed:
				example.Fatal(fmt.Errorf("connection closed: %v", err))
			case <-time.After(5 * time.Second):
				example.Fatal(errors.New("timed out waiting for the echo"))
			}
		}
		fmt.Printf("echo: %s\n", echo)
	}
	conn.Close()
}
