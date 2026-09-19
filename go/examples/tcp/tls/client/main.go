//go:build linux || darwin || windows

// Command client sends messages to the TCP+TLS echo server and prints the
// echoes.
//
//	go run ./examples/tcp/tls/client
package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "server address")
	count := flag.Int("n", 5, "messages to send")
	certFlags := certs.RegisterClientFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	engine, stop := example.ClientEngine()
	defer stop()

	chunks := make(chan []byte, 64)
	closed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		Data:  func(_ *fib.Connection, data []byte) { chunks <- append([]byte(nil), data...) },
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}
	dialed := make(chan error, 1)
	var conn *fib.Connection
	// done runs once TCP is connected; the TLS handshake follows, and what is
	// sent before it completes waits for it.
	err = engine.DialTLS("tcp", *addr, 3*time.Second, tlsConfig, handler, func(c *fib.Connection, err error) {
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
		if i == 1 {
			if state, ok := conn.TLSConnectionState(); ok {
				fmt.Printf("TLS %s, %s\n", tls.VersionName(state.Version), tls.CipherSuiteName(state.CipherSuite))
			}
		}
		fmt.Printf("echo: %s\n", echo)
	}
	conn.Close()
}
