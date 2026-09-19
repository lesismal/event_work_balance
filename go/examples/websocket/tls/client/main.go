//go:build linux || darwin || windows

// Command client sends messages to the WebSocket echo server over TLS
// (wss://) and prints the echoes.
//
//	go run ./examples/websocket/tls/client
package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	"github.com/lesismal/fib/go/websocket"
)

func main() {
	url := flag.String("url", "wss://127.0.0.1:8444/ws", "server URL")
	count := flag.Int("n", 5, "messages to send")
	compress := flag.Bool("compress", false, "offer permessage-deflate compression")
	certFlags := certs.RegisterClientFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	engine, stop := example.ClientEngine()
	defer stop()
	config := websocket.DefaultDialerConfig()
	config.EnableCompression = *compress
	// Used for wss:// URLs; the server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	dialer := websocket.NewDialer(engine, config)

	messages := make(chan string, 64)
	closed := make(chan error, 1)
	handler := websocket.HandlerFuncs{
		Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) { messages <- string(data) },
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			closed <- fmt.Errorf("code=%d reason=%q error=%v", code, reason, err)
		},
	}
	conn, resp, err := dialer.Go(*url, nil, handler).Wait()
	if err != nil {
		example.Fatal(err)
	}
	fmt.Printf("connected: %s\n", resp.Status)

	for i := 1; i <= *count; i++ {
		if err := conn.WriteText(example.Message(i)); err != nil {
			example.Fatal(err)
		}
		select {
		case echo := <-messages:
			fmt.Printf("echo: %s\n", echo)
		case err := <-closed:
			example.Fatal(fmt.Errorf("connection closed: %v", err))
		case <-time.After(5 * time.Second):
			example.Fatal(errors.New("timed out waiting for the echo"))
		}
	}
	_ = conn.Close(websocket.CloseNormal, "done")
	select {
	case err := <-closed:
		fmt.Println("closed:", err)
	case <-time.After(3 * time.Second):
	}
}
