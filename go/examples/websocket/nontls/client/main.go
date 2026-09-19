//go:build linux || darwin || windows

// Command client sends messages to the WebSocket echo server and prints the
// echoes.
//
//	go run ./examples/websocket/nontls/client
package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/lesismal/fib/go/examples/internal/example"
	"github.com/lesismal/fib/go/websocket"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:8081/ws", "server URL")
	count := flag.Int("n", 5, "messages to send")
	compress := flag.Bool("compress", false, "offer permessage-deflate compression")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()
	config := websocket.DefaultDialerConfig()
	config.EnableCompression = *compress
	dialer := websocket.NewDialer(engine, config)

	messages := make(chan string, 64)
	closed := make(chan error, 1)
	handler := websocket.HandlerFuncs{
		Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) { messages <- string(data) },
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			closed <- fmt.Errorf("code=%d reason=%q error=%v", code, reason, err)
		},
	}
	// Go dials and runs the opening handshake without blocking the engine;
	// Wait blocks this goroutine until it is done.
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
	// Close runs the closing handshake; OnClose reports its outcome.
	_ = conn.Close(websocket.CloseNormal, "done")
	select {
	case err := <-closed:
		fmt.Println("closed:", err)
	case <-time.After(3 * time.Second):
	}
}
