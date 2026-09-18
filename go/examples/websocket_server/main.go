//go:build linux || darwin || windows

package main

import (
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/websocket"
)

func main() {
	handler := websocket.NewHandler(websocket.HandlerFuncs{
		Open: func(_ *websocket.Connection, request *stdhttp.Request) {
			fmt.Printf("WebSocket opened: %s\n", request.RemoteAddr)
		},
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if err := c.WriteMessage(opcode, data); err != nil {
				_ = c.Close(websocket.CloseInternalError, "write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			fmt.Printf("WebSocket closed: code=%d reason=%q error=%v\n", code, reason, err)
		},
	})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:8080"
	server, err := fib.Bind(config, handler)
	if err != nil {
		panic(err)
	}
	defer server.Close()
	fmt.Println("WebSocket echo server listening on ws://127.0.0.1:8080")
	if err := server.Run(); err != nil {
		panic(err)
	}
}
