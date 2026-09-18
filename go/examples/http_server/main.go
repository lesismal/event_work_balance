//go:build linux

package main

import (
	"fmt"
	stdhttp "net/http"

	epoll "github.com/lesismal/fib/go"
	epollhttp "github.com/lesismal/fib/go/http"
)

func main() {
	handler := epollhttp.NewHandler(epollhttp.HandlerFunc(func(c *epollhttp.Context, request *stdhttp.Request) {
		body := []byte(fmt.Sprintf("hello from %s\n", request.URL.Path))
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", body); err != nil {
			c.Conn.Close()
		}
	}))
	config := epoll.DefaultConfig()
	config.Addr = "127.0.0.1:8080"
	server, err := epoll.Bind(config, handler)
	if err != nil {
		panic(err)
	}
	defer server.Close()
	fmt.Println("HTTP server listening on http://127.0.0.1:8080")
	if err := server.Run(); err != nil {
		panic(err)
	}
}
