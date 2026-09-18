//go:build linux

package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	epoll "github.com/lesismal/fib/go"
)

func main() {
	config := epoll.DefaultConfig()
	config.Addr = "127.0.0.1:9000"
	config.Backlog = 256
	if len(os.Args) > 1 {
		config.Addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		enabled, _ := strconv.ParseBool(os.Args[2])
		config.UseWritev = enabled
	}
	server, err := epoll.Bind(config, epoll.HandlerFuncs{Data: func(c *epoll.Connection, data []byte) {
		if c.Send(data) != nil {
			c.Close()
		}
	}})
	if err != nil {
		panic(err)
	}
	defer server.Close()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-interrupt; server.Stop() }()
	address, _ := server.LocalAddr()
	fmt.Printf("echo server listening on %s\n", address)
	if err := server.Run(); err != nil {
		panic(err)
	}
}
