//go:build linux || darwin || windows

// Command server is an HTTP echo server: it answers each request with its
// method, path and body.
//
//	go run ./examples/http/nontls/server
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	flag.Parse()

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, fibhttp.NewHandler(echo()))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("HTTP echo server listening on http://%s", *addr))
}

func echo() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *stdhttp.Request) {
		// The body has already been read off the connection whole, so
		// reading it here never waits on the network.
		body, _ := io.ReadAll(r.Body)
		reply := fmt.Sprintf("%s %s %s", r.Method, r.URL.Path, body)
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
			c.Conn.Close()
		}
	}
}
