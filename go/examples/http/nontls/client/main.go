//go:build linux || darwin || windows

// Command client posts messages to the HTTP echo server and prints the
// replies.
//
//	go run ./examples/http/nontls/client
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/echo", "server URL")
	count := flag.Int("n", 5, "requests to send")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()
	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	for i := 1; i <= *count; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, *url, strings.NewReader(example.Message(i)))
		if err != nil {
			example.Fatal(err)
		}
		// Go returns at once; Wait blocks this goroutine, not the engine.
		// Client.Do takes a callback instead, for callers that must not wait.
		// The connection is kept alive and reused by the next request.
		resp, err := client.Go(req).Wait()
		if err != nil {
			example.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s: %s\n", resp.Status, body)
	}
}
