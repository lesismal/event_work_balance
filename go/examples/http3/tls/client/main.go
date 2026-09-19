//go:build linux || darwin || windows

// Command client posts messages to the HTTP/3 echo server and prints the
// replies.
//
//	go run ./examples/http3/tls/client
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	"github.com/lesismal/fib/go/http3"
)

func main() {
	url := flag.String("url", "https://127.0.0.1:8445/echo", "server URL")
	count := flag.Int("n", 5, "requests to send")
	certFlags := certs.RegisterClientFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	// The client dials UDP through an engine of any kind; this one has no
	// listener of its own.
	engine, stop := example.ClientEngine()
	defer stop()
	config := http3.DefaultClientConfig()
	// The server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	client := http3.NewClient(engine, config)
	defer client.Close()

	for i := 1; i <= *count; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, *url, strings.NewReader(example.Message(i)))
		if err != nil {
			example.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			example.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s %s: %s\n", resp.Proto, resp.Status, body)
	}
}
