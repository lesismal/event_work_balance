//go:build linux || darwin || windows

// Command client posts messages to the HTTPS echo server and prints the
// replies.
//
//	go run ./examples/http/tls/client
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
)

func main() {
	url := flag.String("url", "https://127.0.0.1:8443/echo", "server URL")
	count := flag.Int("n", 5, "requests to send")
	certFlags := certs.RegisterClientFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	engine, stop := example.ClientEngine()
	defer stop()
	config := fibhttp.DefaultClientConfig()
	// Used for https:// URLs; the server name comes from the URL's host.
	config.TLSConfig = tlsConfig
	client := fibhttp.NewClient(engine, config)
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
		fmt.Printf("%s: %s\n", resp.Status, body)
	}
}
