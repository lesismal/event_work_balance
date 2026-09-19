//go:build (linux || darwin || windows) && go1.24

package http

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestH2ServerCleartextWithNetHTTPClient serves h2c, HTTP/2 without TLS, to
// net/http's client speaking it with prior knowledge.
func TestH2ServerCleartextWithNetHTTPClient(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := client.Post(fmt.Sprintf("http://%s/h2c/%d", addr, i), "text/plain", strings.NewReader("body"))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 || string(got) != fmt.Sprintf("POST /h2c/%d body", i) {
				t.Errorf("%s %q", resp.Proto, got)
			}
		}(i)
	}
	wg.Wait()
}
