package websocket

import (
	"testing"

	epollhttp "github.com/lesismal/fib/go/http"
)

var benchmarkHandshakeRequest = []byte("GET /chat?id=1 HTTP/1.1\r\nHost: example.test\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\nOrigin: https://example.test\r\n\r\n")

func TestHandshakeParserFragmentedWithRemainder(t *testing.T) {
	parser := handshakeParser{maxHeaderBytes: 1024}
	cut := len(benchmarkHandshakeRequest) / 2
	if request, complete, err := parser.Feed(benchmarkHandshakeRequest[:cut]); err != nil || complete || request != nil {
		t.Fatalf("first Feed = %#v, %v, %v", request, complete, err)
	}
	request, complete, err := parser.Feed(append(benchmarkHandshakeRequest[cut:], 0x81, 0x80))
	if err != nil || !complete {
		t.Fatalf("second Feed complete=%v, err=%v", complete, err)
	}
	if request.Method != "GET" || request.Host != "example.test" || request.URL.RequestURI() != "/chat?id=1" {
		t.Fatalf("unexpected request: %#v", request)
	}
	if request.Header.Get("Sec-WebSocket-Key") != "MDEyMzQ1Njc4OWFiY2RlZg==" ||
		request.Header.Get("Sec-WebSocket-Version") != "13" {
		t.Fatalf("websocket headers were not canonicalized: %#v", request.Header)
	}
	if got := parser.TakeBuffered(); len(got) != 2 || got[0] != 0x81 || got[1] != 0x80 {
		t.Fatalf("remainder = %x", got)
	}
}

func TestHandshakeParserRejectsBodyAndFoldedHeader(t *testing.T) {
	for _, request := range [][]byte{
		[]byte("GET / HTTP/1.1\r\nHost: test\r\nContent-Length: 1\r\n\r\n"),
		[]byte("GET / HTTP/1.1\r\nHost: test\r\n folded\r\n\r\n"),
		[]byte("GET / HTTP/1.1\r\nHost: test\r\nOrigin: \vbad\r\n\r\n"),
	} {
		parser := handshakeParser{maxHeaderBytes: 1024}
		if _, _, err := parser.Feed(request); err == nil {
			t.Fatalf("accepted malformed request %q", request)
		}
	}
}

func BenchmarkHandshakeParser(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkHandshakeRequest)))
	for i := 0; i < b.N; i++ {
		parser := handshakeParser{maxHeaderBytes: 1024}
		request, complete, err := parser.Feed(benchmarkHandshakeRequest)
		if err != nil || !complete || request.Host == "" {
			b.Fatal(err)
		}
	}
}

func BenchmarkStandardHTTPHandshakeParser(b *testing.B) {
	parser := epollhttp.NewParser(epollhttp.DefaultConfig())
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkHandshakeRequest)))
	for i := 0; i < b.N; i++ {
		request, complete, err := parser.FeedOne(benchmarkHandshakeRequest)
		if err != nil || !complete || request.Host == "" {
			b.Fatal(err)
		}
	}
}
