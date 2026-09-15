package http

import "testing"

func BenchmarkParserKeepAliveGET(b *testing.B) {
	parser := NewParser(DefaultConfig())
	request := []byte("GET /events?id=42 HTTP/1.1\r\nHost: example.test\r\nUser-Agent: benchmark\r\nAccept: */*\r\n\r\n")
	b.ReportAllocs()
	b.SetBytes(int64(len(request)))
	for i := 0; i < b.N; i++ {
		requests, err := parser.Feed(request)
		if err != nil || len(requests) != 1 {
			b.Fatalf("Feed returned %d requests, %v", len(requests), err)
		}
	}
}

func BenchmarkParserContentLength(b *testing.B) {
	parser := NewParser(DefaultConfig())
	request := []byte("POST /events HTTP/1.1\r\nHost: example.test\r\nContent-Length: 16\r\n\r\n0123456789abcdef")
	b.ReportAllocs()
	b.SetBytes(int64(len(request)))
	for i := 0; i < b.N; i++ {
		requests, err := parser.Feed(request)
		if err != nil || len(requests) != 1 {
			b.Fatalf("Feed returned %d requests, %v", len(requests), err)
		}
	}
}
