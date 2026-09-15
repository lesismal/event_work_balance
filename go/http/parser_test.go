package http

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestParserFragmentedAndPipelinedRequests(t *testing.T) {
	parser := NewParser(DefaultConfig())
	first, err := parser.Feed([]byte("POST /one?x=1 HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\nhe"))
	if err != nil || len(first) != 0 {
		t.Fatalf("first Feed = %d requests, %v", len(first), err)
	}
	requests, err := parser.Feed([]byte("lloGET /two HTTP/1.1\r\nHost: example.test\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("got %d requests, want 2", len(requests))
	}
	body, err := io.ReadAll(requests[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if requests[0].URL.Path != "/one" || string(body) != "hello" {
		t.Fatalf("first request path/body = %q/%q", requests[0].URL.Path, body)
	}
	if requests[1].URL.Path != "/two" {
		t.Fatalf("second request path = %q", requests[1].URL.Path)
	}
}

func TestParserChunkedRequestWithTrailer(t *testing.T) {
	parser := NewParser(DefaultConfig())
	raw := "POST /chunks HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n" +
		"4\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Checksum: ok\r\n\r\n"
	requests, err := parser.Feed([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("got %d requests, want 1", len(requests))
	}
	body, err := io.ReadAll(requests[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "Wikipedia" || requests[0].Trailer.Get("X-Checksum") != "ok" {
		t.Fatalf("body/trailer = %q/%q", body, requests[0].Trailer.Get("X-Checksum"))
	}
}

func TestParserLimitsAndMalformedInput(t *testing.T) {
	t.Run("header", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 32, MaxBodyBytes: 100})
		_, err := parser.Feed([]byte("GET / HTTP/1.1\r\nX-Long: " + strings.Repeat("x", 40)))
		if !errors.Is(err, ErrHeaderTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("body", func(t *testing.T) {
		parser := NewParser(Config{MaxHeaderBytes: 1024, MaxBodyBytes: 3})
		_, err := parser.Feed([]byte("POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\ntest"))
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("chunk", func(t *testing.T) {
		parser := NewParser(DefaultConfig())
		_, err := parser.Feed([]byte("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nnope\r\n"))
		if !errors.Is(err, ErrMalformed) {
			t.Fatalf("error = %v", err)
		}
	})
}
