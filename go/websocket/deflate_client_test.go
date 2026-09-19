//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/base64"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"
)

func compressionConfig() Config {
	config := DefaultConfig()
	config.EnableCompression = true
	return config
}

func TestCompressedEchoThroughDialer(t *testing.T) {
	servers := map[string]HandlerFuncs{
		// Without an Open callback the server takes its minimal handshake path.
		"minimal handshake": echoServerHandler(),
		"parsed handshake": func() HandlerFuncs {
			h := echoServerHandler()
			h.Open = func(*Connection, *stdhttp.Request) {}
			return h
		}(),
	}
	for name, handler := range servers {
		t.Run(name, func(t *testing.T) {
			url := startWebSocketServer(t, compressionConfig(), handler)
			dialerConfig := DefaultDialerConfig()
			dialerConfig.EnableCompression = true
			recorder := newClientRecorder()
			conn, resp, err := NewDialer(startClientEngine(t), dialerConfig).Go(url, nil, recorder.handler()).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := resp.Header.Get("Sec-WebSocket-Extensions"); got !=
				"permessage-deflate; server_no_context_takeover; client_no_context_takeover" {
				t.Fatalf("Sec-WebSocket-Extensions = %q", got)
			}
			if !conn.compress {
				t.Fatal("connection is not compressing")
			}
			for _, size := range []int{0, 1, 125, 4096, 70000} {
				payload := compressiblePayload(size)
				if err := conn.WriteMessage(Text, payload); err != nil {
					t.Fatal(err)
				}
				if event := recorder.next(t); event.Opcode != Text || !bytes.Equal(event.Payload, payload) {
					t.Fatalf("size %d: echo opcode=%v len=%d", size, event.Opcode, len(event.Payload))
				}
			}
		})
	}
}

func TestDialerCompressionDeclined(t *testing.T) {
	url := startWebSocketServer(t, DefaultConfig(), echoServerHandler())
	dialerConfig := DefaultDialerConfig()
	dialerConfig.EnableCompression = true
	recorder := newClientRecorder()
	conn, resp, err := NewDialer(startClientEngine(t), dialerConfig).Go(url, nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Values("Sec-WebSocket-Extensions"); len(got) != 0 || conn.compress {
		t.Fatalf("extensions %q, compress=%v", got, conn.compress)
	}
	if err := conn.WriteText("plain"); err != nil {
		t.Fatal(err)
	}
	if event := recorder.next(t); string(event.Payload) != "plain" {
		t.Fatalf("echo = %q", event.Payload)
	}
}

// TestServerHonorsWindowOffer checks on the wire that the server accepts a
// client's window limit and marks what it sends as compressed.
func TestServerHonorsWindowOffer(t *testing.T) {
	url := startWebSocketServer(t, compressionConfig(), echoServerHandler())
	conn, err := net.DialTimeout("tcp4", url[len("ws://"):len(url)-len("/ws")], 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /ws HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=9; client_max_window_bits\r\n\r\n"
	if _, err = io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Header.Get("Sec-WebSocket-Extensions"); got !=
		"permessage-deflate; server_no_context_takeover; server_max_window_bits=9" {
		t.Fatalf("Sec-WebSocket-Extensions = %q", got)
	}
	payload := compressiblePayload(3000)
	// The client keeps its context, so the second message refers to the first.
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestCompression)
	for i := 0; i < 2; i++ {
		buf.Reset()
		_, _ = w.Write(payload)
		_ = w.Flush()
		frame := deflateFrame(Binary, true, true, bytes.TrimSuffix(buf.Bytes(), []byte{0, 0, 0xff, 0xff}))
		if _, err = conn.Write(frame); err != nil {
			t.Fatal(err)
		}
		header, err := reader.Peek(1)
		if err != nil {
			t.Fatal(err)
		}
		if header[0]&0x40 == 0 {
			t.Fatal("echo is not marked compressed")
		}
		opcode, echoed, err := readServerFrame(reader)
		if err != nil || opcode != Binary {
			t.Fatalf("opcode=%v err=%v", opcode, err)
		}
		got, err := decompressMessage(nil, echoed, nil, 1<<20)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("echo does not decompress to the payload: %v", err)
		}
	}
}
