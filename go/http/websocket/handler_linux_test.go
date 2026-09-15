//go:build linux

package websocket

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	epoll "github.com/lesismal/auto-balance-epoll/go"
)

func TestServerHandshakeEchoPingAndClose(t *testing.T) {
	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			if err := c.WriteMessage(opcode, payload); err != nil {
				t.Error(err)
			}
		},
	})
	config := epoll.DefaultConfig()
	config.BindAddress = "127.0.0.1"
	config.Port = 0
	server, err := epoll.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()

	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	request := "GET /chat HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
	if _, err = io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha1.Sum([]byte(key + websocketGUID))
	wantAccept := base64.StdEncoding.EncodeToString(sum[:])
	if response.StatusCode != stdhttp.StatusSwitchingProtocols ||
		response.Header.Get("Sec-WebSocket-Accept") != wantAccept {
		t.Fatalf("invalid handshake: status=%d accept=%q", response.StatusCode, response.Header.Get("Sec-WebSocket-Accept"))
	}

	frames := append(clientFrame(Text, true, []byte("hello")), clientFrame(Ping, true, []byte("p"))...)
	frames = append(frames, clientFrame(Close, true, []byte{0x03, 0xe8})...)
	if _, err = conn.Write(frames); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []Opcode{Text, Pong, Close} {
		opcode, _, err := readServerFrame(reader)
		if err != nil {
			t.Fatal(err)
		}
		if opcode != expected {
			t.Fatalf("opcode = %d, want %d", opcode, expected)
		}
	}
}

func readServerFrame(reader *bufio.Reader) (Opcode, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, nil, err
	}
	length := uint64(header[1] & 0x7f)
	if length == 126 {
		size := make([]byte, 2)
		if _, err := io.ReadFull(reader, size); err != nil {
			return 0, nil, err
		}
		length = uint64(size[0])<<8 | uint64(size[1])
	} else if length == 127 {
		size := make([]byte, 8)
		if _, err := io.ReadFull(reader, size); err != nil {
			return 0, nil, err
		}
		length = 0
		for _, value := range size {
			length = length<<8 | uint64(value)
		}
	}
	payload := make([]byte, int(length))
	_, err := io.ReadFull(reader, payload)
	return Opcode(header[0] & 0xf), payload, err
}
