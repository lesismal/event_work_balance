//go:build linux || darwin || windows

package websocket

import (
	"testing"
	"time"
)

func receive(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(5 * time.Second):
		t.Fatalf("no %s", what)
		return ""
	}
}

// A Ping handler replaces the default reply: it sees the ping and decides
// what, if anything, goes back. A Pong handler sees the pongs that arrive.
func TestCustomPingAndPongHandlers(t *testing.T) {
	serverPings := make(chan string, 4)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{
		Message: echoServerHandler().Message,
		Ping: func(c *Connection, payload []byte) {
			serverPings <- string(payload)
			if string(payload) != "silent" {
				_ = c.Pong(append([]byte("custom:"), payload...))
			}
		},
	})
	clientPongs := make(chan string, 4)
	messages := make(chan string, 4)
	conn, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, HandlerFuncs{
		Message: func(_ *Connection, _ Opcode, payload []byte) { messages <- string(payload) },
		Pong:    func(_ *Connection, payload []byte) { clientPongs <- string(payload) },
	}).Wait()
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Ping([]byte("p1")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, serverPings, "ping at the server"); got != "p1" {
		t.Fatalf("server saw ping %q", got)
	}
	if got := receive(t, clientPongs, "pong at the client"); got != "custom:p1" {
		t.Fatalf("client saw pong %q, want the custom reply", got)
	}

	// A Ping handler that sends nothing means no pong at all, and the
	// connection carries on.
	if err := conn.Ping([]byte("silent")); err != nil {
		t.Fatal(err)
	}
	receive(t, serverPings, "silent ping at the server")
	if err := conn.WriteText("still here"); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, messages, "echo"); got != "still here" {
		t.Fatalf("echo %q", got)
	}
	select {
	case got := <-clientPongs:
		t.Fatalf("pong %q arrived though the Ping handler sent none", got)
	default:
	}
}

// Without a Ping handler a ping is answered with its own payload, in both
// directions.
func TestDefaultPingReplyInBothDirections(t *testing.T) {
	serverSide := make(chan *Connection, 1)
	serverPongs := make(chan string, 1)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{
		Message: func(c *Connection, _ Opcode, _ []byte) { serverSide <- c },
		Pong:    func(_ *Connection, payload []byte) { serverPongs <- string(payload) },
	})
	clientPongs := make(chan string, 1)
	conn, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, HandlerFuncs{
		Pong: func(_ *Connection, payload []byte) { clientPongs <- string(payload) },
	}).Wait()
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Ping([]byte("from client")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, clientPongs, "pong at the client"); got != "from client" {
		t.Fatalf("client saw pong %q", got)
	}

	if err := conn.WriteText("hello"); err != nil {
		t.Fatal(err)
	}
	var server *Connection
	select {
	case server = <-serverSide:
	case <-time.After(5 * time.Second):
		t.Fatal("no message at the server")
	}
	if err := server.Ping([]byte("from server")); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, serverPongs, "pong at the server"); got != "from server" {
		t.Fatalf("server saw pong %q", got)
	}
}
