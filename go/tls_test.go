//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
)

func tlsConfigs(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	server, client, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	return server, client
}

// A standard TLS client talks to a TLS server on the engine. Large payloads
// arrive as many records split across read rounds, and the echoes queue up
// behind backpressure, so every path through the layer is exercised.
func TestTLSServerEchoesStandardClient(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			serverConfig, clientConfig := tlsConfigs(t)
			clientConfig.MaxVersion = version
			_, addr := startEchoServer(t, DefaultConfig(), NewTLSServer(serverConfig, echoHandler()))

			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(value byte) {
					defer wg.Done()
					conn, err := tls.Dial("tcp", addr, clientConfig)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
					if got := conn.ConnectionState().Version; got != version {
						t.Errorf("negotiated %x, want %x", got, version)
					}
					payload := bytes.Repeat([]byte{value}, 1<<20)
					go func() { _, _ = conn.Write(payload) }()
					received := make([]byte, len(payload))
					if _, err = io.ReadFull(conn, received); err != nil {
						t.Error(err)
						return
					}
					if !bytes.Equal(received, payload) {
						t.Error("echo mismatch")
					}
				}(byte(i))
			}
			wg.Wait()
		})
	}
}

// The engine dials a standard TLS server. What done sends goes out before the
// handshake has finished, so it has to wait for it.
func TestDialTLSReachesStandardServer(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.NextProtos = []string{"fib-test"}
	clientConfig.NextProtos = []string{"fib-test"}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	client, _ := startEchoServer(t, DefaultConfig(), nil)
	payload := []byte("hello over tls")
	received := make(chan []byte, 1)
	var mu sync.Mutex
	var got []byte
	var state tls.ConnectionState
	handler := HandlerFuncs{Data: func(c *Connection, b []byte) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, b...)
		if len(got) == len(payload) {
			state, _ = c.TLSConnectionState()
			received <- got
		}
	}}
	// ServerName is left for DialTLS to fill in from the address.
	clientConfig.ServerName = ""
	err = client.DialTLS("tcp", listener.Addr().String(), 5*time.Second, clientConfig, handler,
		func(c *Connection, err error) {
			if err != nil {
				t.Error(err)
				return
			}
			if _, ok := c.TLSConnectionState(); ok {
				t.Error("handshake reported complete before it started")
			}
			if err := c.Send(payload); err != nil {
				t.Error(err)
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case echoed := <-received:
		if !bytes.Equal(echoed, payload) {
			t.Fatalf("echo mismatch: %q", echoed)
		}
		if state.NegotiatedProtocol != "fib-test" {
			t.Fatalf("negotiated protocol %q", state.NegotiatedProtocol)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no echo")
	}
}

// Both ends on engines: SendParts encrypts two parts as one message, and
// CloseAfterSend ends the stream with close_notify after the data it follows.
func TestTLSEngineToEngineCloseAfterSend(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	_, addr := startEchoServer(t, DefaultConfig(), NewTLSServer(serverConfig, HandlerFuncs{
		Open: func(c *Connection) {
			// Sent from OnOpen, before the handshake: held, then flushed.
			_ = c.SendParts([]byte("hello, "), []byte("tls"))
			c.CloseAfterSend()
			if err := c.Send([]byte("late")); err == nil {
				t.Error("send after CloseAfterSend succeeded")
			}
		},
	}))
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	var mu sync.Mutex
	var got []byte
	closed := make(chan error, 1)
	handler := HandlerFuncs{
		Data: func(_ *Connection, b []byte) {
			mu.Lock()
			got = append(got, b...)
			mu.Unlock()
		},
		Close: func(_ *Connection, err error) { closed <- err },
	}
	if err := client.DialTLS("tcp", addr, 5*time.Second, clientConfig, handler, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("connection was not closed")
	}
	mu.Lock()
	defer mu.Unlock()
	if string(got) != "hello, tls" {
		t.Fatalf("received %q", got)
	}
}

// A client that rejects the server's certificate fails the handshake, and the
// handler hears about it in OnClose.
func TestTLSHandshakeFailureClosesConnection(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	serverClosed := make(chan error, 1)
	_, addr := startEchoServer(t, DefaultConfig(), NewTLSServer(serverConfig, HandlerFuncs{
		Close: func(_ *Connection, err error) { serverClosed <- err },
	}))
	client, _ := startEchoServer(t, DefaultConfig(), nil)
	clientClosed := make(chan error, 1)
	handler := HandlerFuncs{Close: func(_ *Connection, err error) { clientClosed <- err }}
	// No RootCAs: the self-signed certificate is not trusted.
	if err := client.DialTLS("tcp", addr, 5*time.Second, &tls.Config{ServerName: "localhost"}, handler, nil); err != nil {
		t.Fatal(err)
	}
	for _, closed := range []chan error{clientClosed, serverClosed} {
		select {
		case err := <-closed:
			if err == nil {
				t.Fatal("handshake failure closed without an error")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("handshake failure did not close the connection")
		}
	}
}

// A peer that connects and never starts the handshake is closed once the
// handshake timeout runs out.
func TestTLSHandshakeTimeout(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	closed := make(chan error, 1)
	handler := NewTLSServer(serverConfig, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	handler.HandshakeTimeout = 100 * time.Millisecond
	_, addr := startEchoServer(t, DefaultConfig(), handler)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("timed-out handshake closed without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handshake did not time out")
	}
}
