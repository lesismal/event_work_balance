//go:build !linux && !darwin && !windows

package fib

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPortableConcurrentEcho(t *testing.T) {
	config := DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, data []byte) {
		if err := c.Send(data); err != nil {
			c.Close()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	address, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	var clients sync.WaitGroup
	for i := 0; i < 16; i++ {
		clients.Add(1)
		go func(value byte) {
			defer clients.Done()
			payload := bytes.Repeat([]byte{value}, 256*1024)
			conn, err := net.DialTimeout("tcp4", address.String(), 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
			if _, err = conn.Write(payload); err != nil {
				t.Error(err)
				return
			}
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
	clients.Wait()
	server.Stop()
	if err := <-runDone; err != nil {
		t.Error(err)
	}
	if err := server.Close(); err != nil {
		t.Error(err)
	}
}
