//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/go/taskpool"
)

func TestEngineServesOnAdaptivePool(t *testing.T) {
	config := DefaultConfig()
	config.SetTaskPoolMode(taskpool.ModeAdaptive)
	config.MinWorkerCount = 2
	config.SharedTaskPool = false
	_, addr := startEchoServer(t, config, echoHandler())
	payload := bytes.Repeat([]byte("adaptive"), 1024)
	for i := 0; i < 8; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, len(payload))
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(reply, payload) {
			t.Fatal("echo mismatch")
		}
		conn.Close()
	}
}

func TestBindRejectsAdaptiveFloorAboveCeiling(t *testing.T) {
	for _, floor := range []int{-1, 65} {
		config := DefaultConfig()
		config.Addr = "127.0.0.1:0"
		config.SetTaskPoolMode(taskpool.ModeAdaptive)
		config.SetPoolSizing(64, 0)
		config.MinWorkerCount = floor
		if server, err := Bind(config, nil); err == nil {
			server.Close()
			t.Fatalf("Bind accepted MinWorkerCount %d with WorkerCount 64", floor)
		}
	}
}
