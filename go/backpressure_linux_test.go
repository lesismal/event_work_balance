//go:build linux

package epoll

import (
	"bytes"
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"
)

// startEchoServer brings up a server on a loopback port and returns its address
// plus a shutdown function that fails the test if the loop errored.
func startEchoServer(t *testing.T, config Config, handler Handler) (*Server, string) {
	t.Helper()
	config.BindAddress = "127.0.0.1"
	config.Port = 0
	server, err := Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	return server, addr.String()
}

// The server-wide budget is the bound that actually holds at high connection
// counts, where the per-connection watermark alone would admit its own limit
// times the connection count. Peers that stop reading must not be able to push
// the server past it.
func TestServerWideBudgetBoundsQueuedBytes(t *testing.T) {
	const (
		budget      = 256 << 10
		connections = 8
		payload     = 32 << 10
	)
	config := DefaultConfig()
	// Give each connection room to buffer far more than the budget allows in
	// aggregate, so only the server-wide bound can hold the total down.
	config.WriteBufferHighWatermark = budget
	config.MaxPendingBytes = budget
	server, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		// Reply with far more than arrived, so a peer that never reads builds
		// a backlog quickly.
		for i := 0; i < 8; i++ {
			if err := c.Send(b); err != nil {
				c.Close()
				return
			}
		}
	}})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < connections; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Never read: the replies pile up in the server's queue and the
			// kernel's buffers until backpressure stops them.
			data := bytes.Repeat([]byte{'x'}, payload)
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := conn.Write(data); err != nil {
					return
				}
			}
		}()
	}

	// Sample the budget while the peers hammer it.
	deadline := time.Now().Add(3 * time.Second)
	var peak int64
	for time.Now().Before(deadline) {
		if pending := server.pendingTotal.Load(); pending > peak {
			peak = pending
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if peak == 0 {
		t.Fatal("no outbound backlog built up; the test never exercised the budget")
	}
	// The bound is checked before a round queues its replies, so one round's
	// worth of output may land on top of it. Allow that overshoot, but nothing
	// like the connections*watermark a per-connection bound alone would admit.
	limit := int64(budget) * 4
	if peak > limit {
		t.Fatalf("peak pending = %d bytes, want at most %d (budget %d)", peak, limit, budget)
	}
}

// A connection stopped only by the server-wide budget may have nothing of its
// own left to flush, so nothing of its own would ever re-evaluate it. It has to
// be resumed once the budget recovers, or it stalls forever.
func TestBudgetPausedConnectionResumes(t *testing.T) {
	config := DefaultConfig()
	config.WriteBufferHighWatermark = 64 << 10
	// A budget this small is exhausted by the first reply, so the reading
	// connection below is certain to be paused on the budget's account rather
	// than its own.
	config.MaxPendingBytes = 1
	_, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	// Each round trip has to survive the pause: the reply exhausts the budget,
	// the connection is parked, and only the resume path can bring it back for
	// the next request.
	request := bytes.Repeat([]byte{'p'}, 512)
	reply := make([]byte, len(request))
	for i := 0; i < 20; i++ {
		if _, err := conn.Write(request); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(reply, request) {
			t.Fatalf("round %d: echo mismatch", i)
		}
	}
}

// Descriptors are recycled by the kernel, and the connection table is indexed
// by descriptor. A new connection landing on a closed one's descriptor must be
// reached by its own events, and must not inherit anything from its predecessor.
func TestRecycledDescriptorGetsFreshConnection(t *testing.T) {
	var mu sync.Mutex
	tokensByFD := map[int][]uint64{}
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			tokensByFD[c.FD()] = append(tokensByFD[c.FD()], c.token)
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})

	// Serially open, use and close connections. Closing each before opening the
	// next makes the kernel hand the same descriptor back repeatedly.
	payload := []byte("recycled")
	reply := make([]byte, len(payload))
	for i := 0; i < 24; i++ {
		conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(reply, payload) {
			t.Fatalf("round %d: echo mismatch", i)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		// Give the loop a moment to process the close before the next dial, so
		// the descriptor is actually free to be reused.
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	reused := false
	for fd, tokens := range tokensByFD {
		if len(tokens) > 1 {
			reused = true
		}
		seen := map[uint64]bool{}
		for _, token := range tokens {
			if seen[token] {
				t.Fatalf("descriptor %d handed out token %d twice", fd, token)
			}
			seen[token] = true
			if int(uint32(token)) != fd {
				t.Fatalf("token %d does not encode descriptor %d", token, fd)
			}
		}
	}
	if !reused {
		t.Skip("kernel never recycled a descriptor; nothing was exercised")
	}
}

// A stale token must not resolve, even to a live connection on the same
// descriptor. This is what the generation half of the token buys.
//
// The lookups run inside OnOpen because the connection table belongs to the
// event loop; checking it from the test goroutine would be the race it is
// meant to rule out.
func TestConnectionForRejectsStaleToken(t *testing.T) {
	type failure struct{ msg string }
	checked := make(chan failure, 1)
	config := DefaultConfig()
	_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) {
		// Reach the server through the connection rather than through a
		// variable the test goroutine is still assigning.
		server := c.server
		report := func(msg string) {
			select {
			case checked <- failure{msg}:
			default:
			}
		}
		switch {
		case server.connectionFor(c.token) != c:
			report("connectionFor(live token) did not return the live connection")
		// Same descriptor, different generation.
		case server.connectionFor(c.token^(1<<32)) != nil:
			report("connectionFor(stale token) resolved to a connection")
		// A descriptor past the end of the table must not panic.
		case server.connectionFor(uint64(uint32(len(server.connections)+100))) != nil:
			report("connectionFor(out-of-range descriptor) resolved to a connection")
		default:
			report("")
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case got := <-checked:
		if got.msg != "" {
			t.Fatal(got.msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never registered the connection")
	}
}

// Chunks queued behind an undrained item must merge into that item's buffer
// rather than each taking one of their own. Before this, a connection under
// backpressure allocated a fresh array per reply, which is what made a
// 100k-connection rate test hold gigabytes.
func TestQueuedChunksMergeIntoOneBuffer(t *testing.T) {
	server := newOfflineServer(t)
	c := &Connection{token: 1, server: server}
	c.fd.Store(-1)

	chunk := bytes.Repeat([]byte{'z'}, 1024)
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < 16; i++ {
		c.queueLocked(chunk, nil)
	}
	if len(c.sends) != 1 {
		t.Fatalf("queue holds %d items, want the chunks merged into 1", len(c.sends))
	}
	if got, want := len(c.sends[0].data), 16*len(chunk); got != want {
		t.Fatalf("merged item holds %d bytes, want %d", got, want)
	}
	if c.sends[0].buf == nil {
		t.Fatal("merged item has no pooled buffer to return")
	}

	// Once the socket has consumed part of the item, merging into it would
	// disturb the write in progress, so the next chunk takes its own buffer.
	c.sends[0].offset = 1
	c.queueLocked(chunk, nil)
	if len(c.sends) != 2 {
		t.Fatalf("queue holds %d items, want a second item for the partially written one", len(c.sends))
	}
}

// Draining an item has to hand its buffer back to the pool, or the merge above
// only postpones the allocation to the next round.
func TestDrainedItemReturnsBufferToPool(t *testing.T) {
	server := newOfflineServer(t)
	c := &Connection{token: 1, server: server}
	c.fd.Store(-1)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueLocked(bytes.Repeat([]byte{'z'}, 1024), nil)
	buf := c.sends[0].buf
	if buf == nil {
		t.Fatal("queued item has no pooled buffer")
	}
	c.releaseItemLocked(&c.sends[0])
	if c.sends[0].buf != nil || c.sends[0].data != nil {
		t.Fatal("released item still references its buffer")
	}
	// The pool is a sync.Pool, which may drop entries at any GC, so the only
	// guarantee worth asserting is that what comes back is reusable and that
	// the released buffer was reset rather than left holding its old contents.
	if len(buf.data) != 0 {
		t.Fatalf("released buffer still holds %d bytes", len(buf.data))
	}
	got := server.sendBufferPool.Get().(*sendBuffer)
	if len(got.data) != 0 {
		t.Fatalf("pooled buffer came back holding %d bytes", len(got.data))
	}
}

// newOfflineServer builds a server for exercising connection bookkeeping
// directly, without running its event loop.
func newOfflineServer(t *testing.T) *Server {
	t.Helper()
	config := DefaultConfig()
	config.BindAddress = "127.0.0.1"
	config.Port = 0
	server, err := Bind(config, HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}

// An outsized buffer must not be retained, or one large message would leave
// every pooled buffer permanently inflated.
func TestOutsizedSendBufferIsDropped(t *testing.T) {
	server := newOfflineServer(t)
	c := &Connection{token: 1, server: server}
	c.fd.Store(-1)
	oversized := &sendBuffer{data: make([]byte, 0, server.retainedSendBuffer*2)}
	item := sendItem{data: oversized.data, buf: oversized}
	c.mu.Lock()
	c.releaseItemLocked(&item)
	c.mu.Unlock()
	if item.buf != nil {
		t.Fatal("released item still references its buffer")
	}
	// The pool must not be holding the oversized array: a fresh Get should come
	// back with a small one.
	got := server.sendBufferPool.Get().(*sendBuffer)
	if cap(got.data) > server.retainedSendBuffer {
		t.Fatalf("pool returned a buffer of cap %d, want at most %d", cap(got.data), server.retainedSendBuffer)
	}
}

// Backpressure must not corrupt the stream: a peer that reads slowly still has
// to receive every byte, in order, regardless of how often reads were paused.
func TestPausedReadsPreserveStreamIntegrity(t *testing.T) {
	const payload = 512 << 10
	config := DefaultConfig()
	config.WriteBufferHighWatermark = 8 << 10
	config.MaxPendingBytes = 32 << 10
	_, addr := startEchoServer(t, config, HandlerFuncs{Data: func(c *Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}})

	conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	// A recognisable pattern so a reorder or a gap shows up as a mismatch.
	sent := make([]byte, payload)
	for i := range sent {
		sent[i] = byte(i % 251)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(sent)
		writeErr <- err
	}()

	got := make([]byte, payload)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErr; err != nil && err != syscall.EPIPE {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, sent) {
		for i := range got {
			if got[i] != sent[i] {
				t.Fatalf("echo diverges at byte %d: got %d, want %d", i, got[i], sent[i])
			}
		}
	}
}
