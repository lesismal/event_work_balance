package quic

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"errors"
	mrand "math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
)

// pipeEnd is one side of an in-memory path. Datagrams are delivered in
// order on a goroutine of their own, as a network would, and some may be
// dropped on purpose.
type pipeEnd struct {
	queue chan []byte
	peer  *pipeEnd
	loss  float64
	// drop, when set, decides the fate of each datagram instead of loss.
	drop   func([]byte) bool
	mu     sync.Mutex
	closed bool
}

func (p *pipeEnd) Send(d []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return net.ErrClosed
	}
	if p.drop != nil && p.drop(d) || p.drop == nil && p.loss > 0 && mrand.Float64() < p.loss {
		return nil
	}
	select {
	case p.peer.queue <- bytes.Clone(d):
	default:
		// A full queue is a full socket buffer: the datagram is lost.
	}
	return nil
}

func (p *pipeEnd) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func newPipe() (a, b *pipeEnd) {
	a = &pipeEnd{queue: make(chan []byte, 4096)}
	b = &pipeEnd{queue: make(chan []byte, 4096)}
	a.peer, b.peer = b, a
	return a, b
}

// testHandler records what a connection hears.
type testHandler struct {
	mu        sync.Mutex
	handshake chan struct{}
	closed    chan error
	data      map[uint64][]byte
	fin       map[uint64]bool
	finished  chan uint64
	finSet    map[uint64]bool
	reset     chan uint64
	onData    func(s *Stream, data []byte, fin bool)
}

func newTestHandler() *testHandler {
	return &testHandler{
		handshake: make(chan struct{}),
		closed:    make(chan error, 1),
		data:      make(map[uint64][]byte),
		fin:       make(map[uint64]bool),
		finished:  make(chan uint64, 1024),
		finSet:    make(map[uint64]bool),
		reset:     make(chan uint64, 1024),
	}
}

func (h *testHandler) OnHandshake(*Conn) { close(h.handshake) }
func (h *testHandler) OnStreamData(s *Stream, data []byte, fin bool) {
	h.mu.Lock()
	h.data[s.ID()] = append(h.data[s.ID()], data...)
	h.fin[s.ID()] = fin
	h.mu.Unlock()
	if h.onData != nil {
		h.onData(s, data, fin)
	}
	if fin {
		h.finished <- s.ID()
	}
}
func (h *testHandler) OnStreamReset(s *Stream, code uint64) { h.reset <- s.ID() }
func (h *testHandler) OnStopSending(*Stream, uint64)        {}
func (h *testHandler) OnStreamsAvailable(*Conn)             {}
func (h *testHandler) OnClose(_ *Conn, err error)           { h.closed <- err }

func (h *testHandler) received(id uint64) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.data[id]
}

type testPair struct {
	client, server       *Conn
	clientH, serverH     *testHandler
	clientEnd, serverEnd *pipeEnd
	done                 chan struct{}
	wg                   sync.WaitGroup
	serverMu             sync.Mutex
	// stray gets what reaches the server while it has no connection.
	stray func(d []byte)
}

func (p *testPair) close() {
	close(p.done)
	p.wg.Wait()
}

// newTestPair connects a client and a server over a pipe. The server starts
// on the first datagram that reaches it, as a fib server does.
func newTestPair(t *testing.T, loss float64, configure func(client, server *Config)) *testPair {
	t.Helper()
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.NextProtos = []string{"test"}
	clientTLS.NextProtos = []string{"test"}
	cc := Config{TLSConfig: clientTLS}
	sc := Config{TLSConfig: serverTLS}
	if configure != nil {
		configure(&cc, &sc)
	}
	p := &testPair{clientH: newTestHandler(), serverH: newTestHandler(), done: make(chan struct{})}
	p.clientEnd, p.serverEnd = newPipe()
	p.clientEnd.loss, p.serverEnd.loss = loss, loss
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case d := <-p.serverEnd.queue:
				p.serverMu.Lock()
				if p.server == nil {
					if p.stray != nil {
						p.stray(d)
					} else if IsInitial(d) {
						p.server = Accept(p.serverEnd, addr, sc, p.serverH, d)
					}
					p.serverMu.Unlock()
					continue
				}
				s := p.server
				p.serverMu.Unlock()
				s.HandleDatagram(d)
			case <-p.done:
				return
			}
		}
	}()
	p.client, err = Dial(p.clientEnd, addr, cc, p.clientH)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer p.wg.Done()
		for {
			select {
			case d := <-p.clientEnd.queue:
				p.client.HandleDatagram(d)
			case <-p.done:
				return
			}
		}
	}()
	t.Cleanup(p.close)
	select {
	case <-p.clientH.handshake:
	case err := <-p.clientH.closed:
		t.Fatalf("client closed during handshake: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("handshake timed out")
	}
	return p
}

func waitFinished(t *testing.T, h *testHandler, id uint64) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for !h.finSet[id] {
		select {
		case got := <-h.finished:
			h.finSet[got] = true
		case err := <-h.closed:
			t.Fatalf("connection closed: %v", err)
		case <-timeout:
			t.Fatalf("stream %d not finished", id)
		}
	}
}

func TestHandshakeAndEcho(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("hello, quic"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); string(got) != "hello, quic" {
		t.Fatalf("echo %q", got)
	}
	state := p.client.ConnectionState()
	if state.NegotiatedProtocol != "test" || state.Version != tls.VersionTLS13 {
		t.Fatalf("state %+v", state)
	}
}

func testTransfer(t *testing.T, loss float64, size int) {
	p := newTestPair(t, loss, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	var streams []*Stream
	for i := 0; i < 4; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(payload, true); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, s)
	}
	for _, s := range streams {
		waitFinished(t, p.clientH, s.ID())
		if got := p.clientH.received(s.ID()); !bytes.Equal(got, payload) {
			t.Fatalf("stream %d: got %d bytes, want %d", s.ID(), len(got), len(payload))
		}
	}
}

func TestTransfer(t *testing.T) { testTransfer(t, 0, 2<<20) }

func TestTransferWithLoss(t *testing.T) { testTransfer(t, 0.05, 256<<10) }

func TestHandshakeWithLoss(t *testing.T) {
	for i := 0; i < 5; i++ {
		testTransfer(t, 0.2, 16<<10)
	}
}

func TestManyStreams(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxIncomingStreams = 10 })
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	done := 0
	for done < 100 {
		s, err := p.client.OpenStream()
		if errors.Is(err, ErrStreamLimit) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write([]byte("x"), true)
		waitFinished(t, p.clientH, s.ID())
		done++
	}
}

func TestStreamReset(t *testing.T) {
	p := newTestPair(t, 0, nil)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("partial"), false)
	s.Reset(7)
	select {
	case id := <-p.serverH.reset:
		if id != s.ID() {
			t.Fatalf("reset of %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reset")
	}
}

func TestApplicationClose(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.client.Close(0x42, "bye")
	select {
	case err := <-p.serverH.closed:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != 0x42 || appErr.Reason != "bye" || !appErr.Remote {
			t.Fatalf("server got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server not closed")
	}
	if err := <-p.clientH.closed; err == nil {
		t.Fatal("client closed without an error")
	}
}

func TestIdleTimeout(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) { client.MaxIdleTimeout = 200 * time.Millisecond })
	select {
	case err := <-p.serverH.closed:
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("server got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not idle out")
	}
}

func TestKeepAlive(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) {
		client.MaxIdleTimeout = 300 * time.Millisecond
		client.KeepAlivePeriod = 100 * time.Millisecond
	})
	select {
	case err := <-p.serverH.closed:
		t.Fatalf("server closed: %v", err)
	case err := <-p.clientH.closed:
		t.Fatalf("client closed: %v", err)
	case <-time.After(time.Second):
	}
}

func TestStatelessReset(t *testing.T) {
	key := NewResetKey()
	p := newTestPair(t, 0, func(_, server *Config) { server.ResetKey = &key })
	// The server forgets the connection; what the client sends next is
	// answered with a stateless reset.
	p.serverMu.Lock()
	p.server.Abort(errors.New("forgotten"))
	p.server = nil
	p.serverEnd.closed = false
	p.stray = func(d []byte) {
		if reset := key.StatelessReset(d); reset != nil {
			_ = p.serverEnd.Send(reset)
		}
	}
	p.serverMu.Unlock()
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("anyone there?"), true)
	select {
	case err := <-p.clientH.closed:
		if !errors.Is(err, ErrStatelessReset) {
			t.Fatalf("client got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client not reset")
	}
}

func TestVersionNegotiationPacket(t *testing.T) {
	d := make([]byte, 1200)
	d[0] = 0xc0
	copy(d[1:5], []byte{0x0a, 0x0a, 0x0a, 0x0a})
	d[5] = 8
	copy(d[6:14], "dcid----")
	d[14] = 4
	copy(d[15:19], "scid")
	vn := VersionNegotiation(d)
	if vn == nil {
		t.Fatal("no version negotiation")
	}
	h, err := parseHeader(vn, 0)
	if err != errUnknownVersion || h.version != 0 || string(h.dcid) != "scid" || string(h.scid) != "dcid----" {
		t.Fatalf("header %+v %v", h, err)
	}
	if IsInitial(d) {
		t.Fatal("unknown version taken for an Initial")
	}
}

func TestPathResponsesBounded(t *testing.T) {
	c := &Conn{}
	for i := 0; i < 100; i++ {
		c.queuePathResponse([8]byte{byte(i)})
	}
	if len(c.pathResponses) != maxPathResponses {
		t.Fatalf("%d responses queued, want %d", len(c.pathResponses), maxPathResponses)
	}
	for i, r := range c.pathResponses {
		if want := byte(100 - maxPathResponses + i); r[0] != want {
			t.Fatalf("response %d answers challenge %d, want %d", i, r[0], want)
		}
	}
}

// TestPathChallengeFlood sends many PATH_CHALLENGE frames in one packet: the
// connection answers without queueing them all, and stays up.
func TestPathChallengeFlood(t *testing.T) {
	p := newTestPair(t, 0, nil)
	var payload []byte
	for i := 0; i < 100; i++ {
		payload = append(payload, framePathChallenge, byte(i), 1, 2, 3, 4, 5, 6, 7)
	}
	c := p.client
	c.mu.Lock()
	plan := packetPlan{space: spaceApp, pn: c.spaces[spaceApp].nextPN, pnLen: 4, payload: payload, sp: &sentPacket{}}
	c.spaces[spaceApp].nextPN++
	d := c.sealPlan(nil, &plan)
	c.mu.Unlock()
	_ = p.clientEnd.Send(d)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("still there"), true)
	waitFinished(t, p.clientH, s.ID())
	p.server.mu.Lock()
	queued := len(p.server.pathResponses)
	p.server.mu.Unlock()
	if queued > maxPathResponses {
		t.Fatalf("%d responses queued", queued)
	}
}
