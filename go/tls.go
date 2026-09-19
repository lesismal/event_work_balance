package fib

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

// DefaultTLSHandshakeTimeout bounds a TLS handshake when TLSHandler leaves
// HandshakeTimeout at zero.
const DefaultTLSHandshakeTimeout = 10 * time.Second

// tlsReadBufferSize holds the largest record crypto/tls will hand back from
// one Read, so a drain never splits a record's plaintext across two calls.
const tlsReadBufferSize = 16 << 10

var tlsReadBufferPool = sync.Pool{New: func() any { return &readBuffer{data: make([]byte, tlsReadBufferSize)} }}

// errTLSWouldBlock is what the transport tells crypto/tls when a read would
// have to wait for bytes the socket has not delivered yet. crypto/tls treats a
// temporary net.Error as retryable and keeps the partial record it has already
// buffered, which is what makes a record that arrives across several rounds
// decode correctly.
var errTLSWouldBlock error = tlsWouldBlockError{}

type tlsWouldBlockError struct{}

func (tlsWouldBlockError) Error() string   { return "fib: tls read would block" }
func (tlsWouldBlockError) Timeout() bool   { return false }
func (tlsWouldBlockError) Temporary() bool { return true }

// TLSHandler runs TLS on the connections it serves and hands Handler the
// plaintext. Handler sees an ordinary connection: its Send, SendOwned,
// SendParts and CloseAfterSend encrypt, and its OnData receives decrypted
// bytes, so protocol handlers such as the http and websocket packages run over
// TLS unchanged.
//
// OnOpen reaches Handler as soon as the TCP connection is established, before
// the handshake, and it may send at once: whatever it sends is held until the
// handshake completes and then encrypted in order. A handshake that fails or
// times out closes the connection, and Handler's OnClose receives the error.
//
// The handshake needs a round trip or two with the peer, and crypto/tls runs
// it as a blocking call, so each connection's handshake runs on a goroutine of
// its own that exits once it completes. Records after that are decrypted by
// the engine's workers in OnData like any other input.
type TLSHandler struct {
	Config  *tls.Config
	Handler Handler
	// Client selects the client side of the handshake. Dialed connections
	// need it; accepted ones need it left false.
	Client bool
	// HandshakeTimeout bounds the handshake. Zero means
	// DefaultTLSHandshakeTimeout and a negative value means no bound.
	HandshakeTimeout time.Duration
}

// NewTLSServer returns a handler that serves TLS with config in front of
// handler, for Bind.
func NewTLSServer(config *tls.Config, handler Handler) *TLSHandler {
	return &TLSHandler{Config: config, Handler: handler}
}

// NewTLSClient returns a handler that runs the client side of TLS with config
// in front of handler, for DialWithHandler. config needs ServerName, or
// InsecureSkipVerify, as it does for tls.Client; DialTLS fills ServerName in.
func NewTLSClient(config *tls.Config, handler Handler) *TLSHandler {
	return &TLSHandler{Config: config, Handler: handler, Client: true}
}

// DialTLS is DialWithHandler over TLS. When config names no server, the host in
// addr is used, as tls.Dial does. done runs once the TCP connect completes, as
// it does for Dial; the handshake follows, and what done sends waits for it.
func (e *Engine) DialTLS(network, addr string, timeout time.Duration, config *tls.Config,
	handler Handler, done func(*Connection, error)) error {
	if config == nil {
		config = &tls.Config{}
	}
	if config.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		config = config.Clone()
		config.ServerName = host
	}
	if handler == nil {
		handler = e.handler
	}
	return e.DialWithHandler(network, addr, timeout, NewTLSClient(config, handler), done)
}

func (h *TLSHandler) inner() Handler {
	if h.Handler == nil {
		return HandlerFuncs{}
	}
	return h.Handler
}

func (h *TLSHandler) OnOpen(c *Connection) {
	t := &tlsLayer{c: c, handshaking: true}
	t.cond.L = &t.mu
	if h.Client {
		t.conn = tls.Client(t, h.Config)
	} else {
		t.conn = tls.Server(t, h.Config)
	}
	c.SetLayer(t)
	h.inner().OnOpen(c)
	timeout := h.HandshakeTimeout
	if timeout == 0 {
		timeout = DefaultTLSHandshakeTimeout
	}
	go t.handshake(h.inner(), timeout)
}

func (h *TLSHandler) OnData(c *Connection, data []byte) {
	if t, ok := c.layer.(*tlsLayer); ok {
		t.feed(h.inner(), data)
	}
}

// OnPriorityData passes out-of-band bytes through untouched: they travel
// beside the TLS stream, not inside it.
func (h *TLSHandler) OnPriorityData(c *Connection, data []byte) {
	h.inner().OnPriorityData(c, data)
}

func (h *TLSHandler) OnClose(c *Connection, err error) {
	if t, ok := c.layer.(*tlsLayer); ok {
		t.shutdown()
	}
	h.inner().OnClose(c, err)
}

// tlsLayer sits between a connection's socket and crypto/tls. To crypto/tls it
// is the transport: Read serves the ciphertext OnData collected and Write
// queues records on the connection.
type tlsLayer struct {
	c    *Connection
	conn *tls.Conn

	// mu guards the collected ciphertext and the handshake flag. cond wakes a
	// handshake waiting for the peer.
	mu          sync.Mutex
	cond        sync.Cond
	in          []byte
	handshaking bool
	closed      bool

	// readMu makes whoever decrypts, the handshake goroutine for the bytes
	// that arrived with the handshake or a worker afterwards, the only one
	// delivering plaintext, so OnData never runs twice at once.
	readMu sync.Mutex

	// wmu orders sends. Until ready, they wait in pending in the order they
	// were made, and closeAfterSend remembers a CloseAfterSend among them.
	wmu            sync.Mutex
	ready          bool
	failed         bool
	pending        [][]byte
	closeAfterSend bool
	joined         []byte
}

func (t *tlsLayer) handshake(handler Handler, timeout time.Duration) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	err := t.conn.HandshakeContext(ctx)

	t.wmu.Lock()
	pending, closeAfterSend := t.pending, t.closeAfterSend
	t.pending = nil
	for i := 0; err == nil && i < len(pending); i++ {
		_, err = t.conn.Write(pending[i])
	}
	if err != nil {
		t.failed = true
	} else {
		t.ready = true
		if closeAfterSend {
			t.closeNotifyLocked()
		}
	}
	t.wmu.Unlock()

	t.mu.Lock()
	t.handshaking = false
	t.mu.Unlock()
	if err != nil {
		t.c.closeWithError(err)
		return
	}
	// The peer may have sent application data right behind its last handshake
	// message, and crypto/tls may already have read it. Nothing raises another
	// read for it, so it is delivered here.
	t.drain(handler)
}

// feed collects ciphertext from a read round. During the handshake it only
// wakes the handshake goroutine; afterwards it decrypts what it can.
func (t *tlsLayer) feed(handler Handler, data []byte) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.in = append(t.in, data...)
	if t.handshaking {
		t.cond.Signal()
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	t.drain(handler)
}

// drain decrypts every complete record collected so far and hands the
// plaintext to handler.
func (t *tlsLayer) drain(handler Handler) {
	t.readMu.Lock()
	defer t.readMu.Unlock()
	buffer := tlsReadBufferPool.Get().(*readBuffer)
	defer tlsReadBufferPool.Put(buffer)
	for {
		n, err := t.conn.Read(buffer.data)
		if n > 0 {
			handler.OnData(t.c, buffer.data[:n])
		}
		if err == nil {
			continue
		}
		if err == errTLSWouldBlock {
			return
		}
		if err == io.EOF {
			// close_notify: the peer has finished cleanly.
			t.c.closeWithError(io.EOF)
		} else {
			t.c.closeWithError(err)
		}
		return
	}
}

// shutdown wakes a handshake still waiting on the peer, which then fails.
func (t *tlsLayer) shutdown() {
	t.mu.Lock()
	t.closed = true
	t.in = nil
	t.cond.Broadcast()
	t.mu.Unlock()
}

// Send encrypts plaintext, or holds a copy of it until the handshake is done.
func (t *tlsLayer) Send(first, second []byte) error {
	if len(first)+len(second) == 0 {
		return nil
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.failed || t.closeAfterSend || t.c.sendClosed() {
		return syscall.EPIPE
	}
	if !t.ready {
		data := make([]byte, 0, len(first)+len(second))
		t.pending = append(t.pending, append(append(data, first...), second...))
		return nil
	}
	data := first
	if len(second) > 0 {
		// One record for both parts: a frame header sent as a record of its
		// own would cost more in record overhead than it carries.
		t.joined = append(append(t.joined[:0], first...), second...)
		data = t.joined
	}
	_, err := t.conn.Write(data)
	if cap(t.joined) > tlsReadBufferSize {
		t.joined = nil
	}
	return err
}

// CloseAfterSend ends the stream with close_notify behind what was already
// sent, and then closes the connection once it has all been written.
func (t *tlsLayer) CloseAfterSend() {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.closeAfterSend {
		return
	}
	t.closeAfterSend = true
	if t.ready {
		t.closeNotifyLocked()
	} else if t.failed {
		t.c.closeWithError(nil)
	}
}

func (t *tlsLayer) closeNotifyLocked() {
	_ = t.conn.CloseWrite()
	t.c.closeAfterSendRaw()
}

// Read is crypto/tls reading the transport. During the handshake it waits for
// the peer; afterwards it never waits, and reports errTLSWouldBlock instead.
func (t *tlsLayer) Read(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for len(t.in) == 0 {
		if t.closed {
			return 0, io.EOF
		}
		if !t.handshaking {
			return 0, errTLSWouldBlock
		}
		t.cond.Wait()
	}
	n := copy(p, t.in)
	if n == len(t.in) {
		if cap(t.in) > 4*tlsReadBufferSize {
			t.in = nil
		} else {
			t.in = t.in[:0]
		}
	} else {
		t.in = t.in[n:]
	}
	return n, nil
}

// Write is crypto/tls writing records. The connection copies them, since
// crypto/tls reuses its buffer for the next record.
func (t *tlsLayer) Write(p []byte) (int, error) {
	if err := t.c.sendRaw(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close is crypto/tls abandoning the transport, which it does when the
// handshake context expires.
func (t *tlsLayer) Close() error {
	t.shutdown()
	t.c.closeWithError(errTLSHandshakeTimeout)
	return nil
}

var errTLSHandshakeTimeout = errors.New("fib: tls handshake timed out")

func (t *tlsLayer) LocalAddr() net.Addr              { return tlsAddr{} }
func (t *tlsLayer) RemoteAddr() net.Addr             { return tlsAddr{} }
func (t *tlsLayer) SetDeadline(time.Time) error      { return nil }
func (t *tlsLayer) SetReadDeadline(time.Time) error  { return nil }
func (t *tlsLayer) SetWriteDeadline(time.Time) error { return nil }

type tlsAddr struct{}

func (tlsAddr) Network() string { return "tcp" }
func (tlsAddr) String() string  { return "fib" }

// TLSConnectionState reports the connection's TLS parameters, such as the
// negotiated protocol and the peer's certificates. It reports false for a
// connection without TLS and for one whose handshake has not completed.
func (c *Connection) TLSConnectionState() (tls.ConnectionState, bool) {
	t, ok := c.layer.(*tlsLayer)
	if !ok {
		return tls.ConnectionState{}, false
	}
	t.wmu.Lock()
	ready := t.ready
	t.wmu.Unlock()
	if !ready {
		return tls.ConnectionState{}, false
	}
	return t.conn.ConnectionState(), true
}
