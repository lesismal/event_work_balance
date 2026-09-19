//go:build linux || darwin || windows

package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib/go"
	fibtls "github.com/lesismal/fib/go/tls"
)

var (
	// ErrClientClosed is what requests still waiting for a connection get when
	// the client is closed, and what new requests get afterwards.
	ErrClientClosed = errors.New("http: client closed")
	// ErrUnsupportedScheme is what a request for anything but http:// or
	// https:// gets.
	ErrUnsupportedScheme = errors.New("http: unsupported URL scheme")
	// errRequestTimeout is what a request gets when ClientConfig.Timeout runs
	// out. errors.Is matches it against os.ErrDeadlineExceeded.
	errRequestTimeout = fmt.Errorf("http: request timed out: %w", os.ErrDeadlineExceeded)
)

// ClientConfig bounds what a Client waits for and how many connections it
// keeps.
type ClientConfig struct {
	// Timeout bounds a request from Do until its response is complete:
	// waiting for a connection, dialing, sending and reading all count. Zero
	// means no limit beyond the request's context.
	Timeout time.Duration
	// DialTimeout bounds one connect. Zero leaves it to the operating system.
	DialTimeout time.Duration
	// MaxConnsPerHost caps the connections open or dialing to one host:port.
	// Requests beyond it wait for one of them. Zero or less means no cap.
	MaxConnsPerHost int
	// MaxIdleConnsPerHost is how many finished connections to one host:port
	// are kept for later requests. Zero or less keeps none, so every request
	// dials afresh.
	MaxIdleConnsPerHost int
	// IdleConnTimeout closes a kept connection that has not been reused for
	// this long. Zero keeps it until the server closes it.
	IdleConnTimeout time.Duration
	// MaxResponseHeaderBytes and MaxResponseBodyBytes bound what one response
	// may hold. The body is buffered whole before the callback runs, so the
	// second is also the memory one response can take.
	MaxResponseHeaderBytes int
	MaxResponseBodyBytes   int64
	// TLSConfig is used for https:// requests. Nil means the defaults; either
	// way a config naming no server gets the request's host. Unless it sets
	// NextProtos itself, ALPN offers HTTP/2 and HTTP/1.1.
	TLSConfig *tls.Config
	// DisableHTTP2 keeps https:// requests on HTTP/1.1. Otherwise they speak
	// HTTP/2 to any server that chooses it through ALPN.
	DisableHTTP2 bool
	// UnencryptedHTTP2 sends http:// requests as HTTP/2 with prior knowledge
	// (h2c), for servers known to speak it. It is off by default, since an
	// HTTP/1-only server cannot answer it.
	UnencryptedHTTP2 bool
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		Timeout:                30 * time.Second,
		DialTimeout:            10 * time.Second,
		MaxConnsPerHost:        64,
		MaxIdleConnsPerHost:    16,
		IdleConnTimeout:        90 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		MaxResponseBodyBytes:   64 << 20,
	}
}

// Client sends HTTP/1.1 and HTTP/2 requests over connections an engine dials
// and serves, without blocking the caller. Responses are read by the engine's
// workers and handed to a callback with their body already buffered, so a
// callback never waits on the network.
//
// A client keeps its connections alive between requests. An HTTP/1.1
// connection carries one request at a time; it does not pipeline. An HTTP/2
// connection carries as many at once as the server allows. http:// and
// https:// are supported; https speaks HTTP/2 when the server chooses it
// through ALPN and HTTP/1.1 over TLS otherwise, and http:// speaks HTTP/1.1
// unless ClientConfig.UnencryptedHTTP2 is set.
//
// The engine may be one made by fib.NewEngine for clients alone, or a server's
// own engine: client connections carry their own handler, so the two never
// see each other's traffic. The engine has to be running. Close the client
// before closing the engine: closing the engine drops its connections without
// telling the client, so requests on them are only answered by their timeout.
type Client struct {
	engine *fib.Engine
	config ClientConfig
	// tlsConfig is config.TLSConfig with the client's ALPN offer.
	tlsConfig *tls.Config
	mu        sync.Mutex
	hosts     map[hostTarget]*hostPool
	closed    bool
}

// NewClient returns a client that dials through engine. Zero limits in config
// are taken as written, so start from DefaultClientConfig to change only some.
func NewClient(engine *fib.Engine, config ClientConfig) *Client {
	defaults := DefaultClientConfig()
	if config.MaxResponseHeaderBytes <= 0 {
		config.MaxResponseHeaderBytes = defaults.MaxResponseHeaderBytes
	}
	if config.MaxResponseBodyBytes <= 0 {
		config.MaxResponseBodyBytes = defaults.MaxResponseBodyBytes
	}
	tlsConfig := config.TLSConfig
	if !config.DisableHTTP2 && (tlsConfig == nil || len(tlsConfig.NextProtos) == 0) {
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = tlsConfig.Clone()
		}
		tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	}
	return &Client{engine: engine, config: config, tlsConfig: tlsConfig, hosts: make(map[hostTarget]*hostPool)}
}

// Do sends req and calls callback exactly once with its response or the
// reason there is none. It does not wait for the network, though it does read
// req.Body, which must therefore not block for long.
//
// The response body is buffered whole and may be read after the callback
// returns; closing it is optional. Cancelling req's context abandons the
// request, as does the client's Timeout.
//
// callback may run on any goroutine: an engine worker for a response, which
// the callback then holds until it returns, a timer for a timeout or a
// cancellation, the caller's own for a request Do rejects outright, or one of
// its own for a connection that failed. It must not block the engine for long,
// and when the engine runs handlers inline it runs on the event loop itself.
func (c *Client) Do(req *stdhttp.Request, callback func(*stdhttp.Response, error)) {
	if callback == nil {
		callback = func(*stdhttp.Response, error) {}
	}
	target, err := requestTarget(req)
	if err != nil {
		callback(nil, err)
		return
	}
	// The body is read once and kept, since whether it travels in HTTP/1.1 or
	// HTTP/2 framing is only known once a connection has been chosen.
	var body []byte
	if req.Body != nil && req.Body != stdhttp.NoBody {
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			callback(nil, err)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	var buf bytes.Buffer
	if err = req.Write(&buf); err != nil {
		callback(nil, err)
		return
	}
	r := &clientRequest{req: req, data: buf.Bytes(), body: body, callback: callback}
	// Either hook can fire before it has been stored, so both are stored under
	// the lock finish reads them under.
	r.mu.Lock()
	if c.config.Timeout > 0 {
		r.timer = time.AfterFunc(c.config.Timeout, func() { r.abort(errRequestTimeout) })
	}
	if ctx := req.Context(); ctx.Done() != nil {
		r.stopContext = context.AfterFunc(ctx, func() { r.abort(ctx.Err()) })
	}
	r.mu.Unlock()
	c.enqueue(target, r, false)
}

// Future is a request in flight, for callers that would rather wait on it than
// be called back.
type Future struct {
	done chan struct{}
	resp *stdhttp.Response
	err  error
}

// Go sends req like Do and returns a Future for its outcome.
func (c *Client) Go(req *stdhttp.Request) *Future {
	f := &Future{done: make(chan struct{})}
	c.Do(req, func(resp *stdhttp.Response, err error) {
		f.resp, f.err = resp, err
		close(f.done)
	})
	return f
}

// Wait blocks until the response has arrived or the request has failed.
func (f *Future) Wait() (*stdhttp.Response, error) {
	<-f.done
	return f.resp, f.err
}

// Done is closed once Wait would no longer block, for use in a select.
func (f *Future) Done() <-chan struct{} { return f.done }

// Close stops the client taking requests. Requests still waiting for a
// connection fail with ErrClientClosed and idle connections are closed.
// Requests already on a connection finish as they would have.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	var idle []*clientConn
	var waiting []*clientRequest
	for _, h := range c.hosts {
		idle = append(idle, h.idle...)
		h.idle = nil
		waiting = append(waiting, h.waiting...)
		h.waiting = nil
		for _, cc := range h.h2 {
			if cc.streams == 0 {
				idle = append(idle, cc)
			}
		}
	}
	c.mu.Unlock()
	for _, cc := range idle {
		cc.discard()
	}
	for _, r := range waiting {
		r.finish(nil, ErrClientClosed)
	}
}

// hostTarget is where a request goes: the host:port to dial, and whether to
// speak TLS to it. Connections are pooled per target, so http and https to the
// same address never share one.
type hostTarget struct {
	addr   string
	secure bool
}

// requestTarget is where to send req.
func requestTarget(req *stdhttp.Request) (hostTarget, error) {
	if req.URL == nil {
		return hostTarget{}, errors.New("http: request has no URL")
	}
	var target hostTarget
	port := req.URL.Port()
	switch req.URL.Scheme {
	case "http":
		if port == "" {
			port = "80"
		}
	case "https":
		target.secure = true
		if port == "" {
			port = "443"
		}
	default:
		return hostTarget{}, fmt.Errorf("%w %q", ErrUnsupportedScheme, req.URL.Scheme)
	}
	host := req.URL.Hostname()
	if host == "" {
		return hostTarget{}, errors.New("http: request URL has no host")
	}
	target.addr = net.JoinHostPort(host, port)
	return target, nil
}

// hostPool is the client's state for one target. Guarded by Client.mu.
type hostPool struct {
	target hostTarget
	// open counts connections open or dialing, and dialing the second kind.
	open    int
	dialing int
	idle    []*clientConn
	waiting []*clientRequest
	// h2 holds the HTTP/2 connections that take new streams, busy or not.
	// multiplexed records that the host has spoken HTTP/2, and probed that a
	// connection to it has told which protocol it speaks. Until then, and
	// for good when it is HTTP/2, one dial at a time is enough.
	h2          []*clientConn
	multiplexed bool
	probed      bool
}

// assignment pairs a request with the connection that will carry it. They are
// collected under the client's lock and carried out after it is released.
type assignment struct {
	conn *clientConn
	req  *clientRequest
}

// enqueue queues r for target and starts whatever that makes possible. A
// retried request goes to the front, since it has already waited its turn once.
func (c *Client) enqueue(target hostTarget, r *clientRequest, retry bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		r.finish(nil, ErrClientClosed)
		return
	}
	h := c.hosts[target]
	if h == nil {
		h = &hostPool{target: target}
		c.hosts[target] = h
	}
	if retry {
		h.waiting = append([]*clientRequest{r}, h.waiting...)
	} else {
		h.waiting = append(h.waiting, r)
	}
	work, dials := c.dispatchLocked(h)
	c.mu.Unlock()
	c.carryOut(h, work, dials)
}

// dispatchLocked matches waiting requests with idle connections, and reports
// how many connections to dial for the requests left over.
func (c *Client) dispatchLocked(h *hostPool) (work []assignment, dials int) {
	for len(h.waiting) > 0 {
		r := h.waiting[0]
		if r.done.Load() {
			h.waiting = h.waiting[1:]
			continue
		}
		var cc *clientConn
		for _, m := range h.h2 {
			if m.streams < m.maxStreams {
				cc = m
				break
			}
		}
		if cc != nil {
			cc.streams++
		} else if n := len(h.idle); n > 0 {
			cc = h.idle[n-1]
			h.idle = h.idle[:n-1]
		} else {
			break
		}
		if cc.idleTimer != nil {
			cc.idleTimer.Stop()
			cc.idleTimer = nil
		}
		h.waiting = h.waiting[1:]
		work = append(work, assignment{conn: cc, req: r})
	}
	need := len(h.waiting) - h.dialing
	if h.multiplexed || !h.probed && c.mayMultiplex(h.target) {
		// The next connection may well be HTTP/2, and carry all of them.
		need = min(need, 1-h.dialing)
	}
	for ; need > 0; need-- {
		if c.config.MaxConnsPerHost > 0 && h.open >= c.config.MaxConnsPerHost {
			break
		}
		h.open++
		h.dialing++
		dials++
	}
	if len(h.waiting) == 0 {
		// Let the queue's array go instead of keeping the longest it has been.
		h.waiting = nil
	}
	return work, dials
}

// mayMultiplex reports whether a connection to target might speak HTTP/2.
func (c *Client) mayMultiplex(target hostTarget) bool {
	if !target.secure {
		return c.config.UnencryptedHTTP2
	}
	return c.tlsConfig != nil && slices.Contains(c.tlsConfig.NextProtos, "h2")
}

func (c *Client) carryOut(h *hostPool, work []assignment, dials int) {
	for _, a := range work {
		a.conn.send(a.req)
	}
	for ; dials > 0; dials-- {
		c.dial(h)
	}
}

func (c *Client) dial(h *hostPool) {
	cc := &clientConn{client: c, host: h}
	cc.parser.maxHeader = c.config.MaxResponseHeaderBytes
	cc.parser.maxBody = c.config.MaxResponseBodyBytes
	done := func(_ *fib.Connection, err error) { c.dialed(cc, err) }
	var err error
	if h.target.secure {
		// The dial is settled once the handshake has told which protocol to
		// speak, in OnHandshake, or failed, in OnClose.
		done = func(_ *fib.Connection, err error) {
			if err != nil {
				c.dialed(cc, err)
			}
		}
		err = fibtls.Dial(c.engine, "tcp", h.target.addr, c.config.DialTimeout, c.tlsConfig, cc, done)
	} else {
		if c.config.UnencryptedHTTP2 {
			done = func(_ *fib.Connection, err error) {
				if err == nil {
					cc.startH2()
				}
				c.dialed(cc, err)
			}
		}
		err = c.engine.DialWithHandler("tcp", h.target.addr, c.config.DialTimeout, cc, done)
	}
	if err != nil {
		c.dialed(cc, err)
	}
}

// dialed settles a dial. A new connection goes to work straight away; a failed
// dial fails the request that has waited longest, since it is the one most
// likely to have caused it, and the rest try again.
func (c *Client) dialed(cc *clientConn, err error) {
	h := cc.host
	cc.mu.Lock()
	settled := cc.settled
	cc.settled = true
	cc.mu.Unlock()
	if settled {
		return
	}
	c.mu.Lock()
	h.dialing--
	h.probed = h.probed || err == nil
	if err == nil && cc.h2 != nil {
		h.multiplexed = true
		if !cc.isClosed() {
			h.h2 = append(h.h2, cc)
		}
		work, dials := c.dispatchLocked(h)
		discard := c.idleH2Locked(cc)
		c.mu.Unlock()
		if discard {
			cc.discard()
		}
		c.carryOut(h, work, dials)
		return
	}
	if err == nil {
		c.mu.Unlock()
		c.release(cc)
		return
	}
	h.open--
	var failed *clientRequest
	for len(h.waiting) > 0 && failed == nil {
		if r := h.waiting[0]; !r.done.Load() {
			failed = r
		}
		h.waiting = h.waiting[1:]
	}
	work, dials := c.dispatchLocked(h)
	c.mu.Unlock()
	if failed != nil {
		// Dial callbacks run on the event loop, which a request's callback must
		// never be allowed to hold up.
		go failed.finish(nil, err)
	}
	c.carryOut(h, work, dials)
}

// release hands a connection with nothing in flight to the next waiting
// request, or keeps it idle, or closes it if there is room for neither.
func (c *Client) release(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	if cc.isClosed() {
		c.mu.Unlock()
		return
	}
	for len(h.waiting) > 0 {
		r := h.waiting[0]
		h.waiting = h.waiting[1:]
		if !r.done.Load() {
			c.mu.Unlock()
			cc.send(r)
			return
		}
	}
	keep := !c.closed && len(h.idle) < c.config.MaxIdleConnsPerHost
	if keep {
		h.idle = append(h.idle, cc)
		if c.config.IdleConnTimeout > 0 {
			cc.idleTimer = time.AfterFunc(c.config.IdleConnTimeout, func() { c.expireIdle(cc) })
		}
	}
	c.mu.Unlock()
	if !keep {
		cc.discard()
	}
}

// expireIdle closes a connection that sat idle for IdleConnTimeout, unless a
// request took it in the meantime.
func (c *Client) expireIdle(cc *clientConn) {
	c.mu.Lock()
	var idle bool
	if cc.h2 != nil {
		idle = cc.streams == 0 && cc.idleTimer != nil && removeConn(&cc.host.h2, cc)
	} else {
		idle = removeConn(&cc.host.idle, cc)
	}
	c.mu.Unlock()
	if idle {
		cc.discard()
	}
}

// streamDone gives back the stream an HTTP/2 connection carried, and hands
// the room to a waiting request.
func (c *Client) streamDone(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	cc.streams--
	work, dials := c.dispatchLocked(h)
	discard := c.idleH2Locked(cc)
	c.mu.Unlock()
	if discard {
		cc.discard()
	}
	c.carryOut(h, work, dials)
}

// idleH2Locked starts the idle clock on an HTTP/2 connection left with no
// streams, or reports that it should be closed because the client keeps no
// more idle connections than MaxIdleConnsPerHost.
func (c *Client) idleH2Locked(cc *clientConn) bool {
	h := cc.host
	if cc.streams > 0 || cc.idleTimer != nil {
		return false
	}
	idle := 0
	for _, m := range h.h2 {
		if m.streams == 0 {
			idle++
		}
	}
	if c.closed || idle > c.config.MaxIdleConnsPerHost {
		return removeConn(&h.h2, cc)
	}
	if c.config.IdleConnTimeout > 0 {
		cc.idleTimer = time.AfterFunc(c.config.IdleConnTimeout, func() { c.expireIdle(cc) })
	}
	return false
}

// retire stops an HTTP/2 connection taking new streams, once the server has
// said it is going away.
func (c *Client) retire(cc *clientConn) {
	c.mu.Lock()
	removeConn(&cc.host.h2, cc)
	c.mu.Unlock()
}

// forget drops a closed connection from its host and lets waiting requests
// have its place.
func (c *Client) forget(cc *clientConn) {
	h := cc.host
	c.mu.Lock()
	removeConn(&h.idle, cc)
	removeConn(&h.h2, cc)
	if cc.idleTimer != nil {
		cc.idleTimer.Stop()
		cc.idleTimer = nil
	}
	h.open--
	work, dials := c.dispatchLocked(h)
	c.mu.Unlock()
	c.carryOut(h, work, dials)
}

func removeConn(conns *[]*clientConn, cc *clientConn) bool {
	for i, other := range *conns {
		if other == cc {
			*conns = append((*conns)[:i], (*conns)[i+1:]...)
			return true
		}
	}
	return false
}

// clientRequest is one call to Do, from the queue to its callback.
type clientRequest struct {
	req *stdhttp.Request
	// data is the request in HTTP/1.1 framing, and body its body alone, which
	// is what HTTP/2 sends after the header block.
	data     []byte
	body     []byte
	callback func(*stdhttp.Response, error)
	done     atomic.Bool
	// retried records that the request has already been sent again once, after
	// a reused connection turned out to be closed.
	retried bool
	// streamID is the HTTP/2 stream carrying the request, guarded by that
	// connection's lock.
	streamID uint32
	mu       sync.Mutex
	// conn is the connection carrying the request, so that abandoning the
	// request can also abandon its half-finished exchange. timer and
	// stopContext are the hooks that abandon it. Guarded by mu.
	conn        *clientConn
	timer       *time.Timer
	stopContext func() bool
}

// finish reports the outcome, once: whichever of response, failure, timeout
// and cancellation comes first is the one the callback hears.
func (r *clientRequest) finish(resp *stdhttp.Response, err error) bool {
	if !r.done.CompareAndSwap(false, true) {
		return false
	}
	r.mu.Lock()
	timer, stopContext := r.timer, r.stopContext
	r.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if stopContext != nil {
		stopContext()
	}
	r.callback(resp, err)
	return true
}

// abort fails the request from outside the exchange. A connection left in the
// middle of it cannot carry another request, so it is closed.
func (r *clientRequest) abort(err error) {
	if !r.finish(nil, err) {
		return
	}
	r.mu.Lock()
	cc := r.conn
	r.conn = nil
	r.mu.Unlock()
	if cc == nil {
		return
	}
	if cc.h2 != nil {
		// Only this request's stream is abandoned, not the connection.
		cc.h2.cancel(r)
		return
	}
	cc.discard()
}

// attach records the connection carrying the request. It reports false if the
// request has already been settled, in which case it must not be sent.
func (r *clientRequest) attach(cc *clientConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done.Load() {
		return false
	}
	r.conn = cc
	return true
}

func (r *clientRequest) detach() {
	r.mu.Lock()
	r.conn = nil
	r.mu.Unlock()
}

// retryable reports whether a request that may have reached the server can be
// sent again without the server acting on it twice.
func (r *clientRequest) retryable() bool {
	switch r.req.Method {
	case "", stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodOptions, stdhttp.MethodTrace:
		return !r.retried
	}
	return false
}

// clientConn is one connection the client dialed. It is the connection's
// handler, so everything the engine reports about it arrives here.
type clientConn struct {
	client *Client
	host   *hostPool
	conn   *fib.Connection
	// parser is used only by the worker reading the connection.
	parser responseParser
	mu     sync.Mutex
	// current is the request in flight, if any. sent records that the
	// connection has carried a request before this one, and received that the
	// server has answered this one with at least a byte.
	current  *clientRequest
	sent     bool
	reused   bool
	received bool
	closed   bool
	// idleTimer is armed while the connection is idle. Guarded by Client.mu.
	idleTimer *time.Timer
	// settled records that the dial has been reported to the client, which
	// for TLS happens once the handshake is done. Guarded by mu.
	settled bool
	// h2 is set once the connection is known to speak HTTP/2, before it
	// carries anything, and never changes after. streams counts the streams
	// it carries or has been given, and maxStreams how many the server
	// allows. Both are guarded by Client.mu.
	h2         *h2ClientConn
	streams    int
	maxStreams int
}

func (cc *clientConn) isClosed() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.closed
}

// send puts r on the connection. A connection that closed in the meantime
// hands r back to the queue.
func (cc *clientConn) send(r *clientRequest) {
	if cc.h2 != nil {
		cc.h2.send(r)
		return
	}
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		cc.client.enqueue(cc.host.target, r, true)
		return
	}
	cc.current = r
	cc.reused = cc.sent
	cc.sent = true
	cc.received = false
	cc.parser.reset()
	cc.mu.Unlock()
	if !r.attach(cc) {
		// Settled while it waited: give the connection to the next request.
		cc.mu.Lock()
		cc.current = nil
		cc.mu.Unlock()
		cc.client.release(cc)
		return
	}
	// A failed send closes the connection, and OnClose then settles r.
	_ = cc.conn.Send(r.data)
}

// discard closes the connection without handing it back.
func (cc *clientConn) discard() {
	cc.mu.Lock()
	cc.closed = true
	cc.current = nil
	cc.mu.Unlock()
	cc.conn.Close()
}

func (cc *clientConn) OnOpen(conn *fib.Connection) { cc.conn = conn }

func (cc *clientConn) OnPriorityData(*fib.Connection, []byte) {}

// OnHandshake settles a TLS dial, with the protocol ALPN chose.
func (cc *clientConn) OnHandshake(_ *fib.Connection, state tls.ConnectionState) {
	if state.NegotiatedProtocol == "h2" {
		cc.startH2()
	}
	cc.client.dialed(cc, nil)
}

func (cc *clientConn) OnData(conn *fib.Connection, data []byte) {
	if cc.h2 != nil {
		cc.h2.feed(data)
		return
	}
	cc.mu.Lock()
	r := cc.current
	closed := cc.closed
	cc.received = true
	cc.mu.Unlock()
	if closed {
		return
	}
	if r == nil {
		// Nothing was asked on this connection, so these bytes answer nothing
		// and the stream can no longer be trusted.
		cc.discard()
		return
	}
	resp, err := cc.parser.feed(data, r.req)
	if err != nil {
		r.detach()
		cc.discard()
		r.finish(nil, err)
		return
	}
	if resp == nil {
		return
	}
	// Bytes past the response, a protocol switch, or either side asking to
	// close all mean the connection cannot carry another request.
	reusable := !resp.Close && !r.req.Close && !cc.parser.buffered() &&
		resp.StatusCode != stdhttp.StatusSwitchingProtocols
	cc.mu.Lock()
	cc.current = nil
	cc.mu.Unlock()
	r.detach()
	if reusable {
		cc.client.release(cc)
	} else {
		cc.discard()
	}
	r.finish(resp, nil)
}

func (cc *clientConn) OnClose(_ *fib.Connection, err error) {
	cc.mu.Lock()
	settled := cc.settled
	cc.mu.Unlock()
	if !settled {
		// A TLS handshake that failed: the dial failed.
		cc.mu.Lock()
		cc.closed = true
		cc.mu.Unlock()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		cc.client.dialed(cc, err)
		return
	}
	if cc.h2 != nil {
		cc.mu.Lock()
		cc.closed = true
		cc.mu.Unlock()
		cc.client.forget(cc)
		cc.h2.shutdown(err)
		return
	}
	cc.mu.Lock()
	r := cc.current
	cc.current = nil
	cc.closed = true
	retry := r != nil && cc.reused && !cc.received
	cc.mu.Unlock()
	cc.client.forget(cc)
	if r == nil {
		return
	}
	r.detach()
	if resp := cc.parser.finish(); resp != nil && (err == nil || err == io.EOF) {
		go r.finish(resp, nil)
		return
	}
	if retry && r.retryable() {
		// A kept connection the server had already closed. Nothing came back,
		// so a request that is safe to repeat is sent again on another.
		r.retried = true
		cc.client.enqueue(cc.host.target, r, true)
		return
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	// OnClose runs on the event loop, which the callback must not hold up.
	go r.finish(nil, err)
}
