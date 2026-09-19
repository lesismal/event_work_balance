//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib/go"
)

var (
	// ErrUnsupportedScheme is what a dial to anything but ws:// gets.
	ErrUnsupportedScheme = errors.New("websocket: unsupported URL scheme")
	// ErrBadHandshake is what a dial gets when the server does not complete the
	// opening handshake. If the server answered at all, done also receives its
	// response, so the status and headers of a refusal can be inspected.
	ErrBadHandshake = errors.New("websocket: bad handshake")
	// errHandshakeTimeout is what a dial gets when DialerConfig.HandshakeTimeout
	// runs out. errors.Is matches it against os.ErrDeadlineExceeded.
	errHandshakeTimeout = fmt.Errorf("websocket: handshake timed out: %w", os.ErrDeadlineExceeded)
)

// DialerConfig bounds the opening handshake and the messages that follow it.
type DialerConfig struct {
	// HandshakeTimeout bounds a dial from the call until the server's
	// handshake response has arrived: connecting counts. Zero means no limit.
	HandshakeTimeout time.Duration
	// Subprotocols are offered to the server in order of preference. The
	// server's choice, if it makes one, is Connection.Subprotocol.
	Subprotocols []string
	// MaxMessageBytes bounds one message from the server, reassembled.
	MaxMessageBytes int64
	// MaxHandshakeBytes bounds the server's handshake response header.
	MaxHandshakeBytes int
	// EnableCompression offers the server permessage-deflate (RFC 7692). If
	// the server accepts, messages are sent compressed and may arrive so.
	EnableCompression bool
}

func DefaultDialerConfig() DialerConfig {
	return DialerConfig{HandshakeTimeout: 10 * time.Second, MaxMessageBytes: 16 << 20, MaxHandshakeBytes: 1 << 20}
}

// Dialer opens client WebSocket connections over an engine without blocking
// the caller. Once open, a client connection is served exactly as a server one
// is: the same Handler callbacks, run by the engine's workers, and the same
// Connection methods, which mask what the client sends as RFC 6455 requires.
//
// The engine may be one made by fib.NewEngine for clients alone, or a server's
// own: each client connection carries its own handler. The engine has to be
// running. Only ws:// is supported.
type Dialer struct {
	engine *fib.Engine
	config DialerConfig
}

// NewDialer returns a dialer for engine. Zero limits in config are replaced by
// their defaults, but a zero HandshakeTimeout means no timeout.
func NewDialer(engine *fib.Engine, config DialerConfig) *Dialer {
	defaults := DefaultDialerConfig()
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if config.MaxHandshakeBytes <= 0 {
		config.MaxHandshakeBytes = defaults.MaxHandshakeBytes
	}
	return &Dialer{engine: engine, config: config}
}

// Dial connects to rawURL, performs the opening handshake and reports the
// outcome to done exactly once. header carries extra handshake headers, such as
// Origin or credentials; the ones the handshake itself sets may not be in it.
//
// On success handler's OnOpen runs with the handshake request, then done with
// the connection and the server's response, and handler then receives the
// connection's messages and its close. On failure done alone runs, with a nil
// connection, the server's response if it sent one, and the error.
//
// done may run on any goroutine: an engine worker for a completed handshake,
// a timer for a timeout, the caller's own for a URL Dial rejects outright, or
// one of its own for a connection that failed. It should not block for long.
func (d *Dialer) Dial(rawURL string, header stdhttp.Header, handler Handler,
	done func(*Connection, *stdhttp.Response, error)) {
	if done == nil {
		done = func(*Connection, *stdhttp.Response, error) {}
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	req, addr, key, err := d.handshakeRequest(rawURL, header)
	if err != nil {
		done(nil, nil, err)
		return
	}
	var buf bytes.Buffer
	if err = req.Write(&buf); err != nil {
		done(nil, nil, err)
		return
	}
	cc := &clientConn{dialer: d, handler: handler, done: done, req: req, request: buf.Bytes()}
	websocketAccept(cc.accept[:], []byte(key))
	cc.mu.Lock()
	if d.config.HandshakeTimeout > 0 {
		cc.timer = time.AfterFunc(d.config.HandshakeTimeout, func() { cc.fail(nil, errHandshakeTimeout, false) })
	}
	cc.mu.Unlock()
	err = d.engine.DialWithHandler("tcp", addr, d.config.HandshakeTimeout, cc, func(conn *fib.Connection, err error) {
		if err != nil {
			cc.fail(nil, err, true)
			return
		}
		// A failed send closes the connection, and OnClose fails the dial.
		_ = conn.SendOwned(cc.request)
	})
	if err != nil {
		cc.fail(nil, err, false)
	}
}

// DialFuture is a dial in progress, for callers that would rather wait on it
// than be called back.
type DialFuture struct {
	done chan struct{}
	conn *Connection
	resp *stdhttp.Response
	err  error
}

// Go dials like Dial and returns a DialFuture for the outcome.
func (d *Dialer) Go(rawURL string, header stdhttp.Header, handler Handler) *DialFuture {
	f := &DialFuture{done: make(chan struct{})}
	d.Dial(rawURL, header, handler, func(conn *Connection, resp *stdhttp.Response, err error) {
		f.conn, f.resp, f.err = conn, resp, err
		close(f.done)
	})
	return f
}

// Wait blocks until the handshake has completed or failed.
func (f *DialFuture) Wait() (*Connection, *stdhttp.Response, error) {
	<-f.done
	return f.conn, f.resp, f.err
}

// Done is closed once Wait would no longer block, for use in a select.
func (f *DialFuture) Done() <-chan struct{} { return f.done }

// reservedHeaders are the handshake headers the dialer sets itself.
var reservedHeaders = []string{"Upgrade", "Connection", "Sec-Websocket-Key", "Sec-Websocket-Version",
	"Sec-Websocket-Extensions", "Sec-Websocket-Protocol"}

// handshakeRequest builds the opening handshake for rawURL, and reports the
// address to dial and the key the server's answer must be derived from.
func (d *Dialer) handshakeRequest(rawURL string, header stdhttp.Header) (*stdhttp.Request, string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", "", err
	}
	if u.Scheme != "ws" {
		return nil, "", "", fmt.Errorf("%w %q", ErrUnsupportedScheme, u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return nil, "", "", errors.New("websocket: URL has no host")
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	for _, name := range reservedHeaders {
		if len(header.Values(name)) != 0 {
			return nil, "", "", fmt.Errorf("websocket: the dialer sets the %s header itself", name)
		}
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, "", "", err
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])
	h := header.Clone()
	if h == nil {
		h = make(stdhttp.Header)
	}
	h.Set("Upgrade", "websocket")
	h.Set("Connection", "Upgrade")
	h.Set("Sec-WebSocket-Key", key)
	h.Set("Sec-WebSocket-Version", "13")
	for _, protocol := range d.config.Subprotocols {
		h.Add("Sec-WebSocket-Protocol", protocol)
	}
	if d.config.EnableCompression {
		h.Set("Sec-WebSocket-Extensions", deflateOffer)
	}
	// The request goes on the wire as http://, which is what the handshake is.
	target := *u
	target.Scheme = "http"
	req := &stdhttp.Request{Method: stdhttp.MethodGet, URL: &target, Proto: "HTTP/1.1", ProtoMajor: 1,
		ProtoMinor: 1, Header: h, Host: u.Host}
	return req, net.JoinHostPort(host, port), key, nil
}

// clientConn is one client connection, from the dial through the handshake
// and then for as long as it lasts. It is the connection's handler, so every
// callback the engine makes for it arrives here.
type clientConn struct {
	dialer  *Dialer
	handler Handler
	done    func(*Connection, *stdhttp.Response, error)
	req     *stdhttp.Request
	request []byte
	accept  [28]byte
	// settled records that done has run, or is about to: whichever of the
	// handshake, a failure and the timeout claims it first is the outcome.
	settled atomic.Bool
	// upgraded is set once the handshake has succeeded, after which the engine's
	// callbacks are the WebSocket connection's.
	upgraded atomic.Bool
	mu       sync.Mutex
	// conn and timer are guarded by mu until upgraded.
	conn  *fib.Connection
	timer *time.Timer
	// response holds the handshake response while it arrives. Only the worker
	// reading the connection touches it.
	response []byte
	ws       Connection
	parser   Parser
}

// claim decides the outcome of the dial. It reports false if something else
// already has.
func (cc *clientConn) claim() bool {
	if !cc.settled.CompareAndSwap(false, true) {
		return false
	}
	cc.mu.Lock()
	timer := cc.timer
	cc.timer = nil
	cc.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	return true
}

// fail ends a dial that did not produce a connection. onLoop says the caller is
// the event loop, which done must never be allowed to hold up.
func (cc *clientConn) fail(resp *stdhttp.Response, err error, onLoop bool) {
	if !cc.claim() {
		return
	}
	cc.mu.Lock()
	conn := cc.conn
	cc.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	if onLoop {
		go cc.done(nil, resp, err)
	} else {
		cc.done(nil, resp, err)
	}
}

func (cc *clientConn) OnOpen(conn *fib.Connection) {
	cc.mu.Lock()
	cc.conn = conn
	cc.mu.Unlock()
	// A dial that timed out while still connecting found no connection to
	// close, so the connection closes itself on arrival. fail marks the dial
	// settled before it looks for the connection, and this stores the
	// connection before it looks at the mark, so one of the two always sees
	// the other.
	if cc.settled.Load() {
		conn.Close()
	}
}

func (cc *clientConn) OnPriorityData(*fib.Connection, []byte) {}

func (cc *clientConn) OnData(conn *fib.Connection, data []byte) {
	if cc.upgraded.Load() {
		serveFrames(cc.handler, &cc.ws, &cc.parser, data)
		return
	}
	if cc.settled.Load() {
		// The dial has already failed; the connection is on its way out.
		return
	}
	cc.response = append(cc.response, data...)
	headerAt := bytes.Index(cc.response, []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(cc.response) > cc.dialer.config.MaxHandshakeBytes {
			cc.fail(nil, fmt.Errorf("%w: response header too large", ErrBadHandshake), false)
		}
		return
	}
	headerEnd := headerAt + 4
	resp, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(cc.response[:headerEnd])), cc.req)
	if err != nil {
		cc.fail(nil, fmt.Errorf("%w: %v", ErrBadHandshake, err), false)
		return
	}
	_ = resp.Body.Close()
	resp.Body = stdhttp.NoBody
	subprotocol, deflate, compress, err := cc.checkResponse(resp)
	if err != nil {
		cc.fail(resp, err, false)
		return
	}
	if !cc.claim() {
		return
	}
	// Frames the server sent straight after its response came in the same
	// read; they are the connection's first.
	remainder := cc.response[headerEnd:]
	cc.response = nil
	cc.ws = Connection{conn: conn, subprotocol: subprotocol, client: true, compress: compress,
		windowBits: deflate.windowBits}
	cc.parser = Parser{maxMessageBytes: cc.dialer.config.MaxMessageBytes, fromServer: true, deflate: compress,
		contextTakeover: compress && deflate.peerContextTakeover}
	cc.upgraded.Store(true)
	cc.handler.OnOpen(&cc.ws, cc.req)
	cc.done(&cc.ws, resp, nil)
	if len(remainder) != 0 {
		serveFrames(cc.handler, &cc.ws, &cc.parser, remainder)
	}
}

// checkResponse confirms that the server switched protocols, answered this
// handshake's own key, and chose nothing that was not offered. compress
// reports that the server accepted permessage-deflate, with deflate its
// parameters.
func (cc *clientConn) checkResponse(resp *stdhttp.Response) (subprotocol string, deflate deflateParams, compress bool, err error) {
	extensions := resp.Header.Values("Sec-Websocket-Extensions")
	switch {
	case resp.StatusCode != stdhttp.StatusSwitchingProtocols:
		return "", deflateParams{}, false, fmt.Errorf("%w: status %s", ErrBadHandshake, resp.Status)
	case !headerHasToken(resp.Header, "Upgrade", "websocket"):
		return "", deflateParams{}, false, fmt.Errorf("%w: missing Upgrade: websocket", ErrBadHandshake)
	case !headerHasToken(resp.Header, "Connection", "upgrade"):
		return "", deflateParams{}, false, fmt.Errorf("%w: missing Connection: Upgrade", ErrBadHandshake)
	case resp.Header.Get("Sec-Websocket-Accept") != string(cc.accept[:]):
		return "", deflateParams{}, false, fmt.Errorf("%w: Sec-WebSocket-Accept does not match the key", ErrBadHandshake)
	case len(extensions) != 0 && !cc.dialer.config.EnableCompression:
		return "", deflateParams{}, false, fmt.Errorf("%w: server chose an extension that was not offered", ErrBadHandshake)
	}
	if len(extensions) != 0 {
		var ok bool
		if deflate, compress, ok = acceptDeflateResponse(extensions); !ok {
			return "", deflateParams{}, false, fmt.Errorf("%w: server answered permessage-deflate with %q",
				ErrBadHandshake, strings.Join(extensions, ", "))
		}
	}
	subprotocol = resp.Header.Get("Sec-Websocket-Protocol")
	if subprotocol != "" && !slices.Contains(cc.dialer.config.Subprotocols, subprotocol) {
		return "", deflateParams{}, false, fmt.Errorf("%w: server chose subprotocol %q, which was not offered", ErrBadHandshake, subprotocol)
	}
	return subprotocol, deflate, compress, nil
}

func (cc *clientConn) OnClose(_ *fib.Connection, err error) {
	if cc.upgraded.Load() {
		code, reason := cc.ws.closeStatus()
		cc.handler.OnClose(&cc.ws, code, reason, err)
		return
	}
	if err == nil || err == io.EOF {
		err = fmt.Errorf("%w: connection closed before the handshake completed", ErrBadHandshake)
	}
	cc.fail(nil, err, true)
}

var _ fib.Handler = (*clientConn)(nil)
