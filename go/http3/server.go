//go:build linux || darwin || windows

// Package http3 serves and sends HTTP/3 (RFC 9114) over the fib engine's UDP
// sockets, with QUIC (RFC 9000) and QPACK (RFC 9204) implemented here and
// no dependencies beyond the standard library.
//
// A server is a UDP engine whose handler is NewHandler's. Requests go to the
// same http.Handler that serves HTTP/1 and HTTP/2 in package http, answered
// through the same Context:
//
//	config := fib.DefaultConfig()
//	config.Network = "udp"
//	config.Addr = ":443"
//	engine, err := fib.Bind(config, http3.NewHandler(tlsConfig, handler))
//
// The engine demultiplexes datagrams by peer address, and each address that
// sends a QUIC Initial gets a connection of its own; connection migration is
// therefore not supported, and the server says so to its clients.
//
// Browsers find an HTTP/3 server through the Alt-Svc header of a response
// over HTTP/1 or HTTP/2; AltSvc builds its value.
package http3

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	fib "github.com/lesismal/fib/go"
	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/http3/internal/qpack"
	"github.com/lesismal/fib/go/http3/internal/quic"
)

// NextProto is the ALPN protocol ID of HTTP/3.
const NextProto = "h3"

// Config configures a server.
type Config struct {
	// TLSConfig is required, with the server's certificates. HTTP/3 needs
	// TLS 1.3 and the "h3" ALPN protocol, which the handler sees to.
	TLSConfig *tls.Config
	// MaxHeaderBytes bounds a request's header section, decoded and counted
	// as RFC 9114 section 4.2.2 does; the server also tells clients so.
	MaxHeaderBytes int
	// MaxBodyBytes bounds a request body, which is read whole before the
	// handler runs.
	MaxBodyBytes int64
	// MaxConcurrentStreams is how many requests a client may have open on
	// one connection.
	MaxConcurrentStreams uint64
	// MaxIdleTimeout closes a connection that has been silent this long.
	// Keep it below the engine's UDPIdleTimeout, or the engine closes a
	// quiet peer first.
	MaxIdleTimeout time.Duration
}

// DefaultConfig returns the defaults, which are what zero values in a Config
// mean too.
func DefaultConfig() Config {
	return Config{
		MaxHeaderBytes:       1 << 20,
		MaxBodyBytes:         16 << 20,
		MaxConcurrentStreams: 100,
		MaxIdleTimeout:       30 * time.Second,
	}
}

// ConfigureTLS returns a copy of config offering "h3" through ALPN, which a
// QUIC handshake has to agree on, and requiring TLS 1.3, which QUIC is built
// on.
func ConfigureTLS(config *tls.Config) *tls.Config {
	if config == nil {
		config = &tls.Config{}
	}
	config = config.Clone()
	protos := []string{NextProto}
	for _, p := range config.NextProtos {
		if p != NextProto {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
	config.MinVersion = max(config.MinVersion, tls.VersionTLS13)
	return config
}

// AltSvc is the Alt-Svc header value that tells a client, on a response it
// gets over HTTP/1 or HTTP/2, that the same origin speaks HTTP/3 on port.
func AltSvc(port int) string {
	return fmt.Sprintf(`%s=":%d"; ma=86400`, NextProto, port)
}

// ServerHandler serves HTTP/3 on a UDP engine. It is the engine's
// fib.Handler, and each peer's QUIC connection hangs off the peer's
// fib.Connection.
type ServerHandler struct {
	handler  fibhttp.Handler
	config   Config
	quic     quic.Config
	resetKey quic.ResetKey
}

// NewHandler serves handler over HTTP/3 with the default limits.
func NewHandler(tlsConfig *tls.Config, handler fibhttp.Handler) *ServerHandler {
	config := DefaultConfig()
	config.TLSConfig = tlsConfig
	return NewHandlerWithConfig(config, handler)
}

// NewHandlerWithConfig serves handler over HTTP/3.
func NewHandlerWithConfig(config Config, handler fibhttp.Handler) *ServerHandler {
	defaults := DefaultConfig()
	if config.MaxHeaderBytes <= 0 {
		config.MaxHeaderBytes = defaults.MaxHeaderBytes
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if config.MaxConcurrentStreams == 0 {
		config.MaxConcurrentStreams = defaults.MaxConcurrentStreams
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = defaults.MaxIdleTimeout
	}
	if handler == nil {
		handler = fibhttp.HandlerFunc(func(c *fibhttp.Context, _ *stdhttp.Request) {
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		})
	}
	h := &ServerHandler{handler: handler, config: config, resetKey: quic.NewResetKey()}
	h.quic = quic.Config{
		TLSConfig:          ConfigureTLS(config.TLSConfig),
		MaxIdleTimeout:     config.MaxIdleTimeout,
		MaxIncomingStreams: config.MaxConcurrentStreams,
		// The client's control and QPACK streams, with room to spare for
		// extensions it may try.
		MaxIncomingUniStreams: 16,
		ResetKey:              &h.resetKey,
	}
	return h
}

func (h *ServerHandler) OnOpen(*fib.Connection)                 {}
func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}

// OnData hands a datagram to its peer's connection, starting one if it is a
// client's first. The engine hands each UDP datagram over as a copy of its
// own, which QUIC keeps without copying again while it waits for what comes
// before it.
func (h *ServerHandler) OnData(c *fib.Connection, data []byte) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.HandleDatagram(data)
		return
	}
	if vn := quic.VersionNegotiation(data); vn != nil {
		_ = c.Send(vn)
		c.Close()
		return
	}
	if !quic.IsInitial(data) {
		// A packet for a connection this server no longer has, perhaps from
		// before a restart: tell the client, so that it need not time out.
		if reset := h.resetKey.StatelessReset(data); reset != nil {
			_ = c.Send(reset)
		}
		c.Close()
		return
	}
	sc := &serverConn{h: h, conn: c, streams: make(map[uint64]*requestStream)}
	if addr := c.RemoteAddr(); addr != nil {
		sc.remoteAddr = addr.String()
	}
	sc.peer.onGoAway = func(uint64) error { return nil }
	if tc := h.quic.TLSConfig; tc == nil || len(tc.Certificates) == 0 && tc.GetCertificate == nil && tc.GetConfigForClient == nil {
		c.Close()
		return
	}
	qc := quic.Accept(c, c.RemoteAddr(), h.quic, sc, data)
	if qc == nil {
		c.Close()
		return
	}
	c.SetAttachment(qc)
}

// OnClose ends the peer's connection with its path.
func (h *ServerHandler) OnClose(c *fib.Connection, err error) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		if err == nil {
			err = net.ErrClosed
		}
		qc.Abort(err)
	}
	c.SetAttachment(nil)
}

var _ fib.Handler = (*ServerHandler)(nil)

// serverConn is one client's HTTP/3 connection.
type serverConn struct {
	h          *ServerHandler
	conn       *fib.Connection
	qc         *quic.Conn
	remoteAddr string
	tlsState   *tls.ConnectionState
	control    *quic.Stream
	peer       peerStreams

	mu sync.Mutex
	// streams are the requests not yet answered.
	streams map[uint64]*requestStream
	// nextID is the stream ID after the highest request seen, which is
	// what GOAWAY names.
	nextID    uint64
	goingAway bool
	goAwayID  uint64
}

func (sc *serverConn) OnHandshake(qc *quic.Conn) {
	sc.qc = qc
	state := qc.ConnectionState()
	sc.tlsState = &state
	control, err := openControl(qc, sc.h.config.MaxHeaderBytes)
	if err != nil {
		closeWith(qc, connErr(ErrCodeStreamCreationError, "cannot open the control stream"))
		return
	}
	sc.mu.Lock()
	sc.control = control
	sc.mu.Unlock()
}

func (sc *serverConn) fail(err error) { closeWith(sc.qc, err) }

func (sc *serverConn) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	switch st := s.Context.(type) {
	case *requestStream:
		st.feed(data, fin)
	case *uniStream:
		st.feed(data, fin)
	case nil:
		if s.Bidirectional() {
			rs := sc.newRequestStream(s)
			s.Context = rs
			if rs != nil {
				rs.feed(data, fin)
			}
			return
		}
		u := newUniStream(&sc.peer, s, sc.fail)
		s.Context = u
		u.feed(data, fin)
	}
}

// newRequestStream starts reading a request, or refuses it once the
// connection is going away.
func (sc *serverConn) newRequestStream(s *quic.Stream) *requestStream {
	sc.mu.Lock()
	id := s.ID()
	if sc.goingAway && id >= sc.goAwayID {
		sc.mu.Unlock()
		abortStream(s, ErrCodeRequestRejected)
		return nil
	}
	sc.nextID = max(sc.nextID, id+4)
	sc.mu.Unlock()
	return &requestStream{sc: sc, s: s, declared: -1,
		parser: frameParser{maxFrame: uint64(sc.h.config.MaxHeaderBytes)}}
}

func (sc *serverConn) OnStreamReset(s *quic.Stream, _ uint64) {
	switch st := s.Context.(type) {
	case *requestStream:
		st.peerReset()
	case *uniStream:
		if st.typ == streamControl || st.typ == streamQPACKEncoder || st.typ == streamQPACKDecoder {
			sc.fail(connErr(ErrCodeClosedCriticalStream, "critical stream reset"))
		}
	}
}

func (sc *serverConn) OnStopSending(s *quic.Stream, _ uint64) {
	if st, ok := s.Context.(*requestStream); ok {
		st.peerReset()
	}
}

func (sc *serverConn) OnStreamsAvailable(*quic.Conn) {}

func (sc *serverConn) OnClose(*quic.Conn, error) {
	sc.mu.Lock()
	streams := sc.streams
	sc.streams = nil
	sc.mu.Unlock()
	for _, rs := range streams {
		rs.mu.Lock()
		rs.closed = true
		rs.mu.Unlock()
	}
}

// track records a request as open until it is answered.
func (sc *serverConn) track(rs *requestStream) {
	sc.mu.Lock()
	if sc.streams != nil {
		sc.streams[rs.s.ID()] = rs
	}
	sc.mu.Unlock()
}

// untrack forgets an answered or abandoned request, and finishes going away
// once the last one is gone.
func (sc *serverConn) untrack(rs *requestStream) {
	sc.mu.Lock()
	delete(sc.streams, rs.s.ID())
	done := sc.goingAway && len(sc.streams) == 0
	sc.mu.Unlock()
	if done {
		sc.qc.CloseWhenDone(uint64(ErrCodeNoError), "")
	}
}

// goAway retires the connection gracefully: the client is told which
// requests will be served, those finish, and then the connection closes.
func (sc *serverConn) goAway() {
	sc.mu.Lock()
	if sc.goingAway {
		sc.mu.Unlock()
		return
	}
	sc.goingAway = true
	sc.goAwayID = sc.nextID
	control := sc.control
	sc.mu.Unlock()
	if control != nil {
		payload := quic.AppendVarint(nil, sc.goAwayID)
		_ = control.Write(append(appendFrameHeader(nil, frameGoAway, len(payload)), payload...), false)
	}
}

// requestStream is a request being read and then answered.
type requestStream struct {
	sc       *serverConn
	s        *quic.Stream
	parser   frameParser
	req      *stdhttp.Request
	body     []byte
	declared int64
	trailers bool
	// done is set once the request needs no more reading: it has gone to
	// the handler or been refused.
	done bool
	// remoteDone is set when the whole request has arrived.
	remoteDone bool

	mu        sync.Mutex
	responded bool
	closed    bool
}

func (rs *requestStream) feed(data []byte, fin bool) {
	if rs.done {
		return
	}
	if err := rs.parser.feed(data, rs.onData, rs.onFrame); err != nil {
		rs.sc.fail(err)
		return
	}
	if !fin || rs.done {
		return
	}
	rs.remoteDone = true
	if rs.parser.midFrame() {
		rs.sc.fail(connErr(ErrCodeFrameError, "request stream ended inside a frame"))
		return
	}
	if rs.req == nil {
		rs.abort(ErrCodeRequestIncomplete)
		return
	}
	rs.finish()
}

func (rs *requestStream) onData(chunk []byte) error {
	if rs.done {
		return nil
	}
	if rs.req == nil || rs.trailers {
		return connErr(ErrCodeFrameUnexpected, "DATA before HEADERS or after trailers")
	}
	if int64(len(rs.body)+len(chunk)) > rs.sc.h.config.MaxBodyBytes {
		rs.reject(stdhttp.StatusRequestEntityTooLarge)
		return nil
	}
	rs.body = append(rs.body, chunk...)
	return nil
}

func (rs *requestStream) onFrame(typ uint64, payload []byte) error {
	if rs.done {
		return nil
	}
	switch typ {
	case frameHeaders:
	case frameCancelPush, frameSettings, framePushPromise, frameGoAway, frameMaxPushID:
		return connErr(ErrCodeFrameUnexpected, "control frame on a request stream")
	default:
		if reservedFrame(typ) {
			return connErr(ErrCodeFrameUnexpected, "HTTP/2 frame type")
		}
		return nil
	}
	if rs.trailers {
		return connErr(ErrCodeFrameUnexpected, "HEADERS after trailers")
	}
	fields, err := decodeFields(payload, rs.sc.h.config.MaxHeaderBytes)
	if errors.Is(err, qpack.ErrTooLarge) {
		rs.reject(stdhttp.StatusRequestHeaderFieldsTooLarge)
		return nil
	}
	if err != nil {
		return err
	}
	if rs.req != nil {
		trailer, err := trailerFrom(fields)
		if err != nil {
			rs.abort(ErrCodeMessageError)
			return nil
		}
		rs.req.Trailer = trailer
		rs.trailers = true
		return nil
	}
	req, err := newRequest(fields)
	if err != nil {
		rs.abort(ErrCodeMessageError)
		return nil
	}
	req.RemoteAddr = rs.sc.remoteAddr
	req.TLS = rs.sc.tlsState
	rs.req = req
	rs.sc.track(rs)
	if cl := req.Header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			rs.abort(ErrCodeMessageError)
			return nil
		}
		if n > rs.sc.h.config.MaxBodyBytes {
			rs.reject(stdhttp.StatusRequestEntityTooLarge)
			return nil
		}
		rs.declared = n
	}
	if strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		// The body is read whole before the handler runs, so there is no
		// reason to keep the client waiting for permission to send it.
		_ = rs.WriteInterim(stdhttp.StatusContinue, nil)
	}
	return nil
}

// finish runs the handler for a request that has arrived whole.
func (rs *requestStream) finish() {
	if rs.declared >= 0 && rs.declared != int64(len(rs.body)) {
		rs.abort(ErrCodeMessageError)
		return
	}
	rs.done = true
	req := rs.req
	req.ContentLength = int64(len(rs.body))
	if len(rs.body) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(rs.body))
	}
	rs.body = nil
	rs.sc.h.handler.ServeHTTP(fibhttp.NewStreamContext(rs.sc.conn, req, rs), req)
}

// abort gives up on a malformed or incomplete request.
func (rs *requestStream) abort(code ErrorCode) {
	rs.done = true
	rs.body = nil
	rs.mu.Lock()
	rs.closed = true
	rs.mu.Unlock()
	abortStream(rs.s, code)
	if rs.req != nil {
		rs.sc.untrack(rs)
	}
}

// reject answers a request with an error status before it has arrived
// whole, and stops reading it.
func (rs *requestStream) reject(status int) {
	rs.done = true
	rs.body = nil
	req := rs.req
	if req == nil {
		req = &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 3, Header: make(stdhttp.Header)}
	}
	_ = rs.WriteResponse(req, fibhttp.Response{
		StatusCode: status,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(status) + "\n"),
	})
}

// peerReset is the client abandoning the request.
func (rs *requestStream) peerReset() {
	rs.done = true
	rs.body = nil
	rs.mu.Lock()
	wasOpen := !rs.closed && !rs.responded
	rs.closed = true
	rs.mu.Unlock()
	if wasOpen {
		rs.s.Reset(uint64(ErrCodeRequestCancelled))
		if rs.req != nil {
			rs.sc.untrack(rs)
		}
	}
}

// WriteResponse sends the response: HEADERS, then the body in one DATA
// frame, then the end of the stream. QUIC flow and congestion control pace
// the body.
func (rs *requestStream) WriteResponse(req *stdhttp.Request, response fibhttp.Response) error {
	status := response.StatusCode
	if status < 200 || status > 999 {
		return fmt.Errorf("http3: invalid status code %d", status)
	}
	if err := checkHeader(response.Header); err != nil {
		return err
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return errors.New("http: response already written")
	case rs.closed:
		rs.mu.Unlock()
		return errStreamClosed
	}
	rs.responded = true
	rs.mu.Unlock()

	block := append([]byte(nil), qpack.Prefix...)
	block = qpack.AppendField(block, ":status", strconv.Itoa(status), false)
	statusBody := status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
	if statusBody {
		block = qpack.AppendField(block, "content-length", strconv.Itoa(len(response.Body)), false)
	}
	block = appendHeader(block, response.Header, func(name string) bool { return name == "content-length" })
	body := response.Body
	if !bodyAllowed(req, status) {
		body = nil
	}
	out := make([]byte, 0, len(block)+len(body)+16)
	out = appendHeadersFrame(out, block)
	if len(body) > 0 {
		out = appendFrameHeader(out, frameData, len(body))
		out = append(out, body...)
	}
	err := rs.s.Write(out, true)
	if !rs.remoteDone {
		// Answered before the request finished arriving: the rest of it is
		// not wanted (RFC 9114 section 4.1).
		rs.s.StopSending(uint64(ErrCodeNoError))
	}
	if response.Close {
		// One request cannot close a connection that others share, so
		// Close retires it gracefully.
		rs.sc.goAway()
	}
	if rs.req != nil {
		rs.sc.untrack(rs)
	}
	if err != nil {
		return errStreamClosed
	}
	return nil
}

// WriteInterim sends an informational 1xx response ahead of the final one.
func (rs *requestStream) WriteInterim(status int, header stdhttp.Header) error {
	if err := checkHeader(header); err != nil {
		return err
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return errors.New("http: interim response after the final one")
	case rs.closed:
		rs.mu.Unlock()
		return errStreamClosed
	}
	rs.mu.Unlock()
	block := append([]byte(nil), qpack.Prefix...)
	block = qpack.AppendField(block, ":status", strconv.Itoa(status), false)
	block = appendHeader(block, header, nil)
	if err := rs.s.Write(appendHeadersFrame(nil, block), false); err != nil {
		return errStreamClosed
	}
	return nil
}

// Push is not supported: HTTP/3 server push needs the client to ask for it
// with MAX_PUSH_ID, and browsers do not.
func (rs *requestStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error {
	return stdhttp.ErrNotSupported
}

var _ fibhttp.Stream = (*requestStream)(nil)
