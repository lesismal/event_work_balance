//go:build linux || darwin || windows

package http

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"sort"
	"strconv"
	"strings"

	fib "github.com/lesismal/fib/go"
	fibtls "github.com/lesismal/fib/go/tls"
)

type Handler interface {
	ServeHTTP(*Context, *stdhttp.Request)
}

type HandlerFunc func(*Context, *stdhttp.Request)

func (f HandlerFunc) ServeHTTP(c *Context, r *stdhttp.Request) { f(c, r) }

type Response struct {
	StatusCode int
	Header     stdhttp.Header
	Body       []byte
	// Trailer is sent after the body. HTTP/1.1 carries it by sending the
	// body chunked, and announces its names in a Trailer header; HTTP/2 and
	// HTTP/3 send it as a trailing header block. An HTTP/1.0 client, and a
	// response that has no body, gets none of it.
	Trailer stdhttp.Header
	Close   bool
}

// Context is the connection a request arrived on, and the response to it.
// Respond with Respond or WriteResponse, or write the response piece by piece
// through the http.ResponseWriter methods Context has, rather than by
// sending on Conn: on an HTTP/2 or HTTP/3 connection the response is framed
// for its stream, and bytes sent on Conn directly would corrupt the
// connection.
//
// Since Context is an http.ResponseWriter, http.ServeFile, http.ServeContent
// and other net/http helpers can answer through it; see ReadFrom for how
// they send files.
type Context struct {
	Conn    *fib.Connection
	Request *stdhttp.Request
	wrote   bool
	// closing records that the response ends the HTTP/1 connection, so that
	// no request pipelined behind it is served.
	closing bool
	// w is the response being written through the ResponseWriter methods.
	w *responseWriter
	// stream is the HTTP/2 stream the request arrived on, or nil for HTTP/1.
	stream *h2ServerStream
	// external is the stream of a protocol served outside this package.
	external Stream
}

// Stream answers a request that arrived over a protocol served outside this
// package, as HTTP/3 is by package http3, so that the same Handler serves
// it through the same Context.
type Stream interface {
	// WriteResponse sends the final response to req.
	WriteResponse(req *stdhttp.Request, response Response) error
	// WriteInterim sends an informational 1xx response, already checked to
	// be one.
	WriteInterim(status int, header stdhttp.Header) error
	// Push promises target to the client, as Context.Push does.
	Push(req *stdhttp.Request, target string, opts *stdhttp.PushOptions) error
}

// NewStreamContext returns the Context through which a handler answers req,
// which arrived on conn and is answered through stream.
func NewStreamContext(conn *fib.Connection, req *stdhttp.Request, stream Stream) *Context {
	return &Context{Conn: conn, Request: req, external: stream}
}

func (c *Context) Respond(status int, contentType string, body []byte) error {
	header := make(stdhttp.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return c.WriteResponse(Response{StatusCode: status, Header: header, Body: body})
}

// WriteResponse sends the whole response at once. It fails once the
// response has been begun through the ResponseWriter methods.
func (c *Context) WriteResponse(response Response) error {
	if c.w != nil && c.w.status != 0 {
		return ErrResponseWritten
	}
	return c.writeResponse(response)
}

func (c *Context) writeResponse(response Response) error {
	if c.wrote {
		return ErrResponseWritten
	}
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	if c.external != nil {
		if err := c.external.WriteResponse(c.Request, response); err != nil {
			return err
		}
		c.wrote = true
		return nil
	}
	if c.stream != nil {
		// HTTP/2 multiplexes the connection, so Close retires it gracefully
		// with GOAWAY rather than cutting the other streams short.
		if err := c.stream.respond(c.Request, response); err != nil {
			return err
		}
		c.wrote = true
		return nil
	}
	closeConnection := response.Close || c.Request.Close || headerHasToken(response.Header, "Connection", "close")
	data, err := marshalResponse(c.Request, response, closeConnection)
	if err != nil {
		return err
	}
	if err = c.Conn.SendOwned(data); err != nil {
		return err
	}
	c.wrote = true
	if closeConnection {
		c.closing = true
		c.Conn.CloseAfterSend()
	}
	return nil
}

// Push promises target to the client ahead of its asking, as http.Pusher
// does: target is an absolute path, or an absolute URL with the request's
// scheme, and opts may set the method (GET or HEAD) and request headers.
// The promised request is served through the handler, on its own stream,
// before Push returns; call Push before responding, since a promise must
// arrive before the response that would lead the client to ask for it.
//
// Push returns http.ErrNotSupported on HTTP/1 and HTTP/3, on a request that
// was itself pushed, and when the client has disabled push, as browsers and
// Go's own client do.
func (c *Context) Push(target string, opts *stdhttp.PushOptions) error {
	if c.external != nil {
		return c.external.Push(c.Request, target, opts)
	}
	if c.stream == nil {
		return stdhttp.ErrNotSupported
	}
	return c.stream.push(c.Request, target, opts)
}

// WriteInterim sends an informational 1xx response ahead of the final one,
// such as 103 Early Hints with Link headers. It may be called several times
// before WriteResponse. 101 is not allowed, and an HTTP/1.0 client, which
// does not know interim responses, gets http.ErrNotSupported.
//
// A request that expects 100-continue gets its 100 Continue without asking:
// the body is read whole before the handler runs.
func (c *Context) WriteInterim(status int, header stdhttp.Header) error {
	if status < 100 || status > 199 || status == stdhttp.StatusSwitchingProtocols {
		return fmt.Errorf("http: invalid interim status code %d", status)
	}
	if c.wrote || c.w != nil && c.w.status != 0 {
		return errors.New("http: interim response after the final one")
	}
	if c.external != nil {
		return c.external.WriteInterim(status, header)
	}
	if c.stream != nil {
		return c.stream.writeInterim(status, header)
	}
	if !c.Request.ProtoAtLeast(1, 1) {
		return stdhttp.ErrNotSupported
	}
	out := make([]byte, 0, 64)
	out = append(out, "HTTP/1.1 "...)
	out = strconv.AppendInt(out, int64(status), 10)
	out = append(out, ' ')
	out = append(out, stdhttp.StatusText(status)...)
	out = append(out, '\r', '\n')
	out, err := appendHeaderLines(out, header, nil)
	if err != nil {
		return err
	}
	return c.Conn.SendOwned(append(out, '\r', '\n'))
}

var _ stdhttp.Pusher = (*Context)(nil)

type ServerHandler struct {
	handler Handler
	config  Config
}

func NewHandler(handler Handler) *ServerHandler {
	return NewHandlerWithConfig(DefaultConfig(), handler)
}

func NewHandlerWithConfig(config Config, handler Handler) *ServerHandler {
	if handler == nil {
		handler = HandlerFunc(func(c *Context, _ *stdhttp.Request) {
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("404 page not found\n"))
		})
	}
	return &ServerHandler{handler: handler, config: config}
}

func (h *ServerHandler) OnOpen(c *fib.Connection) {
	c.SetAttachment(h.newParser(c))
}

// newParser starts a connection's parser, recording the peer's address so
// that each request carries it in RemoteAddr as net/http's do.
func (h *ServerHandler) newParser(c *fib.Connection) *Parser {
	parser := NewParser(h.config)
	if addr := c.RemoteAddr(); addr != nil {
		parser.remoteAddr = addr.String()
	}
	return parser
}

func (h *ServerHandler) OnData(c *fib.Connection, data []byte) {
	var parser *Parser
	switch state := c.Attachment().(type) {
	case *h2ServerConn:
		state.feed(data)
		return
	case *Parser:
		parser = state
	default:
		parser = h.newParser(c)
		c.SetAttachment(parser)
	}
	if !parser.sniffed {
		if h.config.DisableHTTP2 {
			parser.sniffed = true
		} else if data = h.sniff(c, parser, data); data == nil {
			return
		}
	}
	// One request at a time, since one may switch the connection to HTTP/2
	// and leave what follows it to the new protocol.
	var err error
	for ; ; data = nil {
		var request *stdhttp.Request
		var complete bool
		request, complete, err = parser.FeedOne(data)
		if err != nil || !complete {
			break
		}
		request.RemoteAddr = parser.remoteAddr
		request.TLS = h.tlsState(c, parser)
		if status := checkRequest(request); status != 0 {
			err = requestError(status)
			break
		}
		if settings, ok := h.h2cUpgrade(c, request); ok {
			h.upgradeH2C(c, parser, request, settings)
			return
		}
		context := &Context{Conn: c, Request: request}
		serveRequest(h.handler, context)
		if request.Close || context.closing {
			return
		}
		if len(parser.buffer) == 0 {
			break
		}
	}
	if parser.wantContinue {
		// The request waits for permission to send its body; the body is
		// read whole before the handler runs, so permission is given at once.
		parser.wantContinue = false
		_ = c.Send([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
	}
	if err != nil {
		status := stdhttp.StatusBadRequest
		var statusErr requestError
		switch {
		case errors.As(err, &statusErr):
			status = int(statusErr)
		case errors.Is(err, ErrHeaderTooLarge):
			status = stdhttp.StatusRequestHeaderFieldsTooLarge
		case errors.Is(err, ErrBodyTooLarge):
			status = stdhttp.StatusRequestEntityTooLarge
		case errors.Is(err, ErrUnsupportedTransferEncoding):
			status = stdhttp.StatusNotImplemented
		}
		request := &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Header: make(stdhttp.Header)}
		context := &Context{Conn: c, Request: request}
		_ = context.WriteResponse(Response{
			StatusCode: status,
			Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
			Body:       []byte(stdhttp.StatusText(status) + "\n"),
			Close:      true,
		})
	}
}

// serveRequest runs the handler for the request context carries, and then ends the
// response the handler wrote through the ResponseWriter methods, if it began
// one, as net/http does when a handler returns.
func serveRequest(handler Handler, context *Context) {
	handler.ServeHTTP(context, context.Request)
	_ = context.Finish()
}

// requestError is a request the server refuses with its status.
type requestError int

func (e requestError) Error() string { return "http: " + stdhttp.StatusText(int(e)) }

// checkRequest returns the status to refuse a request that parsed but that
// the server cannot serve with, or 0. An HTTP/1.1 request has to name its
// host exactly once (RFC 9112 section 3.2), and an expectation other than
// 100-continue cannot be met (RFC 9110 section 10.1.1).
func checkRequest(request *stdhttp.Request) int {
	if request.ProtoAtLeast(1, 1) && request.Method != stdhttp.MethodConnect {
		if hosts, ok := request.Header["Host"]; !ok && request.Host == "" || len(hosts) > 1 {
			return stdhttp.StatusBadRequest
		}
	}
	if expect, ok := request.Header["Expect"]; ok {
		if len(expect) != 1 || !strings.EqualFold(strings.TrimSpace(expect[0]), "100-continue") {
			return stdhttp.StatusExpectationFailed
		}
	}
	return 0
}

// sniff tells HTTP/2 from HTTP/1 by whether the connection opens with the
// HTTP/2 preface. It returns the bytes to parse as HTTP/1, or nil when they
// are HTTP/2 or too few to tell yet.
func (h *ServerHandler) sniff(c *fib.Connection, parser *Parser, data []byte) []byte {
	parser.buffer = append(parser.buffer, data...)
	n := min(len(parser.buffer), len(h2Preface))
	if string(parser.buffer[:n]) == h2Preface[:n] {
		if n < len(h2Preface) {
			return nil
		}
		sc := newH2ServerConn(h, c, parser.remoteAddr)
		sc.tlsState = h.tlsState(c, parser)
		c.SetAttachment(sc)
		sc.start()
		sc.feed(parser.TakeBuffered())
		return nil
	}
	parser.sniffed = true
	return parser.TakeBuffered()
}

// tlsState is the connection's TLS state, looked up once the handshake has
// delivered the first bytes, or nil for a connection without TLS.
func (h *ServerHandler) tlsState(c *fib.Connection, parser *Parser) *stdtls.ConnectionState {
	if !parser.tlsChecked {
		parser.tlsChecked = true
		if state, ok := fibtls.ConnectionState(c); ok {
			parser.tls = &state
		}
	}
	return parser.tls
}

func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *ServerHandler) OnClose(c *fib.Connection, _ error) {
	if sc, ok := c.Attachment().(*h2ServerConn); ok {
		sc.shutdown()
	}
	c.SetAttachment(nil)
}

func marshalResponse(request *stdhttp.Request, response Response, closeConnection bool) ([]byte, error) {
	status := response.StatusCode
	head := responseHead{status: status, header: response.Header, contentLength: -1, close: closeConnection}
	hasBody := statusHasBody(status)
	isHead := request.Method == stdhttp.MethodHead
	var trailer stdhttp.Header
	switch {
	case status == stdhttp.StatusNotModified:
		// A 304 may repeat the length of what it stands for, and only a
		// length the handler gives knows that (RFC 9110 section 8.6).
		head.contentLength = declaredLength(response.Header)
	case !hasBody:
		// 1xx and 204 never carry Content-Length.
	case isHead:
		head.contentLength = declaredLength(response.Header)
		if head.contentLength < 0 {
			head.contentLength = int64(len(response.Body))
		}
	case len(response.Trailer) > 0 && request.ProtoAtLeast(1, 1):
		head.chunked = true
		trailer = make(stdhttp.Header, len(response.Trailer))
		for key, values := range response.Trailer {
			if !forbiddenTrailer(key) {
				key = stdhttp.CanonicalHeaderKey(key)
				trailer[key] = values
				head.trailers = append(head.trailers, key)
			}
		}
		sort.Strings(head.trailers)
	default:
		head.contentLength = int64(len(response.Body))
	}
	capacity := len(response.Body) + 128
	for key, values := range response.Header {
		capacity += len(key) + 4
		for _, value := range values {
			capacity += len(value) + 2
		}
	}
	out, err := appendResponseHead(make([]byte, 0, capacity), request, head)
	if err != nil {
		return nil, err
	}
	if isHead || !hasBody {
		return out, nil
	}
	if !head.chunked {
		return append(out, response.Body...), nil
	}
	if len(response.Body) > 0 {
		out = appendChunk(out, response.Body)
	}
	out = append(out, "0\r\n"...)
	if out, err = appendHeaderLines(out, trailer, nil); err != nil {
		return nil, err
	}
	return append(out, crlf...), nil
}

// declaredLength is the Content-Length header holds, or -1.
func declaredLength(header stdhttp.Header) int64 {
	if values := header["Content-Length"]; len(values) == 1 {
		if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return -1
}

// appendHeaderLines appends header as HTTP/1 lines in key order, leaving out
// the keys skip reports.
func appendHeaderLines(out []byte, header stdhttp.Header, skip func(string) bool) ([]byte, error) {
	keys := make([]string, 0, len(header))
	for key := range header {
		if skip == nil || !skip(key) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" || textproto.CanonicalMIMEHeaderKey(key) == "" {
			return nil, errors.New("http: invalid response header name")
		}
		for _, value := range header[key] {
			if !validHeaderValue(value) {
				return nil, errors.New("http: invalid response header value")
			}
			out = append(out, key...)
			out = append(out, ':', ' ')
			out = append(out, value...)
			out = append(out, '\r', '\n')
		}
	}
	return out, nil
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < ' ' && value[i] != '\t') || value[i] == 0x7f {
			return false
		}
	}
	return true
}

var _ fib.Handler = (*ServerHandler)(nil)
