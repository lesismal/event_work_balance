//go:build linux || darwin || windows

package http

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"sort"
	"strconv"
	"strings"

	fib "github.com/lesismal/fib/go"
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
	Close      bool
}

// Context is the connection a request arrived on. Respond with Respond or
// WriteResponse rather than by sending on Conn: on an HTTP/2 connection the
// response is framed for its stream, and bytes sent on Conn directly would
// corrupt the connection.
type Context struct {
	Conn    *fib.Connection
	Request *stdhttp.Request
	wrote   bool
	// stream is the HTTP/2 stream the request arrived on, or nil for HTTP/1.
	stream *h2ServerStream
}

func (c *Context) Respond(status int, contentType string, body []byte) error {
	header := make(stdhttp.Header)
	if contentType != "" {
		header.Set("Content-Type", contentType)
	}
	return c.WriteResponse(Response{StatusCode: status, Header: header, Body: body})
}

func (c *Context) WriteResponse(response Response) error {
	if c.wrote {
		return errors.New("http: response already written")
	}
	if response.StatusCode == 0 {
		response.StatusCode = stdhttp.StatusOK
	}
	if c.stream != nil {
		// HTTP/2 multiplexes the connection, so Close does not apply to it.
		if err := c.stream.respond(c.Request, response); err != nil {
			return err
		}
		c.wrote = true
		return nil
	}
	closeConnection := response.Close || c.Request.Close
	data, err := marshalResponse(c.Request, response, closeConnection)
	if err != nil {
		return err
	}
	if err = c.Conn.SendOwned(data); err != nil {
		return err
	}
	c.wrote = true
	if closeConnection {
		c.Conn.CloseAfterSend()
	}
	return nil
}

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
	requests, err := parser.Feed(data)
	for _, request := range requests {
		request.RemoteAddr = parser.remoteAddr
		context := &Context{Conn: c, Request: request}
		h.handler.ServeHTTP(context, request)
		if request.Close {
			break
		}
	}
	if err != nil {
		status := stdhttp.StatusBadRequest
		if errors.Is(err, ErrHeaderTooLarge) {
			status = stdhttp.StatusRequestHeaderFieldsTooLarge
		} else if errors.Is(err, ErrBodyTooLarge) {
			status = stdhttp.StatusRequestEntityTooLarge
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
		c.SetAttachment(sc)
		sc.start()
		sc.feed(parser.TakeBuffered())
		return nil
	}
	parser.sniffed = true
	return parser.TakeBuffered()
}

func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *ServerHandler) OnClose(c *fib.Connection, _ error) {
	if sc, ok := c.Attachment().(*h2ServerConn); ok {
		sc.shutdown()
	}
	c.SetAttachment(nil)
}

func marshalResponse(request *stdhttp.Request, response Response, closeConnection bool) ([]byte, error) {
	if response.StatusCode < 100 || response.StatusCode > 999 {
		return nil, fmt.Errorf("http: invalid status code %d", response.StatusCode)
	}
	bodyAllowed := response.StatusCode != stdhttp.StatusNoContent &&
		response.StatusCode != stdhttp.StatusNotModified &&
		(response.StatusCode < 100 || response.StatusCode >= 200)
	contentLength := len(response.Body)
	if !bodyAllowed {
		contentLength = 0
	}
	proto := "HTTP/1.1"
	connectionValue := ""
	if closeConnection {
		connectionValue = "close"
	}
	if request.ProtoMajor == 1 && request.ProtoMinor == 0 {
		proto = "HTTP/1.0"
		if !closeConnection {
			connectionValue = "keep-alive"
		}
	}
	statusText := stdhttp.StatusText(response.StatusCode)
	if statusText == "" {
		statusText = "Status"
	}
	// Build directly into the returned byte slice. Avoid cloning the header map
	// and fmt/bytes.Buffer overhead on every response.
	capacity := len(response.Body) + 96
	for key, values := range response.Header {
		capacity += len(key) + 4
		for _, value := range values {
			capacity += len(value) + 2
		}
	}
	out := make([]byte, 0, capacity)
	out = append(out, proto...)
	out = append(out, ' ')
	out = strconv.AppendInt(out, int64(response.StatusCode), 10)
	out = append(out, ' ')
	out = append(out, statusText...)
	out = append(out, '\r', '\n')
	out = append(out, "Content-Length: "...)
	out = strconv.AppendInt(out, int64(contentLength), 10)
	out = append(out, '\r', '\n')
	if connectionValue != "" {
		out = append(out, "Connection: "...)
		out = append(out, connectionValue...)
		out = append(out, '\r', '\n')
	}
	keys := make([]string, 0, len(response.Header))
	for key := range response.Header {
		if strings.EqualFold(key, "Content-Length") || strings.EqualFold(key, "Transfer-Encoding") ||
			(connectionValue != "" && strings.EqualFold(key, "Connection")) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" || textproto.CanonicalMIMEHeaderKey(key) == "" {
			return nil, errors.New("http: invalid response header name")
		}
		for _, value := range response.Header[key] {
			if !validHeaderValue(value) {
				return nil, errors.New("http: invalid response header value")
			}
			out = append(out, key...)
			out = append(out, ':', ' ')
			out = append(out, value...)
			out = append(out, '\r', '\n')
		}
	}
	out = append(out, '\r', '\n')
	if request.Method != stdhttp.MethodHead && bodyAllowed {
		out = append(out, response.Body...)
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
