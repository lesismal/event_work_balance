//go:build linux

package http

import (
	"bytes"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"sort"
	"strconv"
	"sync"

	epoll "github.com/lesismal/auto-balance-epoll/go"
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

type Context struct {
	Conn    *epoll.Connection
	Request *stdhttp.Request
	wrote   bool
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
	closeConnection := response.Close || c.Request.Close
	data, err := marshalResponse(c.Request, response, closeConnection)
	if err != nil {
		return err
	}
	if err = c.Conn.Send(data); err != nil {
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
	states  sync.Map
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

func (h *ServerHandler) OnOpen(c *epoll.Connection) {
	h.states.Store(c, NewParser(h.config))
}

func (h *ServerHandler) OnData(c *epoll.Connection, data []byte) {
	value, ok := h.states.Load(c)
	if !ok {
		value = NewParser(h.config)
		h.states.Store(c, value)
	}
	requests, err := value.(*Parser).Feed(data)
	for _, request := range requests {
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

func (h *ServerHandler) OnPriorityData(*epoll.Connection, []byte) {}
func (h *ServerHandler) OnClose(c *epoll.Connection, _ error)     { h.states.Delete(c) }

func marshalResponse(request *stdhttp.Request, response Response, closeConnection bool) ([]byte, error) {
	if response.StatusCode < 100 || response.StatusCode > 999 {
		return nil, fmt.Errorf("http: invalid status code %d", response.StatusCode)
	}
	header := response.Header.Clone()
	if header == nil {
		header = make(stdhttp.Header)
	}
	header.Del("Transfer-Encoding")
	bodyAllowed := response.StatusCode != stdhttp.StatusNoContent &&
		response.StatusCode != stdhttp.StatusNotModified &&
		(response.StatusCode < 100 || response.StatusCode >= 200)
	contentLength := len(response.Body)
	if !bodyAllowed {
		contentLength = 0
	}
	header.Set("Content-Length", strconv.Itoa(contentLength))
	if closeConnection {
		header.Set("Connection", "close")
	}
	proto := "HTTP/1.1"
	if request.ProtoMajor == 1 && request.ProtoMinor == 0 {
		proto = "HTTP/1.0"
		if !closeConnection {
			header.Set("Connection", "keep-alive")
		}
	}
	statusText := stdhttp.StatusText(response.StatusCode)
	if statusText == "" {
		statusText = "Status"
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, "%s %d %s\r\n", proto, response.StatusCode, statusText)
	keys := make([]string, 0, len(header))
	for key := range header {
		keys = append(keys, key)
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
			fmt.Fprintf(&out, "%s: %s\r\n", key, value)
		}
	}
	out.WriteString("\r\n")
	if request.Method != stdhttp.MethodHead && bodyAllowed {
		out.Write(response.Body)
	}
	return out.Bytes(), nil
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < ' ' && value[i] != '\t') || value[i] == 0x7f {
			return false
		}
	}
	return true
}

var _ epoll.Handler = (*ServerHandler)(nil)
