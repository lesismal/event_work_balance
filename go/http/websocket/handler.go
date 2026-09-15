//go:build linux

package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"
	"unicode/utf8"

	epoll "github.com/lesismal/auto-balance-epoll/go"
	epollhttp "github.com/lesismal/auto-balance-epoll/go/http"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Config struct {
	MaxMessageBytes int64
	Subprotocols    []string
	CheckOrigin     func(*stdhttp.Request) bool
	HTTP            epollhttp.Config
}

func DefaultConfig() Config {
	return Config{MaxMessageBytes: 16 << 20, HTTP: epollhttp.DefaultConfig()}
}

type Handler interface {
	OnOpen(*Connection, *stdhttp.Request)
	OnMessage(*Connection, Opcode, []byte)
	OnClose(*Connection, uint16, string, error)
}

type HandlerFuncs struct {
	Open    func(*Connection, *stdhttp.Request)
	Message func(*Connection, Opcode, []byte)
	Close   func(*Connection, uint16, string, error)
}

func (h HandlerFuncs) OnOpen(c *Connection, r *stdhttp.Request) {
	if h.Open != nil {
		h.Open(c, r)
	}
}
func (h HandlerFuncs) OnMessage(c *Connection, opcode Opcode, data []byte) {
	if h.Message != nil {
		h.Message(c, opcode, data)
	}
}
func (h HandlerFuncs) OnClose(c *Connection, code uint16, reason string, err error) {
	if h.Close != nil {
		h.Close(c, code, reason, err)
	}
}

// Connection is a WebSocket connection. Its write methods are safe to call
// from application goroutines.
type Connection struct {
	conn        *epoll.Connection
	subprotocol string
	mu          sync.Mutex
	closeSent   bool
	closeCode   uint16
	closeReason string
}

func (c *Connection) Subprotocol() string { return c.subprotocol }

func (c *Connection) WriteMessage(opcode Opcode, payload []byte) error {
	if opcode != Text && opcode != Binary {
		return errors.New("websocket: WriteMessage requires Text or Binary opcode")
	}
	if opcode == Text && !utf8.Valid(payload) {
		return ErrInvalidPayload
	}
	return c.writeFrame(opcode, payload)
}

func (c *Connection) WriteText(text string) error {
	return c.WriteMessage(Text, []byte(text))
}

func (c *Connection) WriteBinary(payload []byte) error {
	return c.WriteMessage(Binary, payload)
}

func (c *Connection) Ping(payload []byte) error { return c.writeFrame(Ping, payload) }
func (c *Connection) Pong(payload []byte) error { return c.writeFrame(Pong, payload) }

func (c *Connection) Close(code uint16, reason string) error {
	if !validCloseCode(code) || !utf8.ValidString(reason) {
		return ErrInvalidPayload
	}
	payload := make([]byte, 2+len(reason))
	if len(payload) > 125 {
		return errors.New("websocket: close reason too long")
	}
	binary.BigEndian.PutUint16(payload, code)
	copy(payload[2:], reason)
	return c.sendClose(payload)
}

func (c *Connection) writeFrame(opcode Opcode, payload []byte) error {
	frame, err := MarshalFrame(opcode, payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	closed := c.closeSent
	c.mu.Unlock()
	if closed {
		return errors.New("websocket: close already sent")
	}
	return c.conn.Send(frame)
}

func (c *Connection) sendClose(payload []byte) error {
	frame, err := MarshalFrame(Close, payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.closeSent {
		c.mu.Unlock()
		return nil
	}
	c.closeSent = true
	c.closeCode, c.closeReason = closePayload(payload)
	c.mu.Unlock()
	if err = c.conn.Send(frame); err != nil {
		return err
	}
	c.conn.CloseAfterSend()
	return nil
}

type connectionState struct {
	httpParser *epollhttp.Parser
	wsParser   *Parser
	websocket  *Connection
	upgraded   bool
}

type ServerHandler struct {
	config  Config
	handler Handler
	states  sync.Map
}

func NewHandler(handler Handler) *ServerHandler {
	return NewHandlerWithConfig(DefaultConfig(), handler)
}

func NewHandlerWithConfig(config Config, handler Handler) *ServerHandler {
	defaults := DefaultConfig()
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	return &ServerHandler{config: config, handler: handler}
}

func (h *ServerHandler) OnOpen(c *epoll.Connection) {
	h.states.Store(c, &connectionState{httpParser: epollhttp.NewParser(h.config.HTTP)})
}

func (h *ServerHandler) OnData(c *epoll.Connection, data []byte) {
	value, ok := h.states.Load(c)
	if !ok {
		h.OnOpen(c)
		value, _ = h.states.Load(c)
	}
	state := value.(*connectionState)
	if state.upgraded {
		h.handleFrames(state, data)
		return
	}
	request, complete, err := state.httpParser.FeedOne(data)
	if err != nil {
		h.reject(c, request, stdhttp.StatusBadRequest)
		return
	}
	if !complete {
		return
	}
	subprotocol, err := h.validateHandshake(request)
	if err != nil {
		h.reject(c, request, stdhttp.StatusBadRequest)
		return
	}
	key := request.Header.Get("Sec-WebSocket-Key")
	if err = c.Send(handshakeResponse(key, subprotocol)); err != nil {
		c.Close()
		return
	}
	state.websocket = &Connection{conn: c, subprotocol: subprotocol}
	state.wsParser = NewParser(h.config.MaxMessageBytes)
	state.upgraded = true
	remainder := state.httpParser.TakeBuffered()
	h.handler.OnOpen(state.websocket, request)
	if len(remainder) != 0 {
		h.handleFrames(state, remainder)
	}
}

func (h *ServerHandler) handleFrames(state *connectionState, data []byte) {
	events, err := state.wsParser.Feed(data)
	for _, event := range events {
		switch event.Opcode {
		case Text, Binary:
			h.handler.OnMessage(state.websocket, event.Opcode, event.Payload)
		case Ping:
			if sendErr := state.websocket.Pong(event.Payload); sendErr != nil {
				state.websocket.conn.Close()
				return
			}
		case Close:
			_ = state.websocket.sendClose(event.Payload)
			return
		}
	}
	if err != nil {
		code := uint16(CloseProtocolError)
		if errors.Is(err, ErrMessageTooBig) {
			code = CloseMessageTooBig
		} else if errors.Is(err, ErrInvalidPayload) {
			code = CloseInvalidPayload
		}
		payload := make([]byte, 2)
		binary.BigEndian.PutUint16(payload, code)
		_ = state.websocket.sendClose(payload)
	}
}

func (h *ServerHandler) validateHandshake(request *stdhttp.Request) (string, error) {
	if request.Method != stdhttp.MethodGet || request.ProtoMajor != 1 || request.ProtoMinor < 1 {
		return "", ErrProtocol
	}
	if !headerHasToken(request.Header, "Connection", "upgrade") ||
		!strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket") ||
		request.Header.Get("Sec-WebSocket-Version") != "13" {
		return "", ErrProtocol
	}
	key, err := base64.StdEncoding.DecodeString(request.Header.Get("Sec-WebSocket-Key"))
	if err != nil || len(key) != 16 {
		return "", ErrProtocol
	}
	if h.config.CheckOrigin != nil && !h.config.CheckOrigin(request) {
		return "", errors.New("websocket: origin rejected")
	}
	offered := splitHeaderTokens(request.Header.Values("Sec-WebSocket-Protocol"))
	for _, candidate := range offered {
		if !validToken(candidate) {
			return "", ErrProtocol
		}
		for _, supported := range h.config.Subprotocols {
			if candidate == supported && validToken(supported) {
				return candidate, nil
			}
		}
	}
	return "", nil
}

func (h *ServerHandler) reject(c *epoll.Connection, request *stdhttp.Request, status int) {
	if request == nil {
		request = &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Header: make(stdhttp.Header)}
	}
	context := &epollhttp.Context{Conn: c, Request: request}
	_ = context.WriteResponse(epollhttp.Response{
		StatusCode: status,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(status) + "\n"),
		Close:      true,
	})
}

func (h *ServerHandler) OnPriorityData(*epoll.Connection, []byte) {}

func (h *ServerHandler) OnClose(c *epoll.Connection, err error) {
	value, ok := h.states.LoadAndDelete(c)
	if !ok {
		return
	}
	state := value.(*connectionState)
	if state.upgraded {
		state.websocket.mu.Lock()
		code := state.websocket.closeCode
		reason := state.websocket.closeReason
		state.websocket.mu.Unlock()
		if code == 0 {
			code = 1006
		}
		h.handler.OnClose(state.websocket, code, reason, err)
	}
}

func handshakeResponse(key, subprotocol string) []byte {
	sum := sha1.Sum([]byte(key + websocketGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n"
	if subprotocol != "" {
		response += "Sec-WebSocket-Protocol: " + subprotocol + "\r\n"
	}
	return []byte(response + "\r\n")
}

func headerHasToken(header stdhttp.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, candidate := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

func splitHeaderTokens(values []string) []string {
	var tokens []string
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				tokens = append(tokens, token)
			}
		}
	}
	return tokens
}

func validToken(value string) bool {
	if value == "" {
		return false
	}
	const separators = "()<>@,;:\\\"/[]?={} \t"
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e || strings.ContainsRune(separators, rune(value[i])) {
			return false
		}
	}
	return true
}

func closePayload(payload []byte) (uint16, string) {
	if len(payload) < 2 {
		return CloseNormal, ""
	}
	return binary.BigEndian.Uint16(payload[:2]), string(payload[2:])
}

var _ epoll.Handler = (*ServerHandler)(nil)
