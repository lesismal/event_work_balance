//go:build linux

package websocket

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
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
	// OnMessage payload is valid only for the duration of the callback. Copy it
	// before returning if it must be retained.
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
	closeSent   atomic.Bool
	closeState  atomic.Pointer[connectionCloseState]
}

type connectionCloseState struct {
	code   uint16
	reason string
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
	if c.closeSent.Load() {
		return errors.New("websocket: close already sent")
	}
	header, headerLen, err := frameHeader(opcode, len(payload))
	if err != nil {
		return err
	}
	return c.conn.SendParts(header[:headerLen], payload)
}

func (c *Connection) sendClose(payload []byte) error {
	if !c.closeSent.CompareAndSwap(false, true) {
		return nil
	}
	header, headerLen, err := frameHeader(Close, len(payload))
	if err != nil {
		c.closeSent.Store(false)
		return err
	}
	code, reason := closePayload(payload)
	c.closeState.Store(&connectionCloseState{code: code, reason: reason})
	if err = c.conn.SendParts(header[:headerLen], payload); err != nil {
		return err
	}
	c.conn.CloseAfterSend()
	return nil
}

type connectionState struct {
	handshake *handshakeParser
	wsParser  Parser
	websocket Connection
	upgraded  bool
}

type ServerHandler struct {
	config           Config
	handler          Handler
	needsRequest     bool
	handshakeParsers sync.Pool
}

func NewHandler(handler Handler) *ServerHandler {
	return NewHandlerWithConfig(DefaultConfig(), handler)
}

func NewHandlerWithConfig(config Config, handler Handler) *ServerHandler {
	defaults := DefaultConfig()
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if config.HTTP.MaxHeaderBytes <= 0 {
		config.HTTP.MaxHeaderBytes = defaults.HTTP.MaxHeaderBytes
	}
	if config.HTTP.MaxBodyBytes <= 0 {
		config.HTTP.MaxBodyBytes = defaults.HTTP.MaxBodyBytes
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	needsRequest := config.CheckOrigin != nil
	if !needsRequest {
		switch typed := handler.(type) {
		case HandlerFuncs:
			needsRequest = typed.Open != nil
		case *HandlerFuncs:
			needsRequest = typed == nil || typed.Open != nil
		default:
			needsRequest = true
		}
	}
	h := &ServerHandler{config: config, handler: handler, needsRequest: needsRequest}
	h.handshakeParsers.New = func() any {
		return &handshakeParser{maxHeaderBytes: config.HTTP.MaxHeaderBytes}
	}
	return h
}

func (h *ServerHandler) OnOpen(c *epoll.Connection) {
	c.SetAttachment(&connectionState{})
}

func (h *ServerHandler) OnData(c *epoll.Connection, data []byte) {
	state, _ := c.Attachment().(*connectionState)
	if state == nil {
		h.OnOpen(c)
		state, _ = c.Attachment().(*connectionState)
	}
	if state.upgraded {
		h.handleFrames(state, data)
		return
	}
	if !h.needsRequest && state.handshake == nil {
		key, subprotocol, remainder, complete, err := h.validateMinimalHandshake(data)
		if err != nil {
			h.reject(c, nil, stdhttp.StatusBadRequest)
			return
		}
		if complete {
			h.upgrade(c, state, nil, key, subprotocol, remainder)
			return
		}
	}
	var request *stdhttp.Request
	var remainder []byte
	var complete bool
	var err error
	if state.handshake == nil {
		request, remainder, complete, err = parseCompleteHandshake(data, h.config.HTTP.MaxHeaderBytes)
		if !complete && err == nil {
			state.handshake = h.handshakeParsers.Get().(*handshakeParser)
			request, complete, err = state.handshake.Feed(data)
		}
	} else {
		request, complete, err = state.handshake.Feed(data)
	}
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
	key := []byte(request.Header.Get("Sec-Websocket-Key"))
	h.upgrade(c, state, request, key, subprotocol, remainder)
}

func (h *ServerHandler) upgrade(c *epoll.Connection, state *connectionState, request *stdhttp.Request, key []byte, subprotocol string, remainder []byte) {
	if err := sendHandshakeResponse(c, key, subprotocol); err != nil {
		c.Close()
		return
	}
	state.websocket = Connection{conn: c, subprotocol: subprotocol}
	state.wsParser.maxMessageBytes = h.config.MaxMessageBytes
	state.upgraded = true
	if state.handshake != nil && remainder == nil {
		remainder = state.handshake.TakeBuffered()
	}
	h.releaseHandshakeParser(state)
	if request != nil {
		h.handler.OnOpen(&state.websocket, request)
	}
	if len(remainder) != 0 {
		h.handleFrames(state, remainder)
	}
}

func (h *ServerHandler) handleFrames(state *connectionState, data []byte) {
	defer state.wsParser.ReleaseBorrowed()
	for {
		event, complete, err := state.wsParser.FeedOneBorrowed(data)
		data = nil
		if err != nil {
			h.closeParserError(state, err)
			return
		}
		if !complete {
			return
		}
		switch event.Opcode {
		case Text, Binary:
			h.handler.OnMessage(&state.websocket, event.Opcode, event.Payload)
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
}

func (h *ServerHandler) closeParserError(state *connectionState, err error) {
	code := uint16(CloseProtocolError)
	if errors.Is(err, ErrMessageTooBig) {
		code = CloseMessageTooBig
	} else if errors.Is(err, ErrInvalidPayload) {
		code = CloseInvalidPayload
	}
	var payload [2]byte
	binary.BigEndian.PutUint16(payload[:], code)
	_ = state.websocket.sendClose(payload[:])
}

func (h *ServerHandler) validateHandshake(request *stdhttp.Request) (string, error) {
	if request.Method != stdhttp.MethodGet || request.ProtoMajor != 1 || request.ProtoMinor < 1 {
		return "", ErrProtocol
	}
	if !headerHasToken(request.Header, "Connection", "upgrade") ||
		!strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket") ||
		request.Header.Get("Sec-Websocket-Version") != "13" {
		return "", ErrProtocol
	}
	key := request.Header.Get("Sec-Websocket-Key")
	var decodedKey [16]byte
	n, err := base64.StdEncoding.Decode(decodedKey[:], []byte(key))
	if err != nil || len(key) != base64.StdEncoding.EncodedLen(len(decodedKey)) || n != len(decodedKey) {
		return "", ErrProtocol
	}
	if h.config.CheckOrigin != nil && !h.config.CheckOrigin(request) {
		return "", errors.New("websocket: origin rejected")
	}
	for _, value := range request.Header.Values("Sec-Websocket-Protocol") {
		for len(value) != 0 {
			candidate, rest := nextHeaderToken(value)
			value = rest
			if candidate == "" {
				continue
			}
			if !validToken(candidate) {
				return "", ErrProtocol
			}
			for _, supported := range h.config.Subprotocols {
				if candidate == supported && validToken(supported) {
					return candidate, nil
				}
			}
		}
	}
	return "", nil
}

func (h *ServerHandler) validateMinimalHandshake(data []byte) ([]byte, string, []byte, bool, error) {
	headerAt := bytes.Index(data, []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(data) > h.config.HTTP.MaxHeaderBytes {
			return nil, "", nil, false, errHandshakeHeaderTooLarge
		}
		return nil, "", nil, false, nil
	}
	headerEnd := headerAt + 4
	if headerEnd > h.config.HTTP.MaxHeaderBytes {
		return nil, "", nil, false, errHandshakeHeaderTooLarge
	}
	lineEnd := bytes.Index(data[:headerAt], []byte("\r\n"))
	if lineEnd <= len("GET  HTTP/1.1") || !bytes.HasPrefix(data[:lineEnd], []byte("GET ")) ||
		!bytes.HasSuffix(data[:lineEnd], []byte(" HTTP/1.1")) {
		return nil, "", nil, false, errMalformedHandshake
	}
	requestURI := data[len("GET ") : lineEnd-len(" HTTP/1.1")]
	if !validMinimalRequestURI(requestURI) {
		return nil, "", nil, false, errMalformedHandshake
	}

	var key []byte
	var subprotocol string
	hostSeen, connectionUpgrade, upgradeWebsocket, version13 := false, false, false, false
	for offset := lineEnd + 2; offset < headerAt; {
		relativeEnd := bytes.Index(data[offset:headerAt+2], []byte("\r\n"))
		if relativeEnd <= 0 {
			return nil, "", nil, false, errMalformedHandshake
		}
		line := data[offset : offset+relativeEnd]
		offset += relativeEnd + 2
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || line[0] == ' ' || line[0] == '\t' || !validTokenBytes(line[:colon]) {
			return nil, "", nil, false, errMalformedHandshake
		}
		name, value := line[:colon], trimOWS(line[colon+1:])
		if !validHeaderValue(value) {
			return nil, "", nil, false, errMalformedHandshake
		}
		switch {
		case bytes.EqualFold(name, []byte("Host")):
			if hostSeen || len(value) == 0 {
				return nil, "", nil, false, errMalformedHandshake
			}
			hostSeen = true
		case bytes.EqualFold(name, []byte("Connection")):
			connectionUpgrade = connectionUpgrade || byteHeaderHasToken(value, []byte("upgrade"))
		case bytes.EqualFold(name, []byte("Upgrade")):
			upgradeWebsocket = bytes.EqualFold(value, []byte("websocket"))
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Version")):
			version13 = bytes.Equal(value, []byte("13"))
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Key")):
			if key == nil {
				key = value
			}
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Protocol")):
			selected, valid := h.selectMinimalSubprotocol(value)
			if !valid {
				return nil, "", nil, false, ErrProtocol
			}
			if subprotocol == "" {
				subprotocol = selected
			}
		case bytes.EqualFold(name, []byte("Content-Length")), bytes.EqualFold(name, []byte("Transfer-Encoding")):
			if len(value) != 0 {
				return nil, "", nil, false, errMalformedHandshake
			}
		}
	}
	var decodedKey [16]byte
	n, decodeErr := base64.StdEncoding.Decode(decodedKey[:], key)
	if !hostSeen || !connectionUpgrade || !upgradeWebsocket || !version13 ||
		decodeErr != nil || len(key) != 24 || n != len(decodedKey) {
		return nil, "", nil, false, ErrProtocol
	}
	return key, subprotocol, data[headerEnd:], true, nil
}

func (h *ServerHandler) selectMinimalSubprotocol(value []byte) (string, bool) {
	for len(value) != 0 {
		candidate, rest := nextByteHeaderToken(value)
		value = rest
		if len(candidate) == 0 {
			continue
		}
		if !validTokenBytes(candidate) {
			return "", false
		}
		for _, supported := range h.config.Subprotocols {
			if validToken(supported) && bytes.Equal(candidate, []byte(supported)) {
				return supported, true
			}
		}
	}
	return "", true
}

func byteHeaderHasToken(value, token []byte) bool {
	for len(value) != 0 {
		candidate, rest := nextByteHeaderToken(value)
		if bytes.EqualFold(candidate, token) {
			return true
		}
		value = rest
	}
	return false
}

func nextByteHeaderToken(value []byte) (token, rest []byte) {
	if comma := bytes.IndexByte(value, ','); comma >= 0 {
		token, rest = value[:comma], value[comma+1:]
	} else {
		token = value
	}
	return trimOWS(token), rest
}

func validMinimalRequestURI(uri []byte) bool {
	if len(uri) == 0 || uri[0] != '/' {
		return false
	}
	for i, c := range uri {
		if c <= ' ' || c == 0x7f || c == '#' {
			return false
		}
		if c == '%' && (i+2 >= len(uri) || !isHex(uri[i+1]) || !isHex(uri[i+2])) {
			return false
		}
	}
	return true
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
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
	state, _ := c.Attachment().(*connectionState)
	c.SetAttachment(nil)
	if state == nil {
		return
	}
	h.releaseHandshakeParser(state)
	if state.upgraded {
		closeState := state.websocket.closeState.Load()
		code := uint16(1006)
		reason := ""
		if closeState != nil {
			code = closeState.code
			reason = closeState.reason
		}
		h.handler.OnClose(&state.websocket, code, reason, err)
	}
}

func (h *ServerHandler) releaseHandshakeParser(state *connectionState) {
	if state.handshake == nil {
		return
	}
	state.handshake.Reset()
	h.handshakeParsers.Put(state.handshake)
	state.handshake = nil
}

var handshakeResponsePrefix = []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ")

func sendHandshakeResponse(c *epoll.Connection, key []byte, subprotocol string) error {
	var accept [28]byte
	websocketAccept(accept[:], key)
	if subprotocol == "" {
		var tail [32]byte
		copy(tail[:], accept[:])
		copy(tail[len(accept):], "\r\n\r\n")
		return c.SendParts(handshakeResponsePrefix, tail[:])
	}
	response := make([]byte, 0, len(handshakeResponsePrefix)+len(accept)+31+len(subprotocol))
	response = append(response, handshakeResponsePrefix...)
	response = append(response, accept[:]...)
	response = append(response, "\r\nSec-WebSocket-Protocol: "...)
	response = append(response, subprotocol...)
	response = append(response, "\r\n\r\n"...)
	return c.SendOwned(response)
}

func websocketAccept(dst, key []byte) {
	var challenge [24 + len(websocketGUID)]byte
	n := copy(challenge[:], key)
	copy(challenge[n:], websocketGUID)
	sum := sha1.Sum(challenge[:n+len(websocketGUID)])
	base64.StdEncoding.Encode(dst, sum[:])
}

func headerHasToken(header stdhttp.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for len(value) != 0 {
			candidate, rest := nextHeaderToken(value)
			value = rest
			if strings.EqualFold(candidate, token) {
				return true
			}
		}
	}
	return false
}

func nextHeaderToken(value string) (token, rest string) {
	if comma := strings.IndexByte(value, ','); comma >= 0 {
		token, rest = value[:comma], value[comma+1:]
	} else {
		token = value
	}
	return strings.TrimSpace(token), rest
}

func closePayload(payload []byte) (uint16, string) {
	if len(payload) < 2 {
		return CloseNormal, ""
	}
	return binary.BigEndian.Uint16(payload[:2]), string(payload[2:])
}

var _ epoll.Handler = (*ServerHandler)(nil)
