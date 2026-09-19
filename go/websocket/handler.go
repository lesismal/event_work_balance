//go:build linux || darwin || windows

package websocket

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	fib "github.com/lesismal/fib/go"
	epollhttp "github.com/lesismal/fib/go/http"
)

const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Config struct {
	MaxMessageBytes int64
	Subprotocols    []string
	CheckOrigin     func(*stdhttp.Request) bool
	// EnableCompression accepts a client's offer of permessage-deflate (RFC
	// 7692). Messages are then sent compressed, and a client may send its own
	// compressed. MaxMessageBytes bounds a message once decompressed.
	EnableCompression bool
	HTTP              epollhttp.Config
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

// PingHandler is implemented by a Handler that handles pings itself. OnPing
// replaces the default reply, so it sends the pong itself, with
// Connection.Pong, if it wants one sent. A Handler that does not implement it
// has every ping answered with a pong carrying the same payload, as RFC 6455
// requires.
//
// The payload is valid only for the duration of the callback.
type PingHandler interface {
	OnPing(*Connection, []byte)
}

// PongHandler is implemented by a Handler that wants the pongs it receives,
// to measure round trips or to tell that a peer is alive. Without it pongs
// are discarded.
//
// The payload is valid only for the duration of the callback.
type PongHandler interface {
	OnPong(*Connection, []byte)
}

// HandlerFuncs implements Handler, PingHandler and PongHandler from whichever
// functions are set. A nil Ping keeps the default reply to pings, and a nil
// Pong discards pongs.
type HandlerFuncs struct {
	Open    func(*Connection, *stdhttp.Request)
	Message func(*Connection, Opcode, []byte)
	Close   func(*Connection, uint16, string, error)
	// Ping replaces the default reply to a ping: it has to call
	// Connection.Pong itself for a pong to be sent.
	Ping func(*Connection, []byte)
	Pong func(*Connection, []byte)
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
func (h HandlerFuncs) OnPing(c *Connection, payload []byte) {
	if h.Ping != nil {
		h.Ping(c, payload)
		return
	}
	_ = c.replyPing(payload)
}
func (h HandlerFuncs) OnPong(c *Connection, payload []byte) {
	if h.Pong != nil {
		h.Pong(c, payload)
	}
}

// Connection is a WebSocket connection. Its write methods are safe to call
// from application goroutines.
type Connection struct {
	conn        *fib.Connection
	subprotocol string
	// client marks the dialing side, whose frames must be masked.
	client bool
	// compress says permessage-deflate is in use, and windowBits bounds how
	// far back what this side sends may refer.
	compress   bool
	windowBits int
	closeSent  atomic.Bool
	closeState atomic.Pointer[connectionCloseState]
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

// Ping sends a ping. The peer answers with a pong carrying the same payload,
// which reaches a PongHandler. The payload is at most 125 bytes.
func (c *Connection) Ping(payload []byte) error { return c.writeFrame(Ping, payload) }

// Pong sends a pong. Pongs are sent on their own only to answer pings, which
// the default handling already does, or as an unsolicited heartbeat.
func (c *Connection) Pong(payload []byte) error { return c.writeFrame(Pong, payload) }

// replyPing is the default answer to a ping. A connection that cannot even
// send a pong is closed, and replyPing reports false.
func (c *Connection) replyPing(payload []byte) bool {
	if err := c.Pong(payload); err != nil {
		c.conn.Close()
		return false
	}
	return true
}

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
	if c.compress && (opcode == Text || opcode == Binary) {
		compressor, compressed := compressMessage(payload, c.windowBits)
		err := c.sendFrame(opcode, compressed, true)
		compressor.release()
		return err
	}
	return c.sendFrame(opcode, payload, false)
}

// sendFrame frames payload for the peer. A server's frames go out as they are.
// A client has to mask every frame with a key the server cannot predict (RFC
// 6455 section 5.3), and masking rewrites the payload, which is the caller's
// buffer and not this connection's to scramble, so a client frame is built in a
// buffer of its own. compressed sets RSV1, which marks a compressed message.
func (c *Connection) sendFrame(opcode Opcode, payload []byte, compressed bool) error {
	header, headerLen, err := frameHeader(opcode, len(payload))
	if err != nil {
		return err
	}
	if compressed {
		header[0] |= 0x40
	}
	if !c.client {
		return c.conn.SendParts(header[:headerLen], payload)
	}
	frame := make([]byte, headerLen+4+len(payload))
	copy(frame, header[:headerLen])
	frame[1] |= 0x80
	mask := frame[headerLen : headerLen+4]
	binary.LittleEndian.PutUint32(mask, rand.Uint32())
	applyMask(frame[headerLen+4:], payload, mask)
	return c.conn.SendOwned(frame)
}

func (c *Connection) sendClose(payload []byte) error {
	if !c.closeSent.CompareAndSwap(false, true) {
		return nil
	}
	if len(payload) > 125 {
		c.closeSent.Store(false)
		return ErrProtocol
	}
	code, reason := closePayload(payload)
	c.closeState.Store(&connectionCloseState{code: code, reason: reason})
	if err := c.sendFrame(Close, payload, false); err != nil {
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

func (h *ServerHandler) OnOpen(c *fib.Connection) {
	c.SetAttachment(&connectionState{})
}

func (h *ServerHandler) OnData(c *fib.Connection, data []byte) {
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
		key, result, remainder, complete, err := h.validateMinimalHandshake(data)
		if err != nil {
			h.reject(c, nil, stdhttp.StatusBadRequest)
			return
		}
		if complete {
			h.upgrade(c, state, nil, key, result, remainder)
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
	result, err := h.validateHandshake(request)
	if err != nil {
		h.reject(c, request, stdhttp.StatusBadRequest)
		return
	}
	key := []byte(request.Header.Get("Sec-Websocket-Key"))
	h.upgrade(c, state, request, key, result, remainder)
}

// handshakeResult is what the server agreed to in the opening handshake.
type handshakeResult struct {
	subprotocol string
	// extensions is the Sec-WebSocket-Extensions response, empty when no
	// extension is in use.
	extensions string
	deflate    deflateParams
}

// negotiateExtensions accepts what the server supports of the client's offers.
func (h *ServerHandler) negotiateExtensions(result *handshakeResult, offers []string) {
	if h.config.EnableCompression && len(offers) != 0 {
		result.extensions, result.deflate = acceptDeflateOffer(offers)
	}
}

func (h *ServerHandler) upgrade(c *fib.Connection, state *connectionState, request *stdhttp.Request, key []byte, result handshakeResult, remainder []byte) {
	if err := sendHandshakeResponse(c, key, result); err != nil {
		c.Close()
		return
	}
	compress := result.extensions != ""
	state.websocket = Connection{conn: c, subprotocol: result.subprotocol, compress: compress,
		windowBits: result.deflate.windowBits}
	state.wsParser.maxMessageBytes = h.config.MaxMessageBytes
	state.wsParser.deflate = compress
	state.wsParser.contextTakeover = compress && result.deflate.peerContextTakeover
	state.upgraded = true
	if state.handshake != nil && remainder == nil {
		remainder = state.handshake.TakeBuffered()
	}
	h.releaseHandshakeParser(state)
	if request != nil {
		if addr := c.RemoteAddr(); addr != nil {
			request.RemoteAddr = addr.String()
		}
		h.handler.OnOpen(&state.websocket, request)
	}
	if len(remainder) != 0 {
		h.handleFrames(state, remainder)
	}
}

func (h *ServerHandler) handleFrames(state *connectionState, data []byte) {
	serveFrames(h.handler, &state.websocket, &state.wsParser, data)
}

// serveFrames feeds bytes from the peer through parser and acts on every frame
// they complete: messages go to handler, pings are answered or handed to a
// PingHandler, pongs go to a PongHandler, and a close is echoed. Both ends of
// a connection run it; they differ only in the parser's direction and in
// whether ws masks what it sends.
func serveFrames(handler Handler, ws *Connection, parser *Parser, data []byte) {
	defer parser.ReleaseBorrowed()
	for {
		event, complete, err := parser.FeedOneBorrowed(data)
		data = nil
		if err != nil {
			closeParserError(ws, err)
			return
		}
		if !complete {
			return
		}
		switch event.Opcode {
		case Text, Binary:
			handler.OnMessage(ws, event.Opcode, event.Payload)
		case Ping:
			if pinged, ok := handler.(PingHandler); ok {
				pinged.OnPing(ws, event.Payload)
			} else if !ws.replyPing(event.Payload) {
				return
			}
		case Pong:
			if ponged, ok := handler.(PongHandler); ok {
				ponged.OnPong(ws, event.Payload)
			}
		case Close:
			_ = ws.sendClose(event.Payload)
			return
		}
	}
}

func closeParserError(ws *Connection, err error) {
	code := uint16(CloseProtocolError)
	if errors.Is(err, ErrMessageTooBig) {
		code = CloseMessageTooBig
	} else if errors.Is(err, ErrInvalidPayload) {
		code = CloseInvalidPayload
	}
	var payload [2]byte
	binary.BigEndian.PutUint16(payload[:], code)
	_ = ws.sendClose(payload[:])
}

func (h *ServerHandler) validateHandshake(request *stdhttp.Request) (handshakeResult, error) {
	var result handshakeResult
	var err error
	result.subprotocol, err = h.validateHandshakeRequest(request)
	if err != nil {
		return handshakeResult{}, err
	}
	h.negotiateExtensions(&result, request.Header.Values("Sec-Websocket-Extensions"))
	return result, nil
}

func (h *ServerHandler) validateHandshakeRequest(request *stdhttp.Request) (string, error) {
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

func (h *ServerHandler) validateMinimalHandshake(data []byte) ([]byte, handshakeResult, []byte, bool, error) {
	headerAt := bytes.Index(data, []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(data) > h.config.HTTP.MaxHeaderBytes {
			return nil, handshakeResult{}, nil, false, errHandshakeHeaderTooLarge
		}
		return nil, handshakeResult{}, nil, false, nil
	}
	headerEnd := headerAt + 4
	if headerEnd > h.config.HTTP.MaxHeaderBytes {
		return nil, handshakeResult{}, nil, false, errHandshakeHeaderTooLarge
	}
	lineEnd := bytes.Index(data[:headerAt], []byte("\r\n"))
	if lineEnd <= len("GET  HTTP/1.1") || !bytes.HasPrefix(data[:lineEnd], []byte("GET ")) ||
		!bytes.HasSuffix(data[:lineEnd], []byte(" HTTP/1.1")) {
		return nil, handshakeResult{}, nil, false, errMalformedHandshake
	}
	requestURI := data[len("GET ") : lineEnd-len(" HTTP/1.1")]
	if !validMinimalRequestURI(requestURI) {
		return nil, handshakeResult{}, nil, false, errMalformedHandshake
	}

	var key []byte
	var subprotocol string
	var offers []string
	hostSeen, connectionUpgrade, upgradeWebsocket, version13 := false, false, false, false
	for offset := lineEnd + 2; offset < headerAt; {
		relativeEnd := bytes.Index(data[offset:headerAt+2], []byte("\r\n"))
		if relativeEnd <= 0 {
			return nil, handshakeResult{}, nil, false, errMalformedHandshake
		}
		line := data[offset : offset+relativeEnd]
		offset += relativeEnd + 2
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || line[0] == ' ' || line[0] == '\t' || !validTokenBytes(line[:colon]) {
			return nil, handshakeResult{}, nil, false, errMalformedHandshake
		}
		name, value := line[:colon], trimOWS(line[colon+1:])
		if !validHeaderValue(value) {
			return nil, handshakeResult{}, nil, false, errMalformedHandshake
		}
		switch {
		case bytes.EqualFold(name, []byte("Host")):
			if hostSeen || len(value) == 0 {
				return nil, handshakeResult{}, nil, false, errMalformedHandshake
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
				return nil, handshakeResult{}, nil, false, ErrProtocol
			}
			if subprotocol == "" {
				subprotocol = selected
			}
		case bytes.EqualFold(name, []byte("Sec-WebSocket-Extensions")):
			if h.config.EnableCompression {
				offers = append(offers, string(value))
			}
		case bytes.EqualFold(name, []byte("Content-Length")), bytes.EqualFold(name, []byte("Transfer-Encoding")):
			if len(value) != 0 {
				return nil, handshakeResult{}, nil, false, errMalformedHandshake
			}
		}
	}
	var decodedKey [16]byte
	n, decodeErr := base64.StdEncoding.Decode(decodedKey[:], key)
	if !hostSeen || !connectionUpgrade || !upgradeWebsocket || !version13 ||
		decodeErr != nil || len(key) != 24 || n != len(decodedKey) {
		return nil, handshakeResult{}, nil, false, ErrProtocol
	}
	result := handshakeResult{subprotocol: subprotocol}
	h.negotiateExtensions(&result, offers)
	return key, result, data[headerEnd:], true, nil
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

func (h *ServerHandler) reject(c *fib.Connection, request *stdhttp.Request, status int) {
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

func (h *ServerHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *ServerHandler) OnClose(c *fib.Connection, err error) {
	state, _ := c.Attachment().(*connectionState)
	c.SetAttachment(nil)
	if state == nil {
		return
	}
	h.releaseHandshakeParser(state)
	if state.upgraded {
		code, reason := state.websocket.closeStatus()
		h.handler.OnClose(&state.websocket, code, reason, err)
	}
}

// closeStatus is the code and reason OnClose reports: those of the close frame
// this end sent, which echoes the peer's when the peer closed first, or 1006
// when the connection ended without a close handshake.
func (c *Connection) closeStatus() (uint16, string) {
	if closeState := c.closeState.Load(); closeState != nil {
		return closeState.code, closeState.reason
	}
	return 1006, ""
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

func sendHandshakeResponse(c *fib.Connection, key []byte, result handshakeResult) error {
	var accept [28]byte
	websocketAccept(accept[:], key)
	subprotocol := result.subprotocol
	if subprotocol == "" && result.extensions == "" {
		var tail [32]byte
		copy(tail[:], accept[:])
		copy(tail[len(accept):], "\r\n\r\n")
		return c.SendParts(handshakeResponsePrefix, tail[:])
	}
	response := make([]byte, 0, len(handshakeResponsePrefix)+len(accept)+60+len(subprotocol)+len(result.extensions))
	response = append(response, handshakeResponsePrefix...)
	response = append(response, accept[:]...)
	if subprotocol != "" {
		response = append(response, "\r\nSec-WebSocket-Protocol: "...)
		response = append(response, subprotocol...)
	}
	if result.extensions != "" {
		response = append(response, "\r\nSec-WebSocket-Extensions: "...)
		response = append(response, result.extensions...)
	}
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

var _ fib.Handler = (*ServerHandler)(nil)
