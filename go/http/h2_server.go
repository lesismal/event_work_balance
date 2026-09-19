//go:build linux || darwin || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/http/internal/hpack"
)

// HTTP/2 on the server side. ServerHandler recognizes a connection that
// starts with the HTTP/2 preface, which is what a client sends first both
// after negotiating "h2" through TLS ALPN and when it speaks cleartext HTTP/2
// with prior knowledge (h2c), and from then on the connection is served by an
// h2ServerConn. The handler sees each stream as an ordinary request.

const (
	// h2StreamWindow is the receive window each stream is granted, and
	// h2ConnWindow the one the whole connection is, by servers and clients
	// alike. Bodies are buffered whole, so these only pace the peer; the
	// body limits bound what is kept.
	h2StreamWindow = 1 << 20
	h2ConnWindow   = 1 << 24
	// DefaultMaxConcurrentStreams is how many requests a client may have open
	// on one connection when Config leaves MaxConcurrentStreams at zero.
	DefaultMaxConcurrentStreams = 250
)

var errH2StreamClosed = errors.New("http2: stream closed")

// ConfigureTLS returns a copy of config whose ALPN offers HTTP/2 ahead of
// HTTP/1.1, which is what makes TLS clients choose HTTP/2. A server serves
// both over the same handler either way; this only lets clients know.
func ConfigureTLS(config *stdtls.Config) *stdtls.Config {
	if config == nil {
		config = &stdtls.Config{}
	}
	config = config.Clone()
	protos := []string{"h2", "http/1.1"}
	for _, p := range config.NextProtos {
		if p != "h2" && p != "http/1.1" {
			protos = append(protos, p)
		}
	}
	config.NextProtos = protos
	return config
}

// h2ServerConn is one HTTP/2 connection on the server side.
type h2ServerConn struct {
	handler    *ServerHandler
	conn       *fib.Connection
	remoteAddr string
	maxStreams uint32

	// The read side is only touched by the worker running OnData.
	in          []byte
	gotPreface  bool
	gotSettings bool
	dec         *hpack.Decoder
	// contStream is the stream whose header block is still arriving in
	// CONTINUATION frames, or zero; contBlock holds what arrived so far.
	contStream    uint32
	contEndStream bool
	contBlock     []byte
	// maxClientID is the highest stream the client has opened.
	maxClientID uint32
	// recvWindow is what the client may still send before the next
	// WINDOW_UPDATE, and recvUnacked what it sent since the last one.
	recvWindow  int64
	recvUnacked int64

	// mu guards the write side and the streams, which writers and the reader
	// share. Frames are sent while holding it, so header blocks leave in the
	// order the encoder produced them.
	mu            sync.Mutex
	streams       map[uint32]*h2ServerStream
	enc           *hpack.Encoder
	peerMaxFrame  int
	peerWindow    int64
	sendWindow    int64
	goAway        bool
	peerGoingAway bool
	closed        bool
	// lastStreamID is the highest client stream accepted, which GOAWAY
	// reports as the last one this side will process.
	lastStreamID uint32
	// Server push: whether the client allows it, the next promised stream,
	// how many pushed streams the client allows open at once, and how many
	// are.
	pushEnabled   bool
	nextPushID    uint32
	peerMaxPush   uint32
	pushedStreams uint32
	// tlsState is the connection's TLS state, handed to every request, or
	// nil in cleartext.
	tlsState *stdtls.ConnectionState
}

// h2ServerStream is one request and its response.
type h2ServerStream struct {
	sc  *h2ServerConn
	id  uint32
	req *stdhttp.Request
	// pushed marks a stream this side promised, which carries a response
	// only; the request it answers came in PUSH_PROMISE.
	pushed bool
	// The reader alone touches body, declared and the receive window.
	body        []byte
	declared    int64
	recvWindow  int64
	recvUnacked int64
	// The rest is guarded by the connection's mu.
	remoteDone bool
	responded  bool
	localDone  bool
	reset      bool
	sendWindow int64
	pending    []byte
}

func newH2ServerConn(h *ServerHandler, c *fib.Connection, remoteAddr string) *h2ServerConn {
	maxStreams := h.config.MaxConcurrentStreams
	if maxStreams == 0 {
		maxStreams = DefaultMaxConcurrentStreams
	}
	dec := hpack.NewDecoder(hpack.DefaultTableSize)
	dec.MaxStringLength = h.config.MaxHeaderBytes
	return &h2ServerConn{
		handler:      h,
		conn:         c,
		remoteAddr:   remoteAddr,
		maxStreams:   maxStreams,
		dec:          dec,
		recvWindow:   h2ConnWindow,
		streams:      make(map[uint32]*h2ServerStream),
		enc:          hpack.NewEncoder(),
		peerMaxFrame: h2DefaultMaxFrameSize,
		peerWindow:   h2DefaultWindow,
		sendWindow:   h2DefaultWindow,
		pushEnabled:  true,
		nextPushID:   2,
		peerMaxPush:  math.MaxUint32,
	}
}

// start sends the server's connection preface: its SETTINGS, and a window
// update raising the connection's receive window past the default.
func (sc *h2ServerConn) start() {
	out := h2AppendSettings(nil,
		[2]uint32{uint32(h2SettingMaxConcurrentStreams), sc.maxStreams},
		[2]uint32{uint32(h2SettingInitialWindowSize), h2StreamWindow},
		[2]uint32{uint32(h2SettingMaxHeaderListSize), uint32(sc.handler.config.MaxHeaderBytes)},
		[2]uint32{uint32(h2SettingEnablePush), 0},
	)
	out = h2AppendWindowUpdate(out, 0, h2ConnWindow-h2DefaultWindow)
	sc.mu.Lock()
	sc.sendLocked(out)
	sc.mu.Unlock()
}

// sendLocked sends out unless the connection has ended.
func (sc *h2ServerConn) sendLocked(out []byte) {
	if len(out) > 0 && !sc.closed {
		_ = sc.conn.SendOwned(out)
	}
}

// feed takes bytes from the connection, preface included, and handles every
// complete frame among them.
func (sc *h2ServerConn) feed(data []byte) {
	sc.in = append(sc.in, data...)
	offset := 0
	if !sc.gotPreface {
		if len(sc.in) < len(h2Preface) {
			return
		}
		if string(sc.in[:len(h2Preface)]) != h2Preface {
			sc.fail(h2ConnErr(H2ProtocolError, "bad connection preface"))
			return
		}
		sc.gotPreface = true
		offset = len(h2Preface)
	}
	for {
		f, n, err := h2ReadFrame(sc.in[offset:], h2DefaultMaxFrameSize)
		if err == nil && n > 0 {
			offset += n
			err = sc.handleFrame(&f)
		}
		if err != nil {
			sc.handleError(err)
			if sc.isClosed() {
				sc.in = nil
				return
			}
			continue
		}
		if n == 0 {
			break
		}
	}
	if offset == len(sc.in) {
		if cap(sc.in) > maxRetainedBuffer {
			sc.in = nil
		} else {
			sc.in = sc.in[:0]
		}
	} else if offset > 0 {
		sc.in = append(sc.in[:0], sc.in[offset:]...)
	}
}

// handleError resets a stream for a stream error and ends the connection for
// anything else.
func (sc *h2ServerConn) handleError(err error) {
	var streamErr *H2StreamError
	if errors.As(err, &streamErr) {
		sc.resetStream(streamErr.StreamID, streamErr.Code)
		return
	}
	var connErr *H2ConnError
	if !errors.As(err, &connErr) {
		connErr = h2ConnErr(H2InternalError, "%v", err)
	}
	sc.fail(connErr)
}

func (sc *h2ServerConn) isClosed() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.closed
}

// fail ends the connection with GOAWAY.
func (sc *h2ServerConn) fail(err *H2ConnError) {
	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		return
	}
	sc.sendLocked(h2AppendGoAway(nil, sc.lastStreamID, err.Code, err.Reason))
	sc.closed = true
	sc.goAway = true
	clear(sc.streams)
	sc.mu.Unlock()
	sc.conn.CloseAfterSend()
}

// shutdown is the connection closing underneath.
func (sc *h2ServerConn) shutdown() {
	sc.mu.Lock()
	sc.closed = true
	clear(sc.streams)
	sc.mu.Unlock()
}

func (sc *h2ServerConn) handleFrame(f *h2Frame) error {
	if sc.contStream != 0 && (f.typ != h2FrameContinuation || f.streamID != sc.contStream) {
		return h2ConnErr(H2ProtocolError, "expected CONTINUATION for stream %d", sc.contStream)
	}
	if !sc.gotSettings {
		if f.typ != h2FrameSettings || f.has(h2FlagAck) {
			return h2ConnErr(H2ProtocolError, "connection preface must be followed by SETTINGS")
		}
		sc.gotSettings = true
	}
	switch f.typ {
	case h2FrameData:
		return sc.handleData(f)
	case h2FrameHeaders:
		return sc.handleHeaders(f)
	case h2FrameContinuation:
		return sc.handleContinuation(f)
	case h2FramePriority:
		if f.streamID == 0 {
			return h2ConnErr(H2ProtocolError, "PRIORITY on stream 0")
		}
		if len(f.payload) != 5 {
			return &H2StreamError{StreamID: f.streamID, Code: H2FrameSizeError}
		}
		return nil
	case h2FrameRSTStream:
		return sc.handleRSTStream(f)
	case h2FrameSettings:
		return sc.handleSettings(f)
	case h2FramePushPromise:
		return h2ConnErr(H2ProtocolError, "client sent PUSH_PROMISE")
	case h2FramePing:
		if f.streamID != 0 {
			return h2ConnErr(H2ProtocolError, "PING on stream %d", f.streamID)
		}
		if len(f.payload) != 8 {
			return h2ConnErr(H2FrameSizeError, "PING length %d", len(f.payload))
		}
		if !f.has(h2FlagAck) {
			out := h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8)
			out = append(out, f.payload...)
			sc.mu.Lock()
			sc.sendLocked(out)
			sc.mu.Unlock()
		}
		return nil
	case h2FrameGoAway:
		if f.streamID != 0 {
			return h2ConnErr(H2ProtocolError, "GOAWAY on stream %d", f.streamID)
		}
		if len(f.payload) < 8 {
			return h2ConnErr(H2FrameSizeError, "GOAWAY length %d", len(f.payload))
		}
		// The client opens nothing more; finish what is open, then close.
		sc.mu.Lock()
		sc.peerGoingAway = true
		sc.mu.Unlock()
		sc.closeIfDone()
		return nil
	case h2FrameWindowUpdate:
		return sc.handleWindowUpdate(f)
	}
	// Unknown frame types are ignored (RFC 9113 section 4.1).
	return nil
}

func (sc *h2ServerConn) handleSettings(f *h2Frame) error {
	if f.streamID != 0 {
		return h2ConnErr(H2ProtocolError, "SETTINGS on stream %d", f.streamID)
	}
	if f.has(h2FlagAck) {
		if len(f.payload) != 0 {
			return h2ConnErr(H2FrameSizeError, "SETTINGS ack with payload")
		}
		return nil
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if err := sc.applySettingsLocked(f.payload); err != nil {
		return err
	}
	out := h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0)
	out = sc.flushLocked(out)
	sc.sendLocked(out)
	sc.closeFinishedLocked()
	return nil
}

// applySettingsLocked applies the client's settings, from a SETTINGS frame
// or from the HTTP2-Settings header of an h2c upgrade.
func (sc *h2ServerConn) applySettingsLocked(payload []byte) error {
	return h2ParseSettings(payload, func(id h2SettingID, value uint32) error {
		switch id {
		case h2SettingHeaderTableSize:
			sc.enc.SetMaxTableSize(int(min(value, hpack.DefaultTableSize)))
		case h2SettingEnablePush:
			if value > 1 {
				return h2ConnErr(H2ProtocolError, "ENABLE_PUSH %d", value)
			}
			sc.pushEnabled = value == 1
		case h2SettingMaxConcurrentStreams:
			sc.peerMaxPush = value
		case h2SettingInitialWindowSize:
			if value > h2MaxWindow {
				return h2ConnErr(H2FlowControlError, "INITIAL_WINDOW_SIZE %d", value)
			}
			delta := int64(value) - sc.peerWindow
			sc.peerWindow = int64(value)
			for _, st := range sc.streams {
				st.sendWindow += delta
				if st.sendWindow > h2MaxWindow {
					return h2ConnErr(H2FlowControlError, "stream window overflow")
				}
			}
		case h2SettingMaxFrameSize:
			if value < h2DefaultMaxFrameSize || value > h2MaxFrameSizeLimit {
				return h2ConnErr(H2ProtocolError, "MAX_FRAME_SIZE %d", value)
			}
			sc.peerMaxFrame = int(value)
		}
		return nil
	})
}

// idleLocked reports whether a stream has not been opened yet, by the client
// for odd ids or by a push for even ones. Frames other than HEADERS on such a
// stream are a connection error.
func (sc *h2ServerConn) idleLocked(id uint32) bool {
	if id%2 == 0 {
		return id >= sc.nextPushID
	}
	return id > sc.maxClientID
}

func (sc *h2ServerConn) handleWindowUpdate(f *h2Frame) error {
	if len(f.payload) != 4 {
		return h2ConnErr(H2FrameSizeError, "WINDOW_UPDATE length %d", len(f.payload))
	}
	increment := int64(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if f.streamID == 0 {
		if increment == 0 {
			return h2ConnErr(H2ProtocolError, "WINDOW_UPDATE of 0")
		}
		sc.sendWindow += increment
		if sc.sendWindow > h2MaxWindow {
			return h2ConnErr(H2FlowControlError, "connection window overflow")
		}
	} else {
		if sc.idleLocked(f.streamID) {
			return h2ConnErr(H2ProtocolError, "WINDOW_UPDATE on idle stream %d", f.streamID)
		}
		st := sc.streams[f.streamID]
		if st == nil {
			return nil
		}
		if increment == 0 {
			return &H2StreamError{StreamID: f.streamID, Code: H2ProtocolError}
		}
		st.sendWindow += increment
		if st.sendWindow > h2MaxWindow {
			return &H2StreamError{StreamID: f.streamID, Code: H2FlowControlError}
		}
	}
	sc.sendLocked(sc.flushLocked(nil))
	sc.closeFinishedLocked()
	return nil
}

func (sc *h2ServerConn) handleRSTStream(f *h2Frame) error {
	if f.streamID == 0 {
		return h2ConnErr(H2ProtocolError, "RST_STREAM on stream 0")
	}
	if len(f.payload) != 4 {
		return h2ConnErr(H2FrameSizeError, "RST_STREAM length %d", len(f.payload))
	}
	sc.mu.Lock()
	if sc.idleLocked(f.streamID) {
		sc.mu.Unlock()
		return h2ConnErr(H2ProtocolError, "RST_STREAM on idle stream %d", f.streamID)
	}
	if st := sc.streams[f.streamID]; st != nil {
		sc.forgetLocked(st)
	}
	sc.mu.Unlock()
	sc.closeIfDone()
	return nil
}

func (sc *h2ServerConn) handleData(f *h2Frame) error {
	if f.streamID == 0 {
		return h2ConnErr(H2ProtocolError, "DATA on stream 0")
	}
	length := int64(f.length)
	if length > sc.recvWindow {
		return h2ConnErr(H2FlowControlError, "DATA exceeds connection window")
	}
	sc.recvWindow -= length
	sc.recvUnacked += length
	if f.streamID%2 == 0 {
		return h2ConnErr(H2ProtocolError, "DATA on server stream %d", f.streamID)
	}
	if f.streamID > sc.maxClientID {
		return h2ConnErr(H2ProtocolError, "DATA on idle stream %d", f.streamID)
	}
	var out []byte
	if sc.recvUnacked >= h2ConnWindow/2 {
		out = h2AppendWindowUpdate(out, 0, uint32(sc.recvUnacked))
		sc.recvWindow += sc.recvUnacked
		sc.recvUnacked = 0
	}
	sc.mu.Lock()
	st := sc.streams[f.streamID]
	remoteDone := st != nil && st.remoteDone
	sc.sendLocked(out)
	sc.mu.Unlock()
	if st == nil {
		// A stream this side has already reset or finished: what was in
		// flight is dropped, and only counts against the connection window.
		return nil
	}
	if remoteDone {
		return &H2StreamError{StreamID: f.streamID, Code: H2StreamClosed}
	}
	if length > st.recvWindow {
		return &H2StreamError{StreamID: f.streamID, Code: H2FlowControlError}
	}
	st.recvWindow -= length
	st.recvUnacked += length
	if int64(len(st.body))+int64(len(f.payload)) > sc.handler.config.MaxBodyBytes {
		sc.reject(st, stdhttp.StatusRequestEntityTooLarge)
		return nil
	}
	st.body = append(st.body, f.payload...)
	if f.has(h2FlagEndStream) {
		return sc.finishRequest(st)
	}
	if st.recvUnacked >= h2StreamWindow/2 {
		increment := st.recvUnacked
		st.recvWindow += increment
		st.recvUnacked = 0
		sc.mu.Lock()
		sc.sendLocked(h2AppendWindowUpdate(nil, st.id, uint32(increment)))
		sc.mu.Unlock()
	}
	return nil
}

func (sc *h2ServerConn) handleHeaders(f *h2Frame) error {
	if f.streamID == 0 || f.streamID%2 == 0 {
		return h2ConnErr(H2ProtocolError, "HEADERS on stream %d", f.streamID)
	}
	block := f.payload
	if f.has(h2FlagPriority) {
		if len(block) < 5 {
			return h2ConnErr(H2FrameSizeError, "HEADERS priority truncated")
		}
		if binary.BigEndian.Uint32(block)&0x7fffffff == f.streamID {
			return &H2StreamError{StreamID: f.streamID, Code: H2ProtocolError}
		}
		block = block[5:]
	}
	if !f.has(h2FlagEndHeaders) {
		if len(block) > sc.handler.config.MaxHeaderBytes {
			return h2ConnErr(H2EnhanceYourCalm, "header block too large")
		}
		sc.contStream = f.streamID
		sc.contEndStream = f.has(h2FlagEndStream)
		sc.contBlock = append(sc.contBlock[:0], block...)
		return nil
	}
	return sc.handleHeaderBlock(f.streamID, block, f.has(h2FlagEndStream))
}

func (sc *h2ServerConn) handleContinuation(f *h2Frame) error {
	if sc.contStream == 0 {
		return h2ConnErr(H2ProtocolError, "unexpected CONTINUATION")
	}
	sc.contBlock = append(sc.contBlock, f.payload...)
	if len(sc.contBlock) > sc.handler.config.MaxHeaderBytes {
		return h2ConnErr(H2EnhanceYourCalm, "header block too large")
	}
	if !f.has(h2FlagEndHeaders) {
		return nil
	}
	id, block := sc.contStream, sc.contBlock
	sc.contStream = 0
	err := sc.handleHeaderBlock(id, block, sc.contEndStream)
	if cap(sc.contBlock) > maxRetainedBuffer {
		sc.contBlock = nil
	}
	return err
}

// handleHeaderBlock decodes a complete header block: a new request, or the
// trailers of one whose body has arrived.
func (sc *h2ServerConn) handleHeaderBlock(id uint32, block []byte, endStream bool) error {
	var fields []hpack.HeaderField
	listSize := 0
	tooLarge := false
	// The block is always decoded, whatever becomes of the stream, so that the
	// dynamic table stays in step with the client's.
	err := sc.dec.Decode(block, func(f hpack.HeaderField) error {
		listSize += len(f.Name) + len(f.Value) + 32
		if listSize > sc.handler.config.MaxHeaderBytes {
			tooLarge = true
		}
		if !tooLarge {
			fields = append(fields, f)
		}
		return nil
	})
	if err != nil {
		return h2ConnErr(H2CompressionError, "%v", err)
	}

	sc.mu.Lock()
	st := sc.streams[id]
	sc.mu.Unlock()
	if st != nil {
		return sc.handleTrailers(st, fields, endStream, tooLarge)
	}
	if id <= sc.maxClientID {
		// Trailers of a stream already reset or answered: nothing to do.
		return nil
	}
	sc.maxClientID = id

	sc.mu.Lock()
	switch {
	case sc.closed || sc.goAway || sc.peerGoingAway:
		sc.mu.Unlock()
		return nil
	case uint32(len(sc.streams))-sc.pushedStreams >= sc.maxStreams:
		sc.mu.Unlock()
		return &H2StreamError{StreamID: id, Code: H2RefusedStream}
	}
	st = &h2ServerStream{sc: sc, id: id, recvWindow: h2StreamWindow, sendWindow: sc.peerWindow, declared: -1}
	sc.streams[id] = st
	sc.lastStreamID = id
	sc.mu.Unlock()

	if tooLarge {
		sc.reject(st, stdhttp.StatusRequestHeaderFieldsTooLarge)
		return nil
	}
	req, err := sc.newRequest(fields)
	if err != nil {
		return &H2StreamError{StreamID: id, Code: H2ProtocolError}
	}
	st.req = req
	if cl := req.Header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return &H2StreamError{StreamID: id, Code: H2ProtocolError}
		}
		if n > sc.handler.config.MaxBodyBytes {
			sc.reject(st, stdhttp.StatusRequestEntityTooLarge)
			return nil
		}
		st.declared = n
	}
	if endStream {
		return sc.finishRequest(st)
	}
	if strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		// The body is read whole before the handler runs, so there is no
		// reason to keep the client waiting for permission to send it.
		_ = st.writeInterim(stdhttp.StatusContinue, nil)
	}
	return nil
}

func (sc *h2ServerConn) handleTrailers(st *h2ServerStream, fields []hpack.HeaderField, endStream, tooLarge bool) error {
	sc.mu.Lock()
	remoteDone := st.remoteDone
	sc.mu.Unlock()
	if remoteDone {
		return &H2StreamError{StreamID: st.id, Code: H2StreamClosed}
	}
	if !endStream || st.req == nil {
		return &H2StreamError{StreamID: st.id, Code: H2ProtocolError}
	}
	if tooLarge {
		sc.reject(st, stdhttp.StatusRequestHeaderFieldsTooLarge)
		return nil
	}
	trailer := make(stdhttp.Header)
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") || !h2ValidHeaderName(f.Name) || !h2ValidHeaderValue(f.Value) {
			return &H2StreamError{StreamID: st.id, Code: H2ProtocolError}
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		trailer[key] = append(trailer[key], f.Value)
	}
	st.req.Trailer = trailer
	return sc.finishRequest(st)
}

// newRequest builds a request from a stream's header fields, rejecting what
// RFC 9113 section 8.3 calls malformed.
func (sc *h2ServerConn) newRequest(fields []hpack.HeaderField) (*stdhttp.Request, error) {
	var method, scheme, authority, path string
	var seen [4]bool
	header := make(stdhttp.Header, len(fields))
	var cookies []string
	regular := false
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			if regular {
				return nil, errors.New("pseudo-header after regular header")
			}
			var slot int
			switch f.Name {
			case ":method":
				slot, method = 0, f.Value
			case ":scheme":
				slot, scheme = 1, f.Value
			case ":authority":
				slot, authority = 2, f.Value
			case ":path":
				slot, path = 3, f.Value
			default:
				return nil, fmt.Errorf("unknown pseudo-header %q", f.Name)
			}
			if seen[slot] {
				return nil, fmt.Errorf("duplicate %s", f.Name)
			}
			seen[slot] = true
			continue
		}
		regular = true
		if !h2ValidHeaderName(f.Name) || !h2ValidHeaderValue(f.Value) || h2ConnectionHeaders[f.Name] {
			return nil, fmt.Errorf("invalid header %q", f.Name)
		}
		if f.Name == "te" && f.Value != "trailers" {
			return nil, errors.New("TE other than trailers")
		}
		if f.Name == "cookie" {
			cookies = append(cookies, f.Value)
			continue
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		header[key] = append(header[key], f.Value)
	}
	if len(cookies) > 0 {
		// HTTP/2 lets cookies arrive as separate fields; HTTP/1 handlers
		// expect one (RFC 9113 section 8.2.3).
		header["Cookie"] = []string{strings.Join(cookies, "; ")}
	}
	if method == "" {
		return nil, errors.New("missing :method")
	}
	req := &stdhttp.Request{
		Method:     method,
		Proto:      "HTTP/2.0",
		ProtoMajor: 2,
		Header:     header,
		RemoteAddr: sc.remoteAddr,
		TLS:        sc.tlsState,
		Body:       stdhttp.NoBody,
	}
	if method == stdhttp.MethodConnect {
		if scheme != "" || path != "" || authority == "" {
			return nil, errors.New("malformed CONNECT")
		}
		req.URL = &url.URL{Host: authority}
		req.RequestURI = authority
	} else {
		if scheme == "" || path == "" {
			return nil, errors.New("missing :scheme or :path")
		}
		u, err := url.ParseRequestURI(path)
		if path == "*" {
			u, err = &url.URL{Path: "*"}, nil
		}
		if err != nil {
			return nil, err
		}
		req.URL = u
		req.RequestURI = path
	}
	req.Host = authority
	if req.Host == "" {
		req.Host = header.Get("Host")
	}
	header.Del("Host")
	return req, nil
}

// finishRequest runs the handler for a stream whose request has arrived
// whole.
func (sc *h2ServerConn) finishRequest(st *h2ServerStream) error {
	if st.declared >= 0 && st.declared != int64(len(st.body)) {
		return &H2StreamError{StreamID: st.id, Code: H2ProtocolError}
	}
	sc.mu.Lock()
	st.remoteDone = true
	done := st.localDone
	if done {
		sc.forgetLocked(st)
	}
	sc.mu.Unlock()
	if done || st.req == nil {
		return nil
	}
	req := st.req
	req.ContentLength = int64(len(st.body))
	if len(st.body) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(st.body))
	}
	st.body = nil
	sc.handler.handler.ServeHTTP(&Context{Conn: sc.conn, Request: req, stream: st}, req)
	return nil
}

// reject answers a stream with an error status before its request is
// complete, and stops reading it.
func (sc *h2ServerConn) reject(st *h2ServerStream, status int) {
	st.body = nil
	req := st.req
	if req == nil {
		req = &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 2, Header: make(stdhttp.Header)}
	}
	_ = st.respond(req, Response{
		StatusCode: status,
		Header:     stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}},
		Body:       []byte(stdhttp.StatusText(status) + "\n"),
	})
}

// resetStream ends a stream with RST_STREAM.
func (sc *h2ServerConn) resetStream(id uint32, code H2ErrorCode) {
	sc.mu.Lock()
	if st := sc.streams[id]; st != nil {
		sc.forgetLocked(st)
	}
	sc.sendLocked(h2AppendRSTStream(nil, id, code))
	sc.mu.Unlock()
	sc.closeIfDone()
}

// closeIfDone closes a connection that is going away once its last stream
// has finished.
func (sc *h2ServerConn) closeIfDone() {
	sc.mu.Lock()
	sc.closeFinishedLocked()
	sc.mu.Unlock()
}

func (sc *h2ServerConn) closeFinishedLocked() {
	if sc.closed || !sc.goAway && !sc.peerGoingAway || len(sc.streams) > 0 {
		return
	}
	sc.closed = true
	sc.conn.CloseAfterSend()
}

// respond sends a response on the stream. Its body goes out as flow control
// allows; what does not fit yet waits for the client's WINDOW_UPDATE.
func (st *h2ServerStream) respond(req *stdhttp.Request, response Response) error {
	status := response.StatusCode
	if status < 200 || status > 999 {
		// Interim responses go through writeInterim.
		return fmt.Errorf("http: invalid status code %d", status)
	}
	bodyAllowed := status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
	body := response.Body
	if !bodyAllowed || req.Method == stdhttp.MethodHead {
		body = nil
	}
	if err := h2CheckHeader(response.Header); err != nil {
		return err
	}

	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset {
		return errH2StreamClosed
	}
	if st.responded {
		return errors.New("http: response already written")
	}
	st.responded = true

	block := sc.enc.Begin(nil)
	block = sc.enc.AppendField(block, ":status", strconv.Itoa(status), false)
	if bodyAllowed {
		block = sc.enc.AppendField(block, "content-length", strconv.Itoa(len(response.Body)), false)
	}
	block = sc.appendHeaderLocked(block, response.Header)
	out := make([]byte, 0, len(block)+2*h2FrameHeaderLen+len(body))
	out = h2AppendHeaderBlock(out, st.id, block, len(body) == 0, sc.peerMaxFrame)
	if len(body) == 0 {
		st.localDone = true
	} else {
		st.pending = body
		out = sc.flushStreamLocked(out, st)
	}
	if response.Close && !sc.goAway {
		// HTTP/2 cannot close one request's connection under the others, so
		// Close retires it gracefully: nothing new is taken, what is open
		// finishes, and then the connection closes.
		sc.goAway = true
		out = h2AppendGoAway(out, sc.lastStreamID, H2NoError, "")
	}
	sc.sendLocked(out)
	sc.finishStreamLocked(st)
	sc.closeFinishedLocked()
	return nil
}

// h2CheckHeader checks the fields of a response or pushed request before
// any is encoded, since encoding changes the table the client tracks.
func h2CheckHeader(header stdhttp.Header) error {
	for key, values := range header {
		if key == "" || textproto.CanonicalMIMEHeaderKey(key) == "" || !h2ValidHeaderName(strings.ToLower(key)) {
			return errors.New("http: invalid header name " + strconv.Quote(key))
		}
		for _, value := range values {
			if !validHeaderValue(value) {
				return errors.New("http: invalid header value for " + key)
			}
		}
	}
	return nil
}

// appendHeaderLocked encodes header's fields, leaving out what HTTP/2 does
// not carry and the Content-Length this side sets itself.
func (sc *h2ServerConn) appendHeaderLocked(block []byte, header stdhttp.Header) []byte {
	for key, values := range header {
		name := strings.ToLower(key)
		if h2ConnectionHeaders[name] || name == "content-length" {
			continue
		}
		for _, value := range values {
			block = sc.enc.AppendField(block, name, value, name == "set-cookie" || name == "authorization")
		}
	}
	return block
}

// writeInterim sends an informational 1xx response ahead of the final one.
func (st *h2ServerStream) writeInterim(status int, header stdhttp.Header) error {
	if err := h2CheckHeader(header); err != nil {
		return err
	}
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset {
		return errH2StreamClosed
	}
	if st.responded {
		return errors.New("http: interim response after the final one")
	}
	block := sc.enc.Begin(nil)
	block = sc.enc.AppendField(block, ":status", strconv.Itoa(status), false)
	block = sc.appendHeaderLocked(block, header)
	sc.sendLocked(h2AppendHeaderBlock(nil, st.id, block, false, sc.peerMaxFrame))
	return nil
}

// forgetLocked drops a stream that is finished or reset.
func (sc *h2ServerConn) forgetLocked(st *h2ServerStream) {
	st.reset = st.reset || !st.localDone
	st.pending = nil
	if sc.streams[st.id] != st {
		return
	}
	delete(sc.streams, st.id)
	if st.pushed {
		sc.pushedStreams--
	}
}

// finishStreamLocked forgets a stream once both sides are done with it. A
// response sent before the request finished arriving ends the request too,
// with RST_STREAM(NO_ERROR) as RFC 9113 section 8.1 describes.
func (sc *h2ServerConn) finishStreamLocked(st *h2ServerStream) {
	if !st.localDone || st.reset {
		return
	}
	if !st.remoteDone {
		st.reset = true
		sc.sendLocked(h2AppendRSTStream(nil, st.id, H2NoError))
	}
	sc.forgetLocked(st)
	sc.closeFinishedLocked()
}

// flushLocked sends what response bodies the windows now allow.
func (sc *h2ServerConn) flushLocked(out []byte) []byte {
	for _, st := range sc.streams {
		if sc.sendWindow <= 0 {
			break
		}
		if len(st.pending) == 0 {
			continue
		}
		out = sc.flushStreamLocked(out, st)
		sc.finishStreamLocked(st)
	}
	return out
}

func (sc *h2ServerConn) flushStreamLocked(out []byte, st *h2ServerStream) []byte {
	for len(st.pending) > 0 {
		n := int64(len(st.pending))
		n = min(n, sc.sendWindow, st.sendWindow, int64(sc.peerMaxFrame))
		if n <= 0 {
			return out
		}
		chunk := st.pending[:n]
		st.pending = st.pending[n:]
		var flags uint8
		if len(st.pending) == 0 {
			flags = h2FlagEndStream
			st.pending = nil
			st.localDone = true
		}
		out = h2AppendFrameHeader(out, h2FrameData, flags, st.id, len(chunk))
		out = append(out, chunk...)
		sc.sendWindow -= n
		st.sendWindow -= n
	}
	return out
}
