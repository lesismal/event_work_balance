//go:build linux || darwin || windows

package http

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	stdhttp "net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"

	"github.com/lesismal/fib/go/internal/hpack"
)

// HTTP/2 on the client side: a clientConn whose TLS handshake chose "h2", or
// that was dialed for h2c, hands its traffic to an h2ClientConn, which runs
// each request as a stream of its own.

// h2InitialMaxStreams is how many streams a connection is given before the
// server's SETTINGS says how many it allows.
const h2InitialMaxStreams = 100

type h2ClientConn struct {
	cc     *clientConn
	client *Client

	// The read side is only touched by the worker running OnData.
	in            []byte
	gotSettings   bool
	dec           *hpack.Decoder
	contStream    uint32
	contEndStream bool
	contBlock     []byte
	recvWindow    int64
	recvUnacked   int64

	// mu guards the write side and the streams. Frames are sent while holding
	// it, so header blocks leave in the order the encoder produced them.
	mu           sync.Mutex
	streams      map[uint32]*h2ClientStream
	enc          *hpack.Encoder
	nextID       uint32
	peerMaxFrame int
	peerWindow   int64
	sendWindow   int64
	goAway       bool
	closed       bool
	// failed is the error this side ended the connection with, which the
	// requests still on it then report.
	failed error
}

type h2ClientStream struct {
	id uint32
	r  *clientRequest
	// The reader alone touches the response as it arrives.
	resp        *stdhttp.Response
	body        []byte
	declared    int64
	recvWindow  int64
	recvUnacked int64
	// The rest is guarded by the connection's mu.
	received   bool
	sendWindow int64
	pending    []byte
	localDone  bool
}

// startH2 turns the connection over to HTTP/2 before it carries anything,
// and sends the client's connection preface.
func (cc *clientConn) startH2() {
	config := cc.client.config
	dec := hpack.NewDecoder(hpack.DefaultTableSize)
	dec.MaxStringLength = config.MaxResponseHeaderBytes
	hc := &h2ClientConn{
		cc:           cc,
		client:       cc.client,
		dec:          dec,
		recvWindow:   h2ConnWindow,
		streams:      make(map[uint32]*h2ClientStream),
		enc:          hpack.NewEncoder(),
		nextID:       1,
		peerMaxFrame: h2DefaultMaxFrameSize,
		peerWindow:   h2DefaultWindow,
		sendWindow:   h2DefaultWindow,
	}
	cc.maxStreams = h2InitialMaxStreams
	cc.h2 = hc
	out := append([]byte(nil), h2Preface...)
	out = h2AppendSettings(out,
		[2]uint32{uint32(h2SettingEnablePush), 0},
		[2]uint32{uint32(h2SettingInitialWindowSize), h2StreamWindow},
		[2]uint32{uint32(h2SettingMaxHeaderListSize), uint32(config.MaxResponseHeaderBytes)},
	)
	out = h2AppendWindowUpdate(out, 0, h2ConnWindow-h2DefaultWindow)
	_ = cc.conn.SendOwned(out)
}

func (hc *h2ClientConn) sendLocked(out []byte) {
	if len(out) > 0 && !hc.closed {
		_ = hc.cc.conn.SendOwned(out)
	}
}

// send opens a stream for r, which the client has already counted against
// the connection.
func (hc *h2ClientConn) send(r *clientRequest) {
	if !r.attach(hc.cc) {
		hc.client.streamDone(hc.cc)
		return
	}
	hc.mu.Lock()
	if hc.closed || hc.goAway {
		hc.mu.Unlock()
		r.detach()
		hc.client.streamDone(hc.cc)
		hc.client.enqueue(hc.cc.host.target, r, true)
		return
	}
	fields, err := h2RequestFields(r, hc.cc.host.target.secure)
	if err != nil {
		hc.mu.Unlock()
		r.detach()
		hc.client.streamDone(hc.cc)
		r.finish(nil, err)
		return
	}
	id := hc.nextID
	hc.nextID += 2
	r.streamID = id
	st := &h2ClientStream{id: id, r: r, declared: -1, recvWindow: h2StreamWindow, sendWindow: hc.peerWindow}
	hc.streams[id] = st
	block := hc.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		name := fields[i]
		block = hc.enc.AppendField(block, name, fields[i+1], name == "authorization" || name == "cookie")
	}
	out := h2AppendHeaderBlock(nil, id, block, len(r.body) == 0, hc.peerMaxFrame)
	if len(r.body) == 0 {
		st.localDone = true
	} else {
		st.pending = r.body
		out = hc.flushStreamLocked(out, st)
	}
	hc.sendLocked(out)
	hc.mu.Unlock()
}

// h2RequestFields lists a request's header fields, pseudo-headers first, as
// name and value pairs. Everything is checked before anything is encoded,
// since encoding changes the table the server tracks.
func h2RequestFields(r *clientRequest, secure bool) ([]string, error) {
	req := r.req
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	method := req.Method
	if method == "" {
		method = stdhttp.MethodGet
	}
	fields := []string{":method", method}
	if method != stdhttp.MethodConnect {
		scheme := "http"
		if secure {
			scheme = "https"
		}
		fields = append(fields, ":scheme", scheme, ":authority", host, ":path", req.URL.RequestURI())
	} else {
		fields = append(fields, ":authority", host)
	}
	userAgent := false
	for key, values := range req.Header {
		name := strings.ToLower(key)
		if h2ConnectionHeaders[name] || name == "host" || name == "content-length" || name == "te" {
			continue
		}
		if !h2ValidHeaderName(name) {
			return nil, errors.New("http: invalid request header name " + strconv.Quote(key))
		}
		userAgent = userAgent || name == "user-agent"
		for _, value := range values {
			if !validHeaderValue(value) {
				return nil, errors.New("http: invalid request header value for " + key)
			}
			fields = append(fields, name, value)
		}
	}
	if !userAgent {
		fields = append(fields, "user-agent", "Go-http-client/2.0")
	}
	switch {
	case len(r.body) > 0:
		fields = append(fields, "content-length", strconv.Itoa(len(r.body)))
	case method == stdhttp.MethodPost || method == stdhttp.MethodPut || method == stdhttp.MethodPatch:
		fields = append(fields, "content-length", "0")
	}
	return fields, nil
}

// cancel abandons r's stream, which the request's timeout or context has
// already failed.
func (hc *h2ClientConn) cancel(r *clientRequest) {
	hc.mu.Lock()
	st := hc.streams[r.streamID]
	if st == nil || st.r != r {
		hc.mu.Unlock()
		return
	}
	delete(hc.streams, st.id)
	hc.sendLocked(h2AppendRSTStream(nil, st.id, H2Cancel))
	hc.closeIfDoneLocked()
	hc.mu.Unlock()
	hc.client.streamDone(hc.cc)
}

// shutdown fails or retries whatever the connection still carried when it
// closed.
func (hc *h2ClientConn) shutdown(err error) {
	hc.mu.Lock()
	hc.closed = true
	if hc.failed != nil {
		err = hc.failed
	}
	streams := make([]*h2ClientStream, 0, len(hc.streams))
	for _, st := range hc.streams {
		streams = append(streams, st)
	}
	clear(hc.streams)
	hc.mu.Unlock()
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	for _, st := range streams {
		r := st.r
		r.detach()
		if !st.received && r.retryable() {
			// Nothing came back, so a request that is safe to repeat goes out
			// again on another connection.
			r.retried = true
			hc.client.enqueue(hc.cc.host.target, r, true)
			continue
		}
		// OnClose runs on the event loop, which the callback must not hold up.
		go r.finish(nil, err)
	}
}

// fail ends the connection with GOAWAY; OnClose then settles its streams.
func (hc *h2ClientConn) fail(err *H2ConnError) {
	hc.mu.Lock()
	if !hc.closed {
		hc.sendLocked(h2AppendGoAway(nil, 0, err.Code, err.Reason))
	}
	hc.goAway = true
	if hc.failed == nil {
		hc.failed = err
	}
	hc.mu.Unlock()
	hc.client.retire(hc.cc)
	hc.cc.conn.CloseAfterSend()
}

// closeIfDoneLocked closes a connection the server is retiring once its last
// stream has finished.
func (hc *h2ClientConn) closeIfDoneLocked() {
	if hc.goAway && !hc.closed && len(hc.streams) == 0 {
		hc.cc.conn.CloseAfterSend()
	}
}

func (hc *h2ClientConn) feed(data []byte) {
	hc.in = append(hc.in, data...)
	offset := 0
	for {
		f, n, err := h2ReadFrame(hc.in[offset:], h2DefaultMaxFrameSize)
		if err == nil && n > 0 {
			offset += n
			err = hc.handleFrame(&f)
		}
		if err != nil {
			var streamErr *H2StreamError
			if errors.As(err, &streamErr) {
				hc.resetStream(streamErr.StreamID, streamErr.Code, streamErr)
				continue
			}
			var connErr *H2ConnError
			if !errors.As(err, &connErr) {
				connErr = h2ConnErr(H2InternalError, "%v", err)
			}
			hc.fail(connErr)
			hc.in = nil
			return
		}
		if n == 0 {
			break
		}
	}
	if offset == len(hc.in) {
		if cap(hc.in) > maxRetainedBuffer {
			hc.in = nil
		} else {
			hc.in = hc.in[:0]
		}
	} else if offset > 0 {
		hc.in = append(hc.in[:0], hc.in[offset:]...)
	}
}

func (hc *h2ClientConn) handleFrame(f *h2Frame) error {
	if hc.contStream != 0 && (f.typ != h2FrameContinuation || f.streamID != hc.contStream) {
		return h2ConnErr(H2ProtocolError, "expected CONTINUATION for stream %d", hc.contStream)
	}
	if !hc.gotSettings {
		if f.typ != h2FrameSettings || f.has(h2FlagAck) {
			return h2ConnErr(H2ProtocolError, "server preface must be SETTINGS")
		}
		hc.gotSettings = true
	}
	switch f.typ {
	case h2FrameData:
		return hc.handleData(f)
	case h2FrameHeaders:
		if f.streamID == 0 {
			return h2ConnErr(H2ProtocolError, "HEADERS on stream 0")
		}
		block := f.payload
		if f.has(h2FlagPriority) {
			if len(block) < 5 {
				return h2ConnErr(H2FrameSizeError, "HEADERS priority truncated")
			}
			block = block[5:]
		}
		if !f.has(h2FlagEndHeaders) {
			if len(block) > hc.client.config.MaxResponseHeaderBytes {
				return h2ConnErr(H2EnhanceYourCalm, "header block too large")
			}
			hc.contStream, hc.contEndStream = f.streamID, f.has(h2FlagEndStream)
			hc.contBlock = append(hc.contBlock[:0], block...)
			return nil
		}
		return hc.handleHeaderBlock(f.streamID, block, f.has(h2FlagEndStream))
	case h2FrameContinuation:
		if hc.contStream == 0 {
			return h2ConnErr(H2ProtocolError, "unexpected CONTINUATION")
		}
		hc.contBlock = append(hc.contBlock, f.payload...)
		if len(hc.contBlock) > hc.client.config.MaxResponseHeaderBytes {
			return h2ConnErr(H2EnhanceYourCalm, "header block too large")
		}
		if !f.has(h2FlagEndHeaders) {
			return nil
		}
		id := hc.contStream
		hc.contStream = 0
		err := hc.handleHeaderBlock(id, hc.contBlock, hc.contEndStream)
		if cap(hc.contBlock) > maxRetainedBuffer {
			hc.contBlock = nil
		}
		return err
	case h2FramePriority:
		return nil
	case h2FrameRSTStream:
		if f.streamID == 0 || len(f.payload) != 4 {
			return h2ConnErr(H2ProtocolError, "malformed RST_STREAM")
		}
		code := H2ErrorCode(binary.BigEndian.Uint32(f.payload))
		hc.endStream(f.streamID, code == H2RefusedStream, &H2StreamError{StreamID: f.streamID, Code: code})
		return nil
	case h2FrameSettings:
		return hc.handleSettings(f)
	case h2FramePushPromise:
		// SETTINGS_ENABLE_PUSH is 0, so a server may not push.
		return h2ConnErr(H2ProtocolError, "server sent PUSH_PROMISE")
	case h2FramePing:
		if f.streamID != 0 || len(f.payload) != 8 {
			return h2ConnErr(H2ProtocolError, "malformed PING")
		}
		if !f.has(h2FlagAck) {
			out := h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8)
			hc.mu.Lock()
			hc.sendLocked(append(out, f.payload...))
			hc.mu.Unlock()
		}
		return nil
	case h2FrameGoAway:
		if f.streamID != 0 || len(f.payload) < 8 {
			return h2ConnErr(H2ProtocolError, "malformed GOAWAY")
		}
		hc.handleGoAway(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
		return nil
	case h2FrameWindowUpdate:
		return hc.handleWindowUpdate(f)
	}
	return nil
}

func (hc *h2ClientConn) handleSettings(f *h2Frame) error {
	if f.streamID != 0 {
		return h2ConnErr(H2ProtocolError, "SETTINGS on stream %d", f.streamID)
	}
	if f.has(h2FlagAck) {
		if len(f.payload) != 0 {
			return h2ConnErr(H2FrameSizeError, "SETTINGS ack with payload")
		}
		return nil
	}
	maxStreams := -1
	hc.mu.Lock()
	err := h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
		switch id {
		case h2SettingHeaderTableSize:
			hc.enc.SetMaxTableSize(int(min(value, hpack.DefaultTableSize)))
		case h2SettingEnablePush:
			if value > 1 {
				return h2ConnErr(H2ProtocolError, "ENABLE_PUSH %d", value)
			}
		case h2SettingMaxConcurrentStreams:
			maxStreams = int(min(value, 1<<20))
		case h2SettingInitialWindowSize:
			if value > h2MaxWindow {
				return h2ConnErr(H2FlowControlError, "INITIAL_WINDOW_SIZE %d", value)
			}
			delta := int64(value) - hc.peerWindow
			hc.peerWindow = int64(value)
			for _, st := range hc.streams {
				st.sendWindow += delta
				if st.sendWindow > h2MaxWindow {
					return h2ConnErr(H2FlowControlError, "stream window overflow")
				}
			}
		case h2SettingMaxFrameSize:
			if value < h2DefaultMaxFrameSize || value > h2MaxFrameSizeLimit {
				return h2ConnErr(H2ProtocolError, "MAX_FRAME_SIZE %d", value)
			}
			hc.peerMaxFrame = int(value)
		}
		return nil
	})
	if err == nil {
		out := h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0)
		hc.sendLocked(hc.flushLocked(out))
	}
	hc.mu.Unlock()
	if err != nil {
		return err
	}
	if maxStreams >= 0 {
		c, h := hc.client, hc.cc.host
		c.mu.Lock()
		hc.cc.maxStreams = maxStreams
		work, dials := c.dispatchLocked(h)
		c.mu.Unlock()
		c.carryOut(h, work, dials)
	}
	return nil
}

func (hc *h2ClientConn) handleWindowUpdate(f *h2Frame) error {
	if len(f.payload) != 4 {
		return h2ConnErr(H2FrameSizeError, "WINDOW_UPDATE length %d", len(f.payload))
	}
	increment := int64(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if f.streamID == 0 {
		if increment == 0 {
			return h2ConnErr(H2ProtocolError, "WINDOW_UPDATE of 0")
		}
		hc.sendWindow += increment
		if hc.sendWindow > h2MaxWindow {
			return h2ConnErr(H2FlowControlError, "connection window overflow")
		}
	} else if st := hc.streams[f.streamID]; st != nil {
		if increment == 0 {
			return &H2StreamError{StreamID: f.streamID, Code: H2ProtocolError}
		}
		st.sendWindow += increment
		if st.sendWindow > h2MaxWindow {
			return &H2StreamError{StreamID: f.streamID, Code: H2FlowControlError}
		}
	}
	hc.sendLocked(hc.flushLocked(nil))
	return nil
}

// handleGoAway retires the connection. Streams past lastID were never
// processed, so they go out again on another connection.
func (hc *h2ClientConn) handleGoAway(lastID uint32) {
	hc.mu.Lock()
	hc.goAway = true
	var unprocessed []*h2ClientStream
	for id, st := range hc.streams {
		if id > lastID {
			unprocessed = append(unprocessed, st)
			delete(hc.streams, id)
		}
	}
	hc.closeIfDoneLocked()
	hc.mu.Unlock()
	hc.client.retire(hc.cc)
	for _, st := range unprocessed {
		st.r.detach()
		hc.client.streamDone(hc.cc)
		hc.client.enqueue(hc.cc.host.target, st.r, true)
	}
}

func (hc *h2ClientConn) handleData(f *h2Frame) error {
	if f.streamID == 0 {
		return h2ConnErr(H2ProtocolError, "DATA on stream 0")
	}
	length := int64(f.length)
	if length > hc.recvWindow {
		return h2ConnErr(H2FlowControlError, "DATA exceeds connection window")
	}
	hc.recvWindow -= length
	hc.recvUnacked += length
	var out []byte
	if hc.recvUnacked >= h2ConnWindow/2 {
		out = h2AppendWindowUpdate(out, 0, uint32(hc.recvUnacked))
		hc.recvWindow += hc.recvUnacked
		hc.recvUnacked = 0
	}
	hc.mu.Lock()
	st := hc.streams[f.streamID]
	if st != nil {
		st.received = true
	}
	hc.sendLocked(out)
	hc.mu.Unlock()
	if st == nil {
		// A stream abandoned in the meantime.
		return nil
	}
	if st.resp == nil {
		return &H2StreamError{StreamID: st.id, Code: H2ProtocolError}
	}
	if length > st.recvWindow {
		return &H2StreamError{StreamID: st.id, Code: H2FlowControlError}
	}
	st.recvWindow -= length
	st.recvUnacked += length
	if int64(len(st.body))+int64(len(f.payload)) > hc.client.config.MaxResponseBodyBytes {
		hc.resetStream(st.id, H2Cancel, ErrResponseBodyTooLarge)
		return nil
	}
	st.body = append(st.body, f.payload...)
	if f.has(h2FlagEndStream) {
		hc.complete(st)
		return nil
	}
	if st.recvUnacked >= h2StreamWindow/2 {
		increment := st.recvUnacked
		st.recvWindow += increment
		st.recvUnacked = 0
		hc.mu.Lock()
		hc.sendLocked(h2AppendWindowUpdate(nil, st.id, uint32(increment)))
		hc.mu.Unlock()
	}
	return nil
}

func (hc *h2ClientConn) handleHeaderBlock(id uint32, block []byte, endStream bool) error {
	var fields []hpack.HeaderField
	listSize := 0
	err := hc.dec.Decode(block, func(f hpack.HeaderField) error {
		listSize += len(f.Name) + len(f.Value) + 32
		fields = append(fields, f)
		return nil
	})
	if err != nil {
		return h2ConnErr(H2CompressionError, "%v", err)
	}
	hc.mu.Lock()
	st := hc.streams[id]
	if st != nil {
		st.received = true
	}
	hc.mu.Unlock()
	if st == nil {
		return nil
	}
	if listSize > hc.client.config.MaxResponseHeaderBytes {
		hc.resetStream(id, H2Cancel, ErrResponseHeaderTooLarge)
		return nil
	}
	if st.resp != nil {
		// Trailers.
		if !endStream {
			return &H2StreamError{StreamID: id, Code: H2ProtocolError}
		}
		trailer := make(stdhttp.Header)
		for _, f := range fields {
			if strings.HasPrefix(f.Name, ":") {
				return &H2StreamError{StreamID: id, Code: H2ProtocolError}
			}
			key := textproto.CanonicalMIMEHeaderKey(f.Name)
			trailer[key] = append(trailer[key], f.Value)
		}
		st.resp.Trailer = trailer
		hc.complete(st)
		return nil
	}
	resp, err := h2NewResponse(fields, st.r.req)
	if err != nil {
		return &H2StreamError{StreamID: id, Code: H2ProtocolError}
	}
	if resp == nil {
		// An interim 1xx response; the real one follows.
		if endStream {
			return &H2StreamError{StreamID: id, Code: H2ProtocolError}
		}
		return nil
	}
	if resp.ContentLength > hc.client.config.MaxResponseBodyBytes && bodyAllowed(st.r.req, resp.StatusCode) {
		hc.resetStream(id, H2Cancel, ErrResponseBodyTooLarge)
		return nil
	}
	st.resp = resp
	st.declared = resp.ContentLength
	if endStream {
		hc.complete(st)
	}
	return nil
}

// h2NewResponse builds a response from its header fields. It returns nil for
// an interim response.
func h2NewResponse(fields []hpack.HeaderField, req *stdhttp.Request) (*stdhttp.Response, error) {
	status := ""
	header := make(stdhttp.Header, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			if f.Name != ":status" || status != "" || len(header) > 0 {
				return nil, errors.New("malformed pseudo-header")
			}
			status = f.Value
			continue
		}
		if h2ConnectionHeaders[f.Name] {
			return nil, errors.New("connection-specific header")
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		header[key] = append(header[key], f.Value)
	}
	code, err := strconv.Atoi(status)
	if err != nil || len(status) != 3 || code < 100 {
		return nil, errors.New("malformed :status")
	}
	if code < 200 {
		if code == stdhttp.StatusSwitchingProtocols {
			return nil, errors.New("101 over HTTP/2")
		}
		return nil, nil
	}
	resp := &stdhttp.Response{
		Status:        status + " " + stdhttp.StatusText(code),
		StatusCode:    code,
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		Header:        header,
		ContentLength: -1,
		Request:       req,
		Body:          stdhttp.NoBody,
	}
	if cl := header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return nil, errors.New("malformed content-length")
		}
		resp.ContentLength = n
	}
	return resp, nil
}

// complete hands a finished response to its request.
func (hc *h2ClientConn) complete(st *h2ClientStream) {
	resp := st.resp
	if bodyAllowed(st.r.req, resp.StatusCode) && st.declared >= 0 && st.declared != int64(len(st.body)) {
		hc.resetStream(st.id, H2ProtocolError, errors.New("http2: response body length does not match Content-Length"))
		return
	}
	hc.mu.Lock()
	if hc.streams[st.id] != st {
		hc.mu.Unlock()
		return
	}
	delete(hc.streams, st.id)
	if !st.localDone {
		// The server answered before taking the whole body; stop sending it.
		st.pending = nil
		hc.sendLocked(h2AppendRSTStream(nil, st.id, H2Cancel))
	}
	hc.closeIfDoneLocked()
	hc.mu.Unlock()
	if len(st.body) > 0 {
		resp.Body = io.NopCloser(bytes.NewReader(st.body))
	}
	if resp.ContentLength < 0 && st.r.req.Method != stdhttp.MethodHead {
		resp.ContentLength = int64(len(st.body))
	}
	st.body = nil
	st.r.detach()
	hc.client.streamDone(hc.cc)
	st.r.finish(resp, nil)
}

// resetStream ends a stream this side found at fault with RST_STREAM, and
// fails its request with err.
func (hc *h2ClientConn) resetStream(id uint32, code H2ErrorCode, err error) {
	hc.mu.Lock()
	hc.sendLocked(h2AppendRSTStream(nil, id, code))
	hc.mu.Unlock()
	hc.endStream(id, false, err)
}

// endStream forgets a stream and fails its request, or retries it when the
// server refused it before doing anything.
func (hc *h2ClientConn) endStream(id uint32, retry bool, err error) {
	hc.mu.Lock()
	st := hc.streams[id]
	if st != nil {
		delete(hc.streams, id)
		st.pending = nil
	}
	hc.closeIfDoneLocked()
	hc.mu.Unlock()
	if st == nil {
		return
	}
	st.r.detach()
	hc.client.streamDone(hc.cc)
	if retry {
		hc.client.enqueue(hc.cc.host.target, st.r, true)
		return
	}
	st.r.finish(nil, err)
}

// flushLocked sends what request bodies the windows now allow.
func (hc *h2ClientConn) flushLocked(out []byte) []byte {
	for _, st := range hc.streams {
		if hc.sendWindow <= 0 {
			break
		}
		if len(st.pending) > 0 {
			out = hc.flushStreamLocked(out, st)
		}
	}
	return out
}

func (hc *h2ClientConn) flushStreamLocked(out []byte, st *h2ClientStream) []byte {
	for len(st.pending) > 0 {
		n := min(int64(len(st.pending)), hc.sendWindow, st.sendWindow, int64(hc.peerMaxFrame))
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
		hc.sendWindow -= n
		st.sendWindow -= n
	}
	return out
}
