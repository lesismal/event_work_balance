//go:build linux || darwin || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strings"
)

// Server push (RFC 9113 section 8.4): the server promises a request the client
// has not made yet, on a stream of its own, and answers it there, so that the
// response is on its way before the client would have asked for it.

var (
	// ErrPushLimit is what Push returns when the client already has as many
	// pushed streams open as its SETTINGS_MAX_CONCURRENT_STREAMS allows.
	ErrPushLimit = errors.New("http2: push would exceed the client's concurrent stream limit")
	// errPushAfterResponse is what Push returns once the response it would
	// accompany has been sent in full.
	errPushAfterResponse = errors.New("http2: push after the response was sent")
)

// h2PushForbiddenHeaders are fields a promised request may not carry: it has
// no body, and the rest are the connection's own business.
var h2PushForbiddenHeaders = map[string]bool{
	"content-length":    true,
	"content-encoding":  true,
	"trailer":           true,
	"te":                true,
	"expect":            true,
	"host":              true,
	"connection":        true,
	"keep-alive":        true,
	"proxy-connection":  true,
	"transfer-encoding": true,
	"upgrade":           true,
}

// push promises target on st and serves the promised request through the
// handler before returning.
func (st *h2ServerStream) push(parent *stdhttp.Request, target string, opts *stdhttp.PushOptions) error {
	if st.pushed {
		// Promises ride on streams the client opened, never on pushed ones.
		return stdhttp.ErrNotSupported
	}
	sc := st.sc
	method, header := stdhttp.MethodGet, stdhttp.Header(nil)
	if opts != nil {
		if opts.Method != "" {
			method = opts.Method
		}
		header = opts.Header
	}
	if method != stdhttp.MethodGet && method != stdhttp.MethodHead {
		return errors.New("http2: push method must be GET or HEAD, not " + method)
	}
	scheme := "http"
	if sc.tlsState != nil {
		scheme = "https"
	}
	authority, path := parent.Host, target
	if !strings.HasPrefix(target, "/") {
		u, err := url.Parse(target)
		if err != nil || u.Scheme != scheme || u.Host == "" {
			return errors.New("http2: push target must be an absolute path or a " + scheme + " URL: " + target)
		}
		authority, path = u.Host, u.RequestURI()
	}
	u, err := url.ParseRequestURI(path)
	if err != nil {
		return err
	}
	for key := range header {
		if h2PushForbiddenHeaders[strings.ToLower(key)] {
			return errors.New("http2: promised request may not carry " + key)
		}
	}
	if err = h2CheckHeader(header); err != nil {
		return err
	}
	req := &stdhttp.Request{
		Method:     method,
		URL:        u,
		Proto:      "HTTP/2.0",
		ProtoMajor: 2,
		Header:     make(stdhttp.Header, len(header)),
		Host:       authority,
		RemoteAddr: sc.remoteAddr,
		RequestURI: path,
		TLS:        sc.tlsState,
		Body:       stdhttp.NoBody,
	}
	for key, values := range header {
		key = textproto.CanonicalMIMEHeaderKey(key)
		req.Header[key] = append(req.Header[key], values...)
	}

	sc.mu.Lock()
	switch {
	case !sc.pushEnabled:
		sc.mu.Unlock()
		return stdhttp.ErrNotSupported
	case sc.closed || sc.goAway || sc.peerGoingAway || st.reset:
		sc.mu.Unlock()
		return errH2StreamClosed
	case st.localDone:
		sc.mu.Unlock()
		return errPushAfterResponse
	case sc.pushedStreams >= sc.peerMaxPush || sc.nextPushID > h2MaxStreamID:
		sc.mu.Unlock()
		return ErrPushLimit
	}
	id := sc.nextPushID
	sc.nextPushID += 2
	block := sc.enc.Begin(nil)
	block = sc.enc.AppendField(block, ":method", method, false)
	block = sc.enc.AppendField(block, ":scheme", scheme, false)
	block = sc.enc.AppendField(block, ":authority", authority, false)
	block = sc.enc.AppendField(block, ":path", path, false)
	block = sc.appendHeaderLocked(block, header)
	promised := &h2ServerStream{sc: sc, id: id, req: req, pushed: true, remoteDone: true, sendWindow: sc.peerWindow}
	sc.streams[id] = promised
	sc.pushedStreams++
	sc.sendLocked(h2AppendPushPromise(nil, st.id, id, block, sc.peerMaxFrame))
	sc.mu.Unlock()

	sc.handler.handler.ServeHTTP(&Context{Conn: sc.conn, Request: req, stream: promised}, req)
	return nil
}
