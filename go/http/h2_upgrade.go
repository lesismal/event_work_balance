//go:build linux || darwin || windows

package http

import (
	"encoding/base64"
	stdhttp "net/http"
	"strings"

	fib "github.com/lesismal/fib/go"
)

// The HTTP/1.1 Upgrade to h2c (RFC 7540 section 3.2): a cleartext client that
// does not know whether the server speaks HTTP/2 sends an ordinary request
// asking to switch, and the server answers that request over HTTP/2, on
// stream 1, after 101 Switching Protocols. RFC 9113 deprecates it, but it is
// what curl --http2 and other clients still do for http:// URLs.

const h2cSwitchingProtocols = "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n"

// h2cUpgrade reports whether request asks to switch to h2c and may, and
// returns the settings its HTTP2-Settings header carries. Over TLS the
// switch is made through ALPN instead, and never this way.
func (h *ServerHandler) h2cUpgrade(c *fib.Connection, request *stdhttp.Request) ([]byte, bool) {
	if h.config.DisableHTTP2 || c.Layer() != nil || !request.ProtoAtLeast(1, 1) {
		return nil, false
	}
	values := request.Header["Http2-Settings"]
	if len(values) != 1 || !headerHasToken(request.Header, "Upgrade", "h2c") ||
		!headerHasToken(request.Header, "Connection", "upgrade") ||
		!headerHasToken(request.Header, "Connection", "http2-settings") {
		return nil, false
	}
	settings, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(values[0], "="))
	if err != nil || len(settings)%6 != 0 {
		return nil, false
	}
	return settings, true
}

// upgradeH2C switches the connection to HTTP/2 and answers request on
// stream 1. Whatever the client sent after the request, its connection
// preface onwards, is HTTP/2.
func (h *ServerHandler) upgradeH2C(c *fib.Connection, parser *Parser, request *stdhttp.Request, settings []byte) {
	sc := newH2ServerConn(h, c, parser.remoteAddr)
	sc.mu.Lock()
	err := sc.applySettingsLocked(settings)
	sc.mu.Unlock()
	if err != nil {
		// Settings the server cannot accept: carry on in HTTP/1.1, which
		// the client has to be ready for anyway.
		serveRequest(h.handler, &Context{Conn: c, Request: request})
		return
	}
	rest := parser.TakeBuffered()
	c.SetAttachment(sc)
	// The 101 is the implicit acknowledgement of the client's settings.
	_ = c.Send([]byte(h2cSwitchingProtocols))
	sc.start()

	// The request becomes stream 1, already half-closed by the client.
	for _, token := range request.Header["Connection"] {
		for _, name := range strings.Split(token, ",") {
			request.Header.Del(strings.TrimSpace(name))
		}
	}
	request.Header.Del("Connection")
	request.Header.Del("Upgrade")
	request.Header.Del("Http2-Settings")
	request.Proto, request.ProtoMajor, request.ProtoMinor = "HTTP/2.0", 2, 0
	st := &h2ServerStream{sc: sc, id: 1, req: request, recvWindow: h2StreamWindow, declared: -1, remoteDone: true}
	sc.maxClientID = 1
	sc.mu.Lock()
	st.sendWindow = sc.peerWindow
	sc.streams[1] = st
	sc.lastStreamID = 1
	sc.mu.Unlock()
	serveRequest(h.handler, &Context{Conn: c, Request: request, stream: st})
	if len(rest) > 0 {
		sc.feed(rest)
	}
}

// headerHasToken reports whether a comma-separated header lists token,
// ignoring case.
func headerHasToken(header stdhttp.Header, key, token string) bool {
	for _, value := range header[key] {
		for _, item := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(item), token) {
				return true
			}
		}
	}
	return false
}
