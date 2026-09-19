//go:build linux || darwin || windows

package http3

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/lesismal/fib/go/http3/internal/qpack"
)

// HTTP/3 carries fields as HTTP/2 does (RFC 9114 section 4.2): names
// lowercase, pseudo-headers first, and no connection-specific fields.

// connectionHeaders are HTTP/1 hop-by-hop fields, which HTTP/3 forbids.
var connectionHeaders = map[string]bool{
	"connection":        true,
	"keep-alive":        true,
	"proxy-connection":  true,
	"transfer-encoding": true,
	"upgrade":           true,
}

// validName reports whether name is a valid lowercase field name.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'A' && c <= 'Z' || c <= ' ' || c >= 0x7f || c == ':' && i > 0 {
			return false
		}
	}
	return true
}

// validValue reports whether value may appear in a field, which rules out
// NUL, CR, LF and leading or trailing whitespace.
func validValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c == 0 || c == '\r' || c == '\n' {
			return false
		}
	}
	n := len(value)
	return n == 0 || value[0] != ' ' && value[0] != '\t' && value[n-1] != ' ' && value[n-1] != '\t'
}

// sendableValue is what this side lets through from a handler or caller:
// the whitespace rules are the peer's to enforce.
func sendableValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < ' ' && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
}

func sensitive(name string) bool {
	return name == "authorization" || name == "cookie" || name == "set-cookie" || name == "proxy-authorization"
}

// checkHeader checks the fields of a message about to be sent, before any
// is encoded.
func checkHeader(header stdhttp.Header) error {
	for key, values := range header {
		if key == "" || textproto.CanonicalMIMEHeaderKey(key) == "" || !validName(strings.ToLower(key)) {
			return errors.New("http3: invalid header name " + strconv.Quote(key))
		}
		for _, value := range values {
			if !sendableValue(value) {
				return errors.New("http3: invalid header value for " + key)
			}
		}
	}
	return nil
}

// appendHeader encodes header's fields, leaving out what HTTP/3 does not
// carry and what skip names.
func appendHeader(block []byte, header stdhttp.Header, skip func(string) bool) []byte {
	for key, values := range header {
		name := strings.ToLower(key)
		if connectionHeaders[name] || skip != nil && skip(name) {
			continue
		}
		for _, value := range values {
			block = qpack.AppendField(block, name, value, sensitive(name))
		}
	}
	return block
}

// appendHeadersFrame appends a HEADERS frame holding block.
func appendHeadersFrame(b, block []byte) []byte {
	b = appendFrameHeader(b, frameHeaders, len(block))
	return append(b, block...)
}

// newRequest builds a request from its fields, rejecting what RFC 9114
// section 4.3.1 calls malformed.
func newRequest(fields []qpack.HeaderField) (*stdhttp.Request, error) {
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
		if !validName(f.Name) || !validValue(f.Value) || connectionHeaders[f.Name] {
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
		header["Cookie"] = []string{strings.Join(cookies, "; ")}
	}
	if method == "" {
		return nil, errors.New("missing :method")
	}
	req := &stdhttp.Request{
		Method:     method,
		Proto:      "HTTP/3.0",
		ProtoMajor: 3,
		Header:     header,
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

// trailerFrom builds a trailer section, which may hold no pseudo-headers.
func trailerFrom(fields []qpack.HeaderField) (stdhttp.Header, error) {
	trailer := make(stdhttp.Header, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") || !validName(f.Name) || !validValue(f.Value) || connectionHeaders[f.Name] {
			return nil, fmt.Errorf("invalid trailer %q", f.Name)
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		trailer[key] = append(trailer[key], f.Value)
	}
	return trailer, nil
}

// newResponse builds a response from its fields. It returns nil for an
// interim response.
func newResponse(fields []qpack.HeaderField, req *stdhttp.Request) (*stdhttp.Response, error) {
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
		if !validName(f.Name) || connectionHeaders[f.Name] {
			return nil, fmt.Errorf("invalid header %q", f.Name)
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
			return nil, errors.New("101 over HTTP/3")
		}
		return nil, nil
	}
	resp := &stdhttp.Response{
		Status:        status + " " + stdhttp.StatusText(code),
		StatusCode:    code,
		Proto:         "HTTP/3.0",
		ProtoMajor:    3,
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

// bodyAllowed reports whether a response to req with status has a body.
func bodyAllowed(req *stdhttp.Request, status int) bool {
	return req.Method != stdhttp.MethodHead && status != stdhttp.StatusNoContent &&
		status != stdhttp.StatusNotModified && status >= 200
}

// decodeFields decodes a field section.
func decodeFields(block []byte, maxSize int) ([]qpack.HeaderField, error) {
	var fields []qpack.HeaderField
	err := qpack.Decode(block, maxSize, func(f qpack.HeaderField) error {
		fields = append(fields, f)
		return nil
	})
	return fields, err
}

// validTrailer reports whether a response may send name, in lower case, as
// a trailer: a field HTTP/3 carries, with sendable values, that does not
// frame, route or authenticate the message (RFC 9110 section 6.5.1).
func validTrailer(name string, values []string) bool {
	switch name {
	case "content-length", "transfer-encoding", "trailer", "host", "content-type", "content-encoding",
		"content-range", "cache-control", "expect", "max-forwards", "pragma", "range", "te",
		"authorization", "set-cookie":
		return false
	}
	if name == "" || connectionHeaders[name] || !validName(name) {
		return false
	}
	for _, value := range values {
		if !sendableValue(value) {
			return false
		}
	}
	return true
}
