package websocket

import (
	"bytes"
	"errors"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strings"
)

var (
	errHandshakeHeaderTooLarge = errors.New("websocket: handshake header too large")
	errMalformedHandshake      = errors.New("websocket: malformed handshake")
)

type handshakeParser struct {
	buffer         []byte
	headerScan     int
	maxHeaderBytes int
}

func (p *handshakeParser) Feed(data []byte) (*stdhttp.Request, bool, error) {
	p.buffer = append(p.buffer, data...)
	at := bytes.Index(p.buffer[p.headerScan:], []byte("\r\n\r\n"))
	if at < 0 {
		if len(p.buffer) > p.maxHeaderBytes {
			return nil, false, errHandshakeHeaderTooLarge
		}
		p.headerScan = len(p.buffer) - 3
		if p.headerScan < 0 {
			p.headerScan = 0
		}
		return nil, false, nil
	}
	headerEnd := p.headerScan + at + 4
	if headerEnd > p.maxHeaderBytes {
		return nil, false, errHandshakeHeaderTooLarge
	}
	request, err := parseHandshakeRequest(p.buffer[:headerEnd])
	if err != nil {
		return nil, false, err
	}
	p.buffer = p.buffer[headerEnd:]
	p.headerScan = 0
	return request, true, nil
}

func (p *handshakeParser) TakeBuffered() []byte {
	data := p.buffer
	p.buffer = nil
	p.headerScan = 0
	return data
}

func parseHandshakeRequest(data []byte) (*stdhttp.Request, error) {
	lineEnd := bytes.Index(data, []byte("\r\n"))
	if lineEnd <= 0 {
		return nil, errMalformedHandshake
	}
	requestLine := string(data[:lineEnd])
	firstSpace := strings.IndexByte(requestLine, ' ')
	lastSpace := strings.LastIndexByte(requestLine, ' ')
	if firstSpace <= 0 || lastSpace <= firstSpace+1 || lastSpace == len(requestLine)-1 {
		return nil, errMalformedHandshake
	}
	method := requestLine[:firstSpace]
	requestURI := requestLine[firstSpace+1 : lastSpace]
	proto := requestLine[lastSpace+1:]
	if !validToken(method) || proto != "HTTP/1.1" {
		return nil, errMalformedHandshake
	}
	parsedURL, err := url.ParseRequestURI(requestURI)
	if err != nil {
		return nil, errMalformedHandshake
	}

	header := make(stdhttp.Header, 8)
	host := ""
	for offset := lineEnd + 2; offset < len(data)-2; {
		relativeEnd := bytes.Index(data[offset:], []byte("\r\n"))
		if relativeEnd <= 0 {
			return nil, errMalformedHandshake
		}
		line := data[offset : offset+relativeEnd]
		offset += relativeEnd + 2
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 || line[0] == ' ' || line[0] == '\t' {
			return nil, errMalformedHandshake
		}
		nameBytes := line[:colon]
		if !validTokenBytes(nameBytes) {
			return nil, errMalformedHandshake
		}
		valueBytes := trimOWS(line[colon+1:])
		if !validHeaderValue(valueBytes) {
			return nil, errMalformedHandshake
		}
		name := handshakeHeaderName(nameBytes)
		value := string(valueBytes)
		if name == "Host" {
			if host != "" {
				return nil, errMalformedHandshake
			}
			host = value
			continue
		}
		header[name] = append(header[name], value)
	}
	if host == "" || header.Get("Content-Length") != "" || header.Get("Transfer-Encoding") != "" {
		return nil, errMalformedHandshake
	}
	return &stdhttp.Request{
		Method:        method,
		URL:           parsedURL,
		Proto:         proto,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          stdhttp.NoBody,
		Host:          host,
		RequestURI:    requestURI,
		ContentLength: 0,
	}, nil
}

func handshakeHeaderName(name []byte) string {
	for _, known := range [...]string{
		"Host",
		"Upgrade",
		"Connection",
		"Sec-Websocket-Version",
		"Sec-Websocket-Key",
		"Sec-Websocket-Protocol",
		"Origin",
		"Cookie",
		"Authorization",
		"User-Agent",
		"X-Forwarded-For",
		"X-Forwarded-Proto",
	} {
		if len(name) == len(known) && bytes.EqualFold(name, []byte(known)) {
			return known
		}
	}
	return textproto.CanonicalMIMEHeaderKey(string(name))
}

func validHeaderValue(value []byte) bool {
	for _, c := range value {
		if (c < ' ' && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

func trimOWS(value []byte) []byte {
	start, end := 0, len(value)
	for start < end && (value[start] == ' ' || value[start] == '\t') {
		start++
	}
	for end > start && (value[end-1] == ' ' || value[end-1] == '\t') {
		end--
	}
	return value[start:end]
}
