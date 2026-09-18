package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
)

var (
	ErrResponseHeaderTooLarge = errors.New("http: response header too large")
	ErrResponseBodyTooLarge   = errors.New("http: response body too large")
	ErrMalformedResponse      = errors.New("http: malformed response")
)

// bodyFraming is how the end of a response body is found.
type bodyFraming uint8

const (
	// framingNone: the response has no body, whatever its headers say.
	framingNone bodyFraming = iota
	framingLength
	framingChunked
	// framingUntilClose: the body is everything until the server closes.
	framingUntilClose
)

// responseParser frames HTTP/1.x responses on a client connection. The client
// sends one request at a time on a connection, so there is at most one
// response in progress and the request it answers is always known.
type responseParser struct {
	maxHeader  int
	maxBody    int64
	buffer     []byte
	headerScan int
	// head is the parsed header of the response in progress, headerEnd where
	// its body starts, and framing how that body ends. head is nil until the
	// header is complete.
	head      *stdhttp.Response
	headerEnd int
	framing   bodyFraming
}

func (p *responseParser) reset() {
	if cap(p.buffer) > maxRetainedBuffer {
		p.buffer = nil
	} else {
		p.buffer = p.buffer[:0]
	}
	p.headerScan = 0
	p.head = nil
	p.headerEnd = 0
}

// buffered reports whether bytes are held beyond the last complete response.
func (p *responseParser) buffered() bool { return len(p.buffer) > 0 }

// feed adds bytes read from the connection and returns the response to req
// once it is complete. Interim 1xx responses are skipped: they are not the
// answer, and the final response follows them on the same connection.
func (p *responseParser) feed(data []byte, req *stdhttp.Request) (*stdhttp.Response, error) {
	p.buffer = append(p.buffer, data...)
	for {
		if p.head == nil {
			done, err := p.parseHead(req)
			if err != nil || !done {
				return nil, err
			}
			if p.head == nil {
				// An interim response was dropped; look for the next header.
				continue
			}
		}
		switch p.framing {
		case framingNone:
			return p.complete(p.headerEnd, nil), nil
		case framingLength:
			end := int64(p.headerEnd) + p.head.ContentLength
			if int64(len(p.buffer)) < end {
				return nil, nil
			}
			return p.complete(int(end), p.buffer[p.headerEnd:end]), nil
		case framingChunked:
			end, complete, err := chunkedEnd(p.buffer, p.headerEnd, p.maxHeader, p.maxBody)
			if err != nil {
				return nil, clientParseError(err)
			}
			if !complete {
				return nil, nil
			}
			return p.completeChunked(end, req)
		default:
			if int64(len(p.buffer)-p.headerEnd) > p.maxBody {
				return nil, ErrResponseBodyTooLarge
			}
			return nil, nil
		}
	}
}

// finish is called when the server closes the connection. A body delimited by
// the close is complete at that point; anything else is cut short, and finish
// returns nil.
func (p *responseParser) finish() *stdhttp.Response {
	if p.head == nil || p.framing != framingUntilClose {
		return nil
	}
	return p.complete(len(p.buffer), p.buffer[p.headerEnd:])
}

// parseHead looks for a complete header and, when it finds one, works out how
// the body that follows it is framed. It reports false while the header is
// still arriving.
func (p *responseParser) parseHead(req *stdhttp.Request) (bool, error) {
	headerAt := bytes.Index(p.buffer[p.headerScan:], []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(p.buffer) > p.maxHeader {
			return false, ErrResponseHeaderTooLarge
		}
		p.headerScan = max(len(p.buffer)-3, 0)
		return false, nil
	}
	headerEnd := p.headerScan + headerAt + 4
	if headerEnd > p.maxHeader {
		return false, ErrResponseHeaderTooLarge
	}
	head, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(p.buffer[:headerEnd])), req)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	_ = head.Body.Close()
	if head.StatusCode >= 100 && head.StatusCode < 200 && head.StatusCode != stdhttp.StatusSwitchingProtocols {
		p.consume(headerEnd)
		return true, nil
	}
	p.head, p.headerEnd = head, headerEnd
	switch {
	case !bodyAllowed(req, head.StatusCode):
		p.framing = framingNone
	case len(head.TransferEncoding) != 0:
		if len(head.TransferEncoding) != 1 || !strings.EqualFold(head.TransferEncoding[0], "chunked") {
			return false, fmt.Errorf("%w: unsupported transfer encoding %q", ErrMalformedResponse, head.TransferEncoding)
		}
		p.framing = framingChunked
	case head.ContentLength >= 0:
		if head.ContentLength > p.maxBody {
			return false, ErrResponseBodyTooLarge
		}
		p.framing = framingLength
	default:
		p.framing = framingUntilClose
	}
	return true, nil
}

// bodyAllowed reports whether a response may carry a body at all. The answer
// to HEAD never does, and neither do 1xx, 204 and 304 responses, whatever
// Content-Length they declare.
func bodyAllowed(req *stdhttp.Request, status int) bool {
	if req.Method == stdhttp.MethodHead {
		return false
	}
	return status >= 200 && status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
}

// completeChunked decodes a chunked body, through net/http's own decoder so
// that trailers land where callers expect them.
func (p *responseParser) completeChunked(end int, req *stdhttp.Request) (*stdhttp.Response, error) {
	full, err := stdhttp.ReadResponse(bufio.NewReader(bytes.NewReader(p.buffer[:end])), req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	body, err := io.ReadAll(io.LimitReader(full.Body, p.maxBody+1))
	_ = full.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	if int64(len(body)) > p.maxBody {
		return nil, ErrResponseBodyTooLarge
	}
	p.head = full
	return p.complete(end, body), nil
}

// complete hands out the response in progress with its body and removes it
// from the buffer. The body is copied: the buffer is reused for the next
// response on the connection.
func (p *responseParser) complete(end int, body []byte) *stdhttp.Response {
	resp := p.head
	if len(body) == 0 {
		resp.Body = stdhttp.NoBody
	} else {
		resp.Body = io.NopCloser(bytes.NewReader(append([]byte(nil), body...)))
	}
	p.head = nil
	p.consume(end)
	return resp
}

func (p *responseParser) consume(n int) {
	if n == len(p.buffer) {
		if cap(p.buffer) > maxRetainedBuffer {
			p.buffer = nil
		} else {
			p.buffer = p.buffer[:0]
		}
	} else {
		p.buffer = append(p.buffer[:0], p.buffer[n:]...)
	}
	p.headerScan = 0
}

// clientParseError restates the shared chunk scanner's errors, which speak of
// requests, in terms of the response being read.
func clientParseError(err error) error {
	switch {
	case errors.Is(err, ErrHeaderTooLarge):
		return ErrResponseHeaderTooLarge
	case errors.Is(err, ErrBodyTooLarge):
		return ErrResponseBodyTooLarge
	case errors.Is(err, ErrMalformed):
		return fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	return err
}
