// Package http provides incremental HTTP/1.x request parsing and response
// handling for the parent epoll package.
package http

import (
	"bufio"
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrHeaderTooLarge = errors.New("http: request header too large")
	ErrBodyTooLarge   = errors.New("http: request body too large")
	ErrMalformed      = errors.New("http: malformed request")
	// ErrUnsupportedTransferEncoding is a request whose body is framed by a
	// transfer coding other than chunked, which the server answers with 501
	// (RFC 9112 section 6.1). errors.Is matches it against ErrMalformed too.
	ErrUnsupportedTransferEncoding = fmt.Errorf("%w: unsupported transfer encoding", ErrMalformed)
)

var requestReaderPool = sync.Pool{New: func() any {
	return bufio.NewReaderSize(bytes.NewReader(nil), 1024)
}}

const maxRetainedBuffer = 64 << 10

type Config struct {
	// MaxHeaderBytes bounds a request's header: its bytes in HTTP/1, and in
	// HTTP/2 both its encoded block and its decoded size as RFC 9113
	// counts it.
	MaxHeaderBytes int
	MaxBodyBytes   int64
	// DisableHTTP2 serves HTTP/1 only. Otherwise a connection that opens with
	// the HTTP/2 preface, over TLS after ALPN chose "h2" or in cleartext with
	// prior knowledge, is served as HTTP/2.
	DisableHTTP2 bool
	// MaxConcurrentStreams is how many requests an HTTP/2 client may have
	// open on one connection. Zero means DefaultMaxConcurrentStreams.
	MaxConcurrentStreams uint32
}

func DefaultConfig() Config {
	return Config{MaxHeaderBytes: 1 << 20, MaxBodyBytes: 16 << 20}
}

// Parser incrementally turns arbitrary TCP chunks into complete HTTP requests.
type Parser struct {
	config     Config
	buffer     []byte
	headerScan int
	// remoteAddr is the peer's address, which the server handler fills in
	// once per connection for every request's RemoteAddr.
	remoteAddr string
	// sniffed records that the server handler has seen enough of the
	// connection to know it is not HTTP/2.
	sniffed bool
	// tls is the connection's TLS state for every request's TLS, looked up
	// once, which tlsChecked records.
	tls        *stdtls.ConnectionState
	tlsChecked bool
	// wantContinue asks the server handler to send 100 Continue for a request
	// whose header has arrived with Expect: 100-continue and whose body has
	// not; continued records that it was asked for the request in progress.
	wantContinue bool
	continued    bool
}

type frameInfo struct {
	end       int
	headerEnd int
	chunked   bool
	request   *stdhttp.Request
}

func NewParser(config Config) *Parser {
	defaults := DefaultConfig()
	if config.MaxHeaderBytes <= 0 {
		config.MaxHeaderBytes = defaults.MaxHeaderBytes
	}
	if config.MaxBodyBytes <= 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	return &Parser{config: config}
}

// Reset clears buffered state so the parser can be reused for another
// connection with the same configuration.
func (p *Parser) Reset() {
	if cap(p.buffer) > maxRetainedBuffer {
		p.buffer = nil
	} else {
		p.buffer = p.buffer[:0]
	}
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
}

// Feed may return zero, one, or several pipelined requests.
func (p *Parser) Feed(data []byte) ([]*stdhttp.Request, error) {
	var requests []*stdhttp.Request
	for {
		req, complete, err := p.FeedOne(data)
		data = nil
		if err != nil {
			return requests, err
		}
		if !complete {
			return requests, nil
		}
		requests = append(requests, req)
		if len(p.buffer) == 0 {
			return requests, nil
		}
	}
}

// FeedOne parses at most one request. Any bytes following that request remain
// buffered and can be retrieved with TakeBuffered. This is useful for protocol
// upgrades whose first frame may arrive in the same TCP read as the request.
func (p *Parser) FeedOne(data []byte) (*stdhttp.Request, bool, error) {
	p.buffer = append(p.buffer, data...)
	frame, complete, err := p.frameLength()
	if err != nil {
		p.buffer = nil
		p.headerScan = 0
		return nil, false, err
	}
	if !complete {
		return nil, false, nil
	}
	req := frame.request
	if frame.chunked {
		// net/http owns the chunk decoder; only chunked requests need this
		// second parse. Content-Length requests reuse the header parse below.
		req, err = readRequest(p.buffer[:frame.end], true)
		if err != nil {
			p.buffer = nil
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		req.Close = req.Close || frame.request.Close
		body, readErr := io.ReadAll(io.LimitReader(req.Body, p.config.MaxBodyBytes+1))
		_ = req.Body.Close()
		if readErr != nil || int64(len(body)) > p.config.MaxBodyBytes {
			p.buffer = nil
			if int64(len(body)) > p.config.MaxBodyBytes {
				return nil, false, ErrBodyTooLarge
			}
			return nil, false, fmt.Errorf("%w: %v", ErrMalformed, readErr)
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
	} else if req.ContentLength > 0 {
		body := append([]byte(nil), p.buffer[frame.headerEnd:frame.end]...)
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	p.consume(frame.end)
	return req, true, nil
}

// TakeBuffered returns and clears bytes read beyond the last parsed request.
func (p *Parser) TakeBuffered() []byte {
	data := p.buffer
	p.buffer = nil
	p.headerScan = 0
	p.continued, p.wantContinue = false, false
	return data
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		if cap(p.buffer) > maxRetainedBuffer {
			p.buffer = nil
		} else {
			p.buffer = p.buffer[:0]
		}
	} else {
		copy(p.buffer, p.buffer[n:])
		p.buffer = p.buffer[:len(p.buffer)-n]
	}
	p.headerScan = 0
	p.continued = false
}

func (p *Parser) frameLength() (frameInfo, bool, error) {
	headerAt := bytes.Index(p.buffer[p.headerScan:], []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(p.buffer) > p.config.MaxHeaderBytes {
			return frameInfo{}, false, ErrHeaderTooLarge
		}
		p.headerScan = len(p.buffer) - 3
		if p.headerScan < 0 {
			p.headerScan = 0
		}
		return frameInfo{}, false, nil
	}
	headerAt += p.headerScan
	headerEnd := headerAt + 4
	if headerEnd > p.config.MaxHeaderBytes {
		return frameInfo{}, false, ErrHeaderTooLarge
	}
	req, err := readRequest(p.buffer[:headerEnd], false)
	if err != nil {
		if strings.Contains(err.Error(), "unsupported transfer encoding") {
			// net/http refuses transfer codings other than chunked, without
			// an error type to tell that from a malformed request.
			return frameInfo{}, false, ErrUnsupportedTransferEncoding
		}
		return frameInfo{}, false, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	_ = req.Body.Close()
	if !req.ProtoAtLeast(1, 1) && hasHeaderField(p.buffer[:headerEnd], "transfer-encoding") {
		// HTTP/1.0 has no transfer codings, so a body framed by one cannot
		// be trusted to end where either side thinks (RFC 9112 section 6.1).
		// net/http drops the field, so it is looked for in the raw header.
		return frameInfo{}, false, fmt.Errorf("%w: Transfer-Encoding in an HTTP/1.0 request", ErrMalformed)
	}
	if len(req.TransferEncoding) != 0 {
		if len(req.TransferEncoding) != 1 || !strings.EqualFold(req.TransferEncoding[0], "chunked") {
			return frameInfo{}, false, ErrUnsupportedTransferEncoding
		}
		if hasHeaderField(p.buffer[:headerEnd], "content-length") {
			// Framed two ways, a request may be read differently by whatever
			// passed it on; chunked wins, and the connection ends after it
			// (RFC 9112 section 6.3).
			req.Close = true
		}
		end, complete, err := chunkedEnd(p.buffer, headerEnd, p.config.MaxHeaderBytes, p.config.MaxBodyBytes)
		if err == nil && !complete {
			p.expectContinue(req)
		}
		if err != nil || !complete {
			return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req}, complete, err
		}
		if int64(end-headerEnd) > p.config.MaxBodyBytes+int64(p.config.MaxHeaderBytes) {
			return frameInfo{}, false, ErrBodyTooLarge
		}
		return frameInfo{end: end, headerEnd: headerEnd, chunked: true, request: req}, true, nil
	}
	if req.ContentLength < 0 {
		return frameInfo{end: headerEnd, headerEnd: headerEnd, request: req}, true, nil
	}
	if req.ContentLength > p.config.MaxBodyBytes {
		return frameInfo{}, false, ErrBodyTooLarge
	}
	end64 := int64(headerEnd) + req.ContentLength
	if end64 > int64(len(p.buffer)) {
		p.expectContinue(req)
		return frameInfo{}, false, nil
	}
	return frameInfo{end: int(end64), headerEnd: headerEnd, request: req}, true, nil
}

// expectContinue notes a request, complete but for its body, that waits for
// 100 Continue before sending it.
func (p *Parser) expectContinue(req *stdhttp.Request) {
	if !p.continued && req.ProtoAtLeast(1, 1) && strings.EqualFold(req.Header.Get("Expect"), "100-continue") {
		p.continued = true
		p.wantContinue = true
	}
}

func readRequest(data []byte, keepBody bool) (*stdhttp.Request, error) {
	if keepBody {
		return stdhttp.ReadRequest(bufio.NewReaderSize(bytes.NewReader(data), 1024))
	}
	var source bytes.Reader
	source.Reset(data)
	reader := requestReaderPool.Get().(*bufio.Reader)
	reader.Reset(&source)
	req, err := stdhttp.ReadRequest(reader)
	if req != nil && !keepBody {
		// Detach the request from the pooled reader. FeedOne installs the real
		// Content-Length body after the complete frame has arrived.
		req.Body = stdhttp.NoBody
	}
	reader.Reset(bytes.NewReader(nil))
	requestReaderPool.Put(reader)
	return req, err
}

func chunkedEnd(data []byte, offset, maxTrailer int, maxBody int64) (int, bool, error) {
	var decoded uint64
	for {
		lineEnd := bytes.Index(data[offset:], []byte("\r\n"))
		if lineEnd < 0 {
			return 0, false, nil
		}
		line := string(data[offset : offset+lineEnd])
		if semicolon := strings.IndexByte(line, ';'); semicolon >= 0 {
			line = line[:semicolon]
		}
		size, err := strconv.ParseUint(strings.TrimSpace(line), 16, 63)
		if err != nil {
			return 0, false, fmt.Errorf("%w: invalid chunk size", ErrMalformed)
		}
		offset += lineEnd + 2
		if size == 0 {
			if len(data[offset:]) >= 2 && bytes.Equal(data[offset:offset+2], []byte("\r\n")) {
				return offset + 2, true, nil
			}
			trailerEnd := bytes.Index(data[offset:], []byte("\r\n\r\n"))
			if trailerEnd >= 0 {
				if trailerEnd+4 > maxTrailer {
					return 0, false, ErrHeaderTooLarge
				}
				return offset + trailerEnd + 4, true, nil
			}
			if len(data)-offset > maxTrailer {
				return 0, false, ErrHeaderTooLarge
			}
			return 0, false, nil
		}
		if size > uint64(maxBody)-decoded {
			return 0, false, ErrBodyTooLarge
		}
		decoded += size
		if size > uint64(len(data)-offset) {
			return 0, false, nil
		}
		chunkEnd := offset + int(size)
		if chunkEnd+2 > len(data) {
			return 0, false, nil
		}
		if !bytes.Equal(data[chunkEnd:chunkEnd+2], []byte("\r\n")) {
			return 0, false, fmt.Errorf("%w: invalid chunk terminator", ErrMalformed)
		}
		offset = chunkEnd + 2
	}
}

// hasHeaderField reports whether a raw header block has a field named name,
// which is given in lower case.
func hasHeaderField(header []byte, name string) bool {
	for line := range bytes.SplitSeq(header, []byte("\n")) {
		if len(line) > len(name) && line[len(name)] == ':' && strings.EqualFold(string(line[:len(name)]), name) {
			return true
		}
	}
	return false
}
