// Package http provides incremental HTTP/1.x request parsing and response
// handling for the parent epoll package.
package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
)

var (
	ErrHeaderTooLarge = errors.New("http: request header too large")
	ErrBodyTooLarge   = errors.New("http: request body too large")
	ErrMalformed      = errors.New("http: malformed request")
)

type Config struct {
	MaxHeaderBytes int
	MaxBodyBytes   int64
}

func DefaultConfig() Config {
	return Config{MaxHeaderBytes: 1 << 20, MaxBodyBytes: 16 << 20}
}

// Parser incrementally turns arbitrary TCP chunks into complete HTTP requests.
type Parser struct {
	config Config
	buffer []byte
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

// Feed may return zero, one, or several pipelined requests.
func (p *Parser) Feed(data []byte) ([]*stdhttp.Request, error) {
	p.buffer = append(p.buffer, data...)
	var requests []*stdhttp.Request
	for len(p.buffer) != 0 {
		frameLen, complete, err := p.frameLength()
		if err != nil {
			p.buffer = nil
			return requests, err
		}
		if !complete {
			return requests, nil
		}
		frame := p.buffer[:frameLen]
		req, err := stdhttp.ReadRequest(bufio.NewReader(bytes.NewReader(frame)))
		if err != nil {
			p.buffer = nil
			return requests, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		body, err := io.ReadAll(io.LimitReader(req.Body, p.config.MaxBodyBytes+1))
		_ = req.Body.Close()
		if err != nil {
			p.buffer = nil
			return requests, fmt.Errorf("%w: %v", ErrMalformed, err)
		}
		if int64(len(body)) > p.config.MaxBodyBytes {
			p.buffer = nil
			return requests, ErrBodyTooLarge
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		p.buffer = p.buffer[frameLen:]
		requests = append(requests, req)
	}
	return requests, nil
}

func (p *Parser) frameLength() (int, bool, error) {
	headerAt := bytes.Index(p.buffer, []byte("\r\n\r\n"))
	if headerAt < 0 {
		if len(p.buffer) > p.config.MaxHeaderBytes {
			return 0, false, ErrHeaderTooLarge
		}
		return 0, false, nil
	}
	headerEnd := headerAt + 4
	if headerEnd > p.config.MaxHeaderBytes {
		return 0, false, ErrHeaderTooLarge
	}
	req, err := stdhttp.ReadRequest(bufio.NewReader(bytes.NewReader(p.buffer[:headerEnd])))
	if err != nil {
		return 0, false, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	_ = req.Body.Close()
	if len(req.TransferEncoding) != 0 {
		if len(req.TransferEncoding) != 1 || !strings.EqualFold(req.TransferEncoding[0], "chunked") {
			return 0, false, fmt.Errorf("%w: unsupported transfer encoding", ErrMalformed)
		}
		end, complete, err := chunkedEnd(p.buffer, headerEnd, p.config.MaxHeaderBytes, p.config.MaxBodyBytes)
		if err != nil || !complete {
			return end, complete, err
		}
		if int64(end-headerEnd) > p.config.MaxBodyBytes+int64(p.config.MaxHeaderBytes) {
			return 0, false, ErrBodyTooLarge
		}
		return end, true, nil
	}
	if req.ContentLength < 0 {
		return headerEnd, true, nil
	}
	if req.ContentLength > p.config.MaxBodyBytes {
		return 0, false, ErrBodyTooLarge
	}
	end64 := int64(headerEnd) + req.ContentLength
	if end64 > int64(len(p.buffer)) {
		return 0, false, nil
	}
	return int(end64), true, nil
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
