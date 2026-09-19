//go:build linux || darwin || windows

package http3

import (
	"errors"
	"fmt"

	"github.com/lesismal/fib/go/http3/internal/quic"
)

// Frame types (RFC 9114 section 7.2).
const (
	frameData        = 0x00
	frameHeaders     = 0x01
	frameCancelPush  = 0x03
	frameSettings    = 0x04
	framePushPromise = 0x05
	frameGoAway      = 0x07
	frameMaxPushID   = 0x0d
)

// Unidirectional stream types (RFC 9114 section 6.2 and RFC 9204 section
// 4.2).
const (
	streamControl      = 0x00
	streamPush         = 0x01
	streamQPACKEncoder = 0x02
	streamQPACKDecoder = 0x03
)

// Settings (RFC 9114 section 7.2.4.1 and RFC 9204 section 5).
const (
	settingQPACKMaxTableCapacity = 0x01
	settingMaxFieldSectionSize   = 0x06
	settingQPACKBlockedStreams   = 0x07
)

// ErrorCode is an HTTP/3 error code (RFC 9114 section 8.1), carried by a
// closed connection or a reset stream.
type ErrorCode uint64

const (
	ErrCodeNoError              ErrorCode = 0x100
	ErrCodeGeneralProtocolError ErrorCode = 0x101
	ErrCodeInternalError        ErrorCode = 0x102
	ErrCodeStreamCreationError  ErrorCode = 0x103
	ErrCodeClosedCriticalStream ErrorCode = 0x104
	ErrCodeFrameUnexpected      ErrorCode = 0x105
	ErrCodeFrameError           ErrorCode = 0x106
	ErrCodeExcessiveLoad        ErrorCode = 0x107
	ErrCodeIDError              ErrorCode = 0x108
	ErrCodeSettingsError        ErrorCode = 0x109
	ErrCodeMissingSettings      ErrorCode = 0x10a
	ErrCodeRequestRejected      ErrorCode = 0x10b
	ErrCodeRequestCancelled     ErrorCode = 0x10c
	ErrCodeRequestIncomplete    ErrorCode = 0x10d
	ErrCodeMessageError         ErrorCode = 0x10e
	ErrCodeConnectError         ErrorCode = 0x10f
	ErrCodeVersionFallback      ErrorCode = 0x110
	// ErrCodeQPACKDecompressionFailed is QPACK's (RFC 9204 section 6).
	ErrCodeQPACKDecompressionFailed ErrorCode = 0x200
)

var errorNames = map[ErrorCode]string{
	ErrCodeNoError:                  "H3_NO_ERROR",
	ErrCodeGeneralProtocolError:     "H3_GENERAL_PROTOCOL_ERROR",
	ErrCodeInternalError:            "H3_INTERNAL_ERROR",
	ErrCodeStreamCreationError:      "H3_STREAM_CREATION_ERROR",
	ErrCodeClosedCriticalStream:     "H3_CLOSED_CRITICAL_STREAM",
	ErrCodeFrameUnexpected:          "H3_FRAME_UNEXPECTED",
	ErrCodeFrameError:               "H3_FRAME_ERROR",
	ErrCodeExcessiveLoad:            "H3_EXCESSIVE_LOAD",
	ErrCodeIDError:                  "H3_ID_ERROR",
	ErrCodeSettingsError:            "H3_SETTINGS_ERROR",
	ErrCodeMissingSettings:          "H3_MISSING_SETTINGS",
	ErrCodeRequestRejected:          "H3_REQUEST_REJECTED",
	ErrCodeRequestCancelled:         "H3_REQUEST_CANCELLED",
	ErrCodeRequestIncomplete:        "H3_REQUEST_INCOMPLETE",
	ErrCodeMessageError:             "H3_MESSAGE_ERROR",
	ErrCodeConnectError:             "H3_CONNECT_ERROR",
	ErrCodeVersionFallback:          "H3_VERSION_FALLBACK",
	ErrCodeQPACKDecompressionFailed: "QPACK_DECOMPRESSION_FAILED",
}

func (c ErrorCode) String() string {
	if name, ok := errorNames[c]; ok {
		return name
	}
	return fmt.Sprintf("H3 error 0x%x", uint64(c))
}

// connError is an error that ends the whole connection.
type connError struct {
	code   ErrorCode
	reason string
}

func (e *connError) Error() string { return "http3: " + e.code.String() + ": " + e.reason }

func connErr(code ErrorCode, reason string) *connError { return &connError{code: code, reason: reason} }

// StreamError is a request that the peer, or this side, reset.
type StreamError struct {
	Code   ErrorCode
	Remote bool
}

func (e *StreamError) Error() string {
	if e.Remote {
		return "http3: stream reset by peer: " + e.Code.String()
	}
	return "http3: stream reset: " + e.Code.String()
}

// errStreamClosed is what writing to a stream that is gone gets.
var errStreamClosed = errors.New("http3: stream closed")

func appendFrameHeader(b []byte, typ uint64, length int) []byte {
	b = quic.AppendVarint(b, typ)
	return quic.AppendVarint(b, uint64(length))
}

func appendSettings(b []byte, settings ...[2]uint64) []byte {
	var payload []byte
	for _, s := range settings {
		payload = quic.AppendVarint(payload, s[0])
		payload = quic.AppendVarint(payload, s[1])
	}
	b = appendFrameHeader(b, frameSettings, len(payload))
	return append(b, payload...)
}

// parseSettings checks a SETTINGS frame and returns the peer's
// MAX_FIELD_SECTION_SIZE, or zero for no limit.
func parseSettings(payload []byte) (uint64, error) {
	seen := make(map[uint64]bool)
	var maxField uint64
	for len(payload) > 0 {
		id, n := quic.ReadVarint(payload)
		if n == 0 {
			return 0, connErr(ErrCodeFrameError, "truncated SETTINGS")
		}
		payload = payload[n:]
		value, n := quic.ReadVarint(payload)
		if n == 0 {
			return 0, connErr(ErrCodeFrameError, "truncated SETTINGS")
		}
		payload = payload[n:]
		if seen[id] {
			return 0, connErr(ErrCodeSettingsError, "duplicate setting")
		}
		seen[id] = true
		switch id {
		case 0x02, 0x03, 0x04, 0x05:
			// HTTP/2 settings that HTTP/3 reserves.
			return 0, connErr(ErrCodeSettingsError, "HTTP/2 setting in HTTP/3")
		case settingMaxFieldSectionSize:
			maxField = value
		}
	}
	return maxField, nil
}

// frameParser splits a stream into frames. DATA payloads are handed on as
// they arrive; every other frame is collected whole first.
type frameParser struct {
	buf []byte
	// dataLeft is how much of the current DATA frame is still to come.
	dataLeft uint64
	// maxFrame bounds a frame other than DATA.
	maxFrame uint64
}

// feed parses what data completes. onData receives DATA payload pieces and
// onFrame every other frame, neither of which may keep what it is given.
func (p *frameParser) feed(data []byte, onData func([]byte) error, onFrame func(typ uint64, payload []byte) error) error {
	p.buf = append(p.buf, data...)
	buf := p.buf
	var err error
	for err == nil {
		if p.dataLeft > 0 {
			n := min(uint64(len(buf)), p.dataLeft)
			if n == 0 {
				break
			}
			err = onData(buf[:n])
			buf = buf[n:]
			p.dataLeft -= n
			continue
		}
		typ, n1 := quic.ReadVarint(buf)
		if n1 == 0 {
			break
		}
		length, n2 := quic.ReadVarint(buf[n1:])
		if n2 == 0 {
			break
		}
		if typ == frameData {
			buf = buf[n1+n2:]
			p.dataLeft = length
			err = onData(nil)
			continue
		}
		if length > p.maxFrame {
			return connErr(ErrCodeExcessiveLoad, "frame too large")
		}
		if uint64(len(buf)-n1-n2) < length {
			break
		}
		payload := buf[n1+n2 : n1+n2+int(length)]
		buf = buf[n1+n2+int(length):]
		err = onFrame(typ, payload)
	}
	p.buf = append(p.buf[:0], buf...)
	if cap(p.buf) > 64<<10 && len(p.buf) < 1<<10 {
		p.buf = append([]byte(nil), p.buf...)
	}
	return err
}

// midFrame reports whether the stream stopped inside a frame.
func (p *frameParser) midFrame() bool { return p.dataLeft > 0 || len(p.buf) > 0 }

// reservedFrame reports whether typ is an HTTP/2 frame type that HTTP/3
// forbids (RFC 9114 section 7.2.8).
func reservedFrame(typ uint64) bool {
	return typ == 0x02 || typ == 0x06 || typ == 0x08 || typ == 0x09
}
