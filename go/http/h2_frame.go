package http

import (
	"encoding/binary"
	"fmt"
)

// HTTP/2 framing (RFC 9113 section 4 and 6), shared by the server and the
// client.

// h2Preface is what a client sends first on every HTTP/2 connection.
const h2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

const (
	h2FrameHeaderLen = 9
	// h2DefaultMaxFrameSize and h2DefaultWindow are what each side assumes
	// of the other until SETTINGS says otherwise.
	h2DefaultMaxFrameSize = 16384
	h2MaxFrameSizeLimit   = 1<<24 - 1
	h2DefaultWindow       = 65535
	h2MaxWindow           = 1<<31 - 1
	h2MaxStreamID         = 1<<31 - 1
)

type h2FrameType uint8

const (
	h2FrameData         h2FrameType = 0x0
	h2FrameHeaders      h2FrameType = 0x1
	h2FramePriority     h2FrameType = 0x2
	h2FrameRSTStream    h2FrameType = 0x3
	h2FrameSettings     h2FrameType = 0x4
	h2FramePushPromise  h2FrameType = 0x5
	h2FramePing         h2FrameType = 0x6
	h2FrameGoAway       h2FrameType = 0x7
	h2FrameWindowUpdate h2FrameType = 0x8
	h2FrameContinuation h2FrameType = 0x9
)

const (
	h2FlagEndStream  = 0x1
	h2FlagAck        = 0x1
	h2FlagEndHeaders = 0x4
	h2FlagPadded     = 0x8
	h2FlagPriority   = 0x20
)

type h2SettingID uint16

const (
	h2SettingHeaderTableSize      h2SettingID = 0x1
	h2SettingEnablePush           h2SettingID = 0x2
	h2SettingMaxConcurrentStreams h2SettingID = 0x3
	h2SettingInitialWindowSize    h2SettingID = 0x4
	h2SettingMaxFrameSize         h2SettingID = 0x5
	h2SettingMaxHeaderListSize    h2SettingID = 0x6
)

// H2ErrorCode is an HTTP/2 error code, carried by RST_STREAM and GOAWAY.
type H2ErrorCode uint32

const (
	H2NoError            H2ErrorCode = 0x0
	H2ProtocolError      H2ErrorCode = 0x1
	H2InternalError      H2ErrorCode = 0x2
	H2FlowControlError   H2ErrorCode = 0x3
	H2SettingsTimeout    H2ErrorCode = 0x4
	H2StreamClosed       H2ErrorCode = 0x5
	H2FrameSizeError     H2ErrorCode = 0x6
	H2RefusedStream      H2ErrorCode = 0x7
	H2Cancel             H2ErrorCode = 0x8
	H2CompressionError   H2ErrorCode = 0x9
	H2ConnectError       H2ErrorCode = 0xa
	H2EnhanceYourCalm    H2ErrorCode = 0xb
	H2InadequateSecurity H2ErrorCode = 0xc
	H2HTTP11Required     H2ErrorCode = 0xd
)

var h2ErrorNames = [...]string{
	"NO_ERROR", "PROTOCOL_ERROR", "INTERNAL_ERROR", "FLOW_CONTROL_ERROR", "SETTINGS_TIMEOUT",
	"STREAM_CLOSED", "FRAME_SIZE_ERROR", "REFUSED_STREAM", "CANCEL", "COMPRESSION_ERROR",
	"CONNECT_ERROR", "ENHANCE_YOUR_CALM", "INADEQUATE_SECURITY", "HTTP_1_1_REQUIRED",
}

func (c H2ErrorCode) String() string {
	if int(c) < len(h2ErrorNames) {
		return h2ErrorNames[c]
	}
	return fmt.Sprintf("unknown error code 0x%x", uint32(c))
}

// H2ConnError ends a whole connection: this side sends it in GOAWAY, or the
// peer sent it, and OnClose and failed requests report it.
type H2ConnError struct {
	Code   H2ErrorCode
	Reason string
	// Remote is set when the peer reported the error rather than this side.
	Remote bool
}

func (e *H2ConnError) Error() string {
	who := "connection error"
	if e.Remote {
		who = "peer sent GOAWAY"
	}
	if e.Reason == "" {
		return fmt.Sprintf("http2: %s: %v", who, e.Code)
	}
	return fmt.Sprintf("http2: %s: %v: %s", who, e.Code, e.Reason)
}

// H2StreamError ends a single stream with RST_STREAM.
type H2StreamError struct {
	StreamID uint32
	Code     H2ErrorCode
}

func (e *H2StreamError) Error() string {
	return fmt.Sprintf("http2: stream %d reset: %v", e.StreamID, e.Code)
}

func h2ConnErr(code H2ErrorCode, format string, args ...any) *H2ConnError {
	return &H2ConnError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

// h2Frame is a frame parsed in place: payload points into the read buffer
// and is only valid until the next read. Padding is already removed.
type h2Frame struct {
	typ      h2FrameType
	flags    uint8
	streamID uint32
	payload  []byte
	// length is the payload length before padding was removed, which is
	// what flow control counts for DATA.
	length uint32
}

func (f *h2Frame) has(flag uint8) bool { return f.flags&flag != 0 }

// h2ReadFrame parses one frame from the front of buf, reporting how many
// bytes it took, or zero if the frame is not complete yet.
func h2ReadFrame(buf []byte, maxFrameSize uint32) (h2Frame, int, error) {
	if len(buf) < h2FrameHeaderLen {
		return h2Frame{}, 0, nil
	}
	length := uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])
	if length > maxFrameSize {
		return h2Frame{}, 0, h2ConnErr(H2FrameSizeError, "frame of %d bytes exceeds %d", length, maxFrameSize)
	}
	end := h2FrameHeaderLen + int(length)
	if len(buf) < end {
		return h2Frame{}, 0, nil
	}
	f := h2Frame{
		typ:      h2FrameType(buf[3]),
		flags:    buf[4],
		streamID: binary.BigEndian.Uint32(buf[5:9]) & 0x7fffffff,
		payload:  buf[h2FrameHeaderLen:end],
		length:   length,
	}
	if f.has(h2FlagPadded) && (f.typ == h2FrameData || f.typ == h2FrameHeaders || f.typ == h2FramePushPromise) {
		if len(f.payload) == 0 {
			return h2Frame{}, 0, h2ConnErr(H2FrameSizeError, "padded frame without pad length")
		}
		pad := int(f.payload[0])
		if pad >= len(f.payload) {
			return h2Frame{}, 0, h2ConnErr(H2ProtocolError, "padding exceeds frame")
		}
		f.payload = f.payload[1 : len(f.payload)-pad]
	}
	return f, end, nil
}

func h2AppendFrameHeader(dst []byte, typ h2FrameType, flags uint8, streamID uint32, length int) []byte {
	return append(dst, byte(length>>16), byte(length>>8), byte(length), byte(typ), flags,
		byte(streamID>>24)&0x7f, byte(streamID>>16), byte(streamID>>8), byte(streamID))
}

func h2AppendSettings(dst []byte, settings ...[2]uint32) []byte {
	dst = h2AppendFrameHeader(dst, h2FrameSettings, 0, 0, 6*len(settings))
	for _, s := range settings {
		dst = binary.BigEndian.AppendUint16(dst, uint16(s[0]))
		dst = binary.BigEndian.AppendUint32(dst, s[1])
	}
	return dst
}

func h2AppendWindowUpdate(dst []byte, streamID uint32, increment uint32) []byte {
	dst = h2AppendFrameHeader(dst, h2FrameWindowUpdate, 0, streamID, 4)
	return binary.BigEndian.AppendUint32(dst, increment&0x7fffffff)
}

func h2AppendRSTStream(dst []byte, streamID uint32, code H2ErrorCode) []byte {
	dst = h2AppendFrameHeader(dst, h2FrameRSTStream, 0, streamID, 4)
	return binary.BigEndian.AppendUint32(dst, uint32(code))
}

func h2AppendGoAway(dst []byte, lastStreamID uint32, code H2ErrorCode, debug string) []byte {
	dst = h2AppendFrameHeader(dst, h2FrameGoAway, 0, 0, 8+len(debug))
	dst = binary.BigEndian.AppendUint32(dst, lastStreamID&0x7fffffff)
	dst = binary.BigEndian.AppendUint32(dst, uint32(code))
	return append(dst, debug...)
}

// h2AppendHeaderBlock splits a header block into a HEADERS frame and as many
// CONTINUATION frames as maxFrameSize requires.
func h2AppendHeaderBlock(dst []byte, streamID uint32, block []byte, endStream bool, maxFrameSize int) []byte {
	typ := h2FrameHeaders
	var flags uint8
	if endStream {
		flags = h2FlagEndStream
	}
	for {
		chunk := block
		if len(chunk) > maxFrameSize {
			chunk = chunk[:maxFrameSize]
		}
		block = block[len(chunk):]
		f := flags
		if len(block) == 0 {
			f |= h2FlagEndHeaders
		}
		dst = h2AppendFrameHeader(dst, typ, f, streamID, len(chunk))
		dst = append(dst, chunk...)
		if len(block) == 0 {
			return dst
		}
		typ, flags = h2FrameContinuation, 0
	}
}

// h2AppendPushPromise promises stream promisedID on streamID, splitting the
// header block across CONTINUATION frames as maxFrameSize requires.
func h2AppendPushPromise(dst []byte, streamID, promisedID uint32, block []byte, maxFrameSize int) []byte {
	chunk := block[:min(len(block), maxFrameSize-4)]
	block = block[len(chunk):]
	var flags uint8
	if len(block) == 0 {
		flags = h2FlagEndHeaders
	}
	dst = h2AppendFrameHeader(dst, h2FramePushPromise, flags, streamID, 4+len(chunk))
	dst = binary.BigEndian.AppendUint32(dst, promisedID&0x7fffffff)
	dst = append(dst, chunk...)
	for len(block) > 0 {
		chunk = block[:min(len(block), maxFrameSize)]
		block = block[len(chunk):]
		flags = 0
		if len(block) == 0 {
			flags = h2FlagEndHeaders
		}
		dst = h2AppendFrameHeader(dst, h2FrameContinuation, flags, streamID, len(chunk))
		dst = append(dst, chunk...)
	}
	return dst
}

// h2ParseSettings calls apply for each setting in a SETTINGS payload.
func h2ParseSettings(payload []byte, apply func(id h2SettingID, value uint32) error) error {
	if len(payload)%6 != 0 {
		return h2ConnErr(H2FrameSizeError, "SETTINGS length %d", len(payload))
	}
	for ; len(payload) > 0; payload = payload[6:] {
		if err := apply(h2SettingID(binary.BigEndian.Uint16(payload)), binary.BigEndian.Uint32(payload[2:])); err != nil {
			return err
		}
	}
	return nil
}

// h2ConnectionHeaders are HTTP/1 hop-by-hop headers, which HTTP/2 forbids
// (RFC 9113 section 8.2.2).
var h2ConnectionHeaders = map[string]bool{
	"connection":        true,
	"keep-alive":        true,
	"proxy-connection":  true,
	"transfer-encoding": true,
	"upgrade":           true,
}

// h2ValidHeaderName reports whether name is a valid lowercase field name.
func h2ValidHeaderName(name string) bool {
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

// h2ValidHeaderValue reports whether value may appear in a field, which rules
// out NUL, CR, LF and leading or trailing whitespace (RFC 9113 section 8.2.1).
func h2ValidHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if c := value[i]; c == 0 || c == '\r' || c == '\n' {
			return false
		}
	}
	if n := len(value); n > 0 && (value[0] == ' ' || value[0] == '\t' || value[n-1] == ' ' || value[n-1] == '\t') {
		return false
	}
	return true
}
