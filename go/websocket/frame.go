// Package websocket implements RFC 6455 handshakes, incremental frame parsing,
// message reassembly, and response framing.
package websocket

import (
	"encoding/binary"
	"errors"
	"sync"
	"unicode/utf8"
)

type Opcode byte

const (
	Continuation Opcode = 0x0
	Text         Opcode = 0x1
	Binary       Opcode = 0x2
	Close        Opcode = 0x8
	Ping         Opcode = 0x9
	Pong         Opcode = 0xa
)

const (
	CloseNormal          = 1000
	CloseGoingAway       = 1001
	CloseProtocolError   = 1002
	CloseUnsupportedData = 1003
	CloseInvalidPayload  = 1007
	ClosePolicyViolation = 1008
	CloseMessageTooBig   = 1009
	CloseInternalError   = 1011
)

var (
	ErrProtocol       = errors.New("websocket: protocol error")
	ErrMessageTooBig  = errors.New("websocket: message too large")
	ErrInvalidPayload = errors.New("websocket: invalid payload")
)

const maxRetainedFrameBuffer = 64 << 10

type frameBuffer struct{ data []byte }

// frameBuffers recycles the arrays a parser adopts a partial frame into. A
// connection needs one only while a frame is split across reads, so pooling
// them lets a connection that is between messages hold none at all instead of
// keeping its own for as long as it stays open. At high connection counts
// those retained arrays were the largest thing on the heap.
var frameBuffers = sync.Pool{New: func() any { return new(frameBuffer) }}

type Event struct {
	Opcode  Opcode
	Payload []byte
}

type fragmentedMessage struct {
	opcode Opcode
	data   []byte
}

type Parser struct {
	maxMessageBytes int64
	// fromServer makes the parser read the frames a server sends, which are
	// unmasked, instead of the masked frames a client sends. Either side must
	// reject a frame masked the other way (RFC 6455 section 5.1).
	fromServer     bool
	buffer         []byte
	owned          *frameBuffer
	borrowedTail   []byte
	fragment       *fragmentedMessage
	pendingConsume int
	borrowedBuffer bool
}

func NewParser(maxMessageBytes int64) *Parser {
	if maxMessageBytes <= 0 {
		maxMessageBytes = 16 << 20
	}
	return &Parser{maxMessageBytes: maxMessageBytes}
}

// NewServerFrameParser returns a parser for the unmasked frames a server sends,
// which is what a client reads.
func NewServerFrameParser(maxMessageBytes int64) *Parser {
	p := NewParser(maxMessageBytes)
	p.fromServer = true
	return p
}

func (p *Parser) Reset() {
	if p.borrowedBuffer || cap(p.buffer) > maxRetainedFrameBuffer {
		p.buffer = nil
	} else {
		p.buffer = p.buffer[:0]
	}
	if p.fragment != nil {
		p.fragment.opcode = 0
		if cap(p.fragment.data) > maxRetainedFrameBuffer {
			p.fragment = nil
		} else {
			p.fragment.data = p.fragment.data[:0]
		}
	}
	p.pendingConsume = 0
	p.borrowedBuffer = false
	p.borrowedTail = nil
	if len(p.buffer) == 0 {
		p.buffer = nil
		p.releaseOwned()
	}
}

// Feed parses frames and returns complete messages and control frames. A
// parser from NewParser reads a client's masked frames; see
// NewServerFrameParser for the other direction. Fragmented data messages are reassembled before being returned.
func (p *Parser) Feed(data []byte) ([]Event, error) {
	var events []Event
	for {
		event, complete, err := p.FeedOne(data)
		data = nil
		if err != nil || !complete {
			return events, err
		}
		events = append(events, event)
	}
}

// FeedOne returns at most one complete event without allocating an event
// slice. Call it again with nil to drain additional frames already buffered.
func (p *Parser) FeedOne(data []byte) (Event, bool, error) {
	return p.feedOne(data, false)
}

// FeedOneBorrowed is the allocation-free variant used by ServerHandler for
// unfragmented frames. Event.Payload remains valid only until the next parser
// call and must not be retained by the callback.
func (p *Parser) FeedOneBorrowed(data []byte) (Event, bool, error) {
	return p.feedOne(data, true)
}

// ReleaseBorrowed detaches any input buffer retained by FeedOneBorrowed. It is
// cheap to call after each network-data callback.
func (p *Parser) ReleaseBorrowed() {
	if p.pendingConsume != 0 {
		p.consume(p.pendingConsume)
		p.pendingConsume = 0
	}
	p.borrowedTail = nil
	if p.borrowedBuffer {
		p.buffer = nil
		p.borrowedBuffer = false
	}
	if len(p.buffer) == 0 {
		// Nothing is half-parsed, so the adopted array can go back for another
		// connection to use until this one needs one again.
		p.buffer = nil
		p.releaseOwned()
	}
}

// releaseOwned returns the adopted array to the pool. The caller must have
// established that no partial frame still lives in it. An array that outgrew
// the retention limit is dropped instead, so one large message cannot leave
// every pooled array permanently inflated.
func (p *Parser) releaseOwned() {
	if p.owned == nil {
		return
	}
	if cap(p.owned.data) <= maxRetainedFrameBuffer {
		p.owned.data = p.owned.data[:0]
		frameBuffers.Put(p.owned)
	}
	p.owned = nil
}

func (p *Parser) feedOne(data []byte, borrowPayload bool) (Event, bool, error) {
	if p.pendingConsume != 0 {
		p.consume(p.pendingConsume)
		p.pendingConsume = 0
	}
	if len(p.buffer) == 0 && len(p.borrowedTail) != 0 {
		p.buffer = p.borrowedTail
		p.borrowedTail = nil
		p.borrowedBuffer = true
	}
	if borrowPayload && len(p.buffer) == 0 && len(data) != 0 {
		p.buffer = data
		p.borrowedBuffer = true
	} else if borrowPayload && !p.borrowedBuffer && len(p.buffer) != 0 && len(data) != 0 {
		p.buffer, p.borrowedTail = appendCurrentFrame(p.buffer, data)
		p.keepOwned()
	} else {
		p.buffer = append(p.buffer, data...)
		p.keepOwned()
	}
	for {
		event, emit, complete, err := p.next(borrowPayload)
		if err != nil {
			p.buffer = nil
			p.owned = nil
			p.borrowedBuffer = false
			p.borrowedTail = nil
			p.fragment = nil
			return Event{}, false, err
		}
		if !complete {
			if p.borrowedBuffer && len(p.buffer) != 0 {
				p.adopt()
			}
			return Event{}, false, nil
		}
		if emit {
			return event, true, nil
		}
	}
}

// keepOwned remembers the parser-owned array behind buffer so a later adopt can
// reuse it. An emptied or borrowed buffer must not displace a larger array that
// is still worth keeping, so the retained one only ever grows.
func (p *Parser) keepOwned() {
	if p.borrowedBuffer || p.owned == nil || cap(p.buffer) < cap(p.owned.data) {
		return
	}
	p.owned.data = p.buffer
}

// adopt copies the borrowed tail into the parser's own array so the read
// buffer can go back to its pool. At high message rates almost every read ends
// mid-frame, so the array is retained and reused across reads rather than
// allocated each time.
func (p *Parser) adopt() {
	capacity := len(p.buffer)
	// Sizing to the whole frame avoids regrowing while the rest of it arrives,
	// but only up to the retention limit: a peer that announces a huge frame
	// must not make every connection reserve it from the first bytes onward.
	if frameEnd, known := frameSize(p.buffer); known && frameEnd > capacity &&
		uint64(frameEnd) <= uint64(p.maxMessageBytes)+14 {
		if frameEnd > maxRetainedFrameBuffer {
			frameEnd = maxRetainedFrameBuffer
		}
		if frameEnd > capacity {
			capacity = frameEnd
		}
	}
	if p.owned == nil {
		p.owned = frameBuffers.Get().(*frameBuffer)
	}
	if cap(p.owned.data) < capacity {
		p.owned.data = make([]byte, 0, capacity)
	}
	p.owned.data = append(p.owned.data[:0], p.buffer...)
	p.buffer = p.owned.data
	p.borrowedBuffer = false
}

func appendCurrentFrame(buffer, data []byte) ([]byte, []byte) {
	for {
		frameEnd, known := frameSize(buffer)
		if known {
			need := frameEnd - len(buffer)
			if need <= 0 || need >= len(data) {
				return append(buffer, data...), nil
			}
			return append(buffer, data[:need]...), data[need:]
		}
		need := frameHeaderSize(buffer) - len(buffer)
		if need <= 0 || need >= len(data) {
			return append(buffer, data...), nil
		}
		buffer = append(buffer, data[:need]...)
		data = data[need:]
	}
}

func frameHeaderSize(data []byte) int {
	if len(data) < 2 {
		return 2
	}
	size := 2
	if data[1]&0x80 != 0 {
		size += 4
	}
	switch data[1] & 0x7f {
	case 126:
		return size + 2
	case 127:
		return size + 8
	default:
		return size
	}
}

func frameSize(data []byte) (int, bool) {
	headerSize := frameHeaderSize(data)
	if len(data) < headerSize {
		return 0, false
	}
	payloadLen := uint64(data[1] & 0x7f)
	if payloadLen == 126 {
		payloadLen = uint64(binary.BigEndian.Uint16(data[2:4]))
	} else if payloadLen == 127 {
		payloadLen = binary.BigEndian.Uint64(data[2:10])
	}
	if payloadLen > uint64(^uint(0)>>1)-uint64(headerSize) {
		return 0, false
	}
	return headerSize + int(payloadLen), true
}

func (p *Parser) next(borrowPayload bool) (Event, bool, bool, error) {
	if len(p.buffer) < 2 {
		return Event{}, false, false, nil
	}
	first, second := p.buffer[0], p.buffer[1]
	fin := first&0x80 != 0
	opcode := Opcode(first & 0x0f)
	masked := second&0x80 != 0
	if first&0x70 != 0 || masked == p.fromServer {
		return Event{}, false, false, ErrProtocol
	}
	control := opcode >= 0x8
	if control && (!fin || second&0x7f > 125) {
		return Event{}, false, false, ErrProtocol
	}
	switch opcode {
	case Continuation:
		if p.fragment == nil {
			return Event{}, false, false, ErrProtocol
		}
	case Text, Binary:
		if p.fragment != nil {
			return Event{}, false, false, ErrProtocol
		}
	case Close, Ping, Pong:
	default:
		return Event{}, false, false, ErrProtocol
	}

	offset := 2
	payloadLen := uint64(second & 0x7f)
	if payloadLen == 126 {
		if len(p.buffer) < offset+2 {
			return Event{}, false, false, nil
		}
		payloadLen = uint64(binary.BigEndian.Uint16(p.buffer[offset : offset+2]))
		offset += 2
		if payloadLen < 126 {
			return Event{}, false, false, ErrProtocol
		}
	} else if payloadLen == 127 {
		if len(p.buffer) < offset+8 {
			return Event{}, false, false, nil
		}
		payloadLen = binary.BigEndian.Uint64(p.buffer[offset : offset+8])
		offset += 8
		if payloadLen < 65536 || payloadLen>>63 != 0 {
			return Event{}, false, false, ErrProtocol
		}
	}
	if control && payloadLen > 125 {
		return Event{}, false, false, ErrProtocol
	}
	current := int64(0)
	if p.fragment != nil {
		current = int64(len(p.fragment.data))
	}
	if !control && (payloadLen > uint64(p.maxMessageBytes) || current > p.maxMessageBytes-int64(payloadLen)) {
		return Event{}, false, false, ErrMessageTooBig
	}
	var mask []byte
	if masked {
		if len(p.buffer) < offset+4 {
			return Event{}, false, false, nil
		}
		mask = p.buffer[offset : offset+4]
		offset += 4
	}
	if payloadLen > uint64(len(p.buffer)-offset) {
		return Event{}, false, false, nil
	}
	frameEnd := offset + int(payloadLen)
	var payload []byte
	if borrowPayload {
		payload = p.buffer[offset:frameEnd]
		if masked {
			applyMask(payload, payload, mask)
		}
	} else {
		payload = make([]byte, int(payloadLen))
		if masked {
			applyMask(payload, p.buffer[offset:frameEnd], mask)
		} else {
			copy(payload, p.buffer[offset:frameEnd])
		}
	}

	if control {
		if opcode == Close {
			if len(payload) == 1 || (len(payload) >= 2 && !validCloseCode(binary.BigEndian.Uint16(payload[:2]))) {
				return Event{}, false, false, ErrProtocol
			}
			if len(payload) > 2 && !utf8.Valid(payload[2:]) {
				return Event{}, false, false, ErrInvalidPayload
			}
		}
		p.finishFrame(frameEnd, borrowPayload)
		return Event{Opcode: opcode, Payload: payload}, true, true, nil
	}
	if opcode == Text || opcode == Binary {
		if fin {
			if opcode == Text && !utf8.Valid(payload) {
				return Event{}, false, false, ErrInvalidPayload
			}
			p.finishFrame(frameEnd, borrowPayload)
			return Event{Opcode: opcode, Payload: payload}, true, true, nil
		}
		p.fragment = &fragmentedMessage{opcode: opcode, data: append([]byte(nil), payload...)}
		p.consume(frameEnd)
		return Event{}, false, true, nil
	}
	p.fragment.data = append(p.fragment.data, payload...)
	p.consume(frameEnd)
	if !fin {
		return Event{}, false, true, nil
	}
	event := Event{Opcode: p.fragment.opcode, Payload: p.fragment.data}
	if event.Opcode == Text && !utf8.Valid(event.Payload) {
		return Event{}, false, false, ErrInvalidPayload
	}
	p.fragment = nil
	return event, true, true, nil
}

func applyMask(dst, src, mask []byte) {
	mask32 := binary.LittleEndian.Uint32(mask)
	mask64 := uint64(mask32) | uint64(mask32)<<32
	i := 0
	for ; i+8 <= len(src); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:i+8], binary.LittleEndian.Uint64(src[i:i+8])^mask64)
	}
	for ; i < len(src); i++ {
		dst[i] = src[i] ^ mask[i&3]
	}
}

func (p *Parser) finishFrame(frameEnd int, borrowed bool) {
	if borrowed {
		p.pendingConsume = frameEnd
	} else {
		p.consume(frameEnd)
	}
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		switch {
		case p.borrowedBuffer:
			// The retained array is kept: the next partial frame reuses it.
			p.buffer = nil
		case cap(p.buffer) > maxRetainedFrameBuffer:
			p.buffer, p.owned = nil, nil
		default:
			p.buffer = p.buffer[:0]
		}
		p.borrowedBuffer = false
		return
	}
	copy(p.buffer, p.buffer[n:])
	p.buffer = p.buffer[:len(p.buffer)-n]
}

func validCloseCode(code uint16) bool {
	if code >= 1000 && code <= 1014 {
		return code != 1004 && code != 1005 && code != 1006
	}
	return code >= 3000 && code <= 4999
}

// MarshalFrame builds an unmasked server-to-client frame.
func MarshalFrame(opcode Opcode, payload []byte) ([]byte, error) {
	if opcode != Text && opcode != Binary && opcode != Close && opcode != Ping && opcode != Pong {
		return nil, ErrProtocol
	}
	if opcode >= 0x8 && len(payload) > 125 {
		return nil, ErrProtocol
	}
	headerLen := 2
	if len(payload) >= 126 && len(payload) <= 65535 {
		headerLen += 2
	} else if len(payload) > 65535 {
		headerLen += 8
	}
	frame := make([]byte, headerLen+len(payload))
	frame[0] = 0x80 | byte(opcode)
	offset := 2
	switch {
	case len(payload) < 126:
		frame[1] = byte(len(payload))
	case len(payload) <= 65535:
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(len(payload)))
		offset = 4
	default:
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(len(payload)))
		offset = 10
	}
	copy(frame[offset:], payload)
	return frame, nil
}

func frameHeader(opcode Opcode, payloadLen int) ([10]byte, int, error) {
	var header [10]byte
	if opcode != Text && opcode != Binary && opcode != Close && opcode != Ping && opcode != Pong {
		return header, 0, ErrProtocol
	}
	if opcode >= 0x8 && payloadLen > 125 {
		return header, 0, ErrProtocol
	}
	header[0] = 0x80 | byte(opcode)
	switch {
	case payloadLen < 126:
		header[1] = byte(payloadLen)
		return header, 2, nil
	case payloadLen <= 65535:
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(payloadLen))
		return header, 4, nil
	default:
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(payloadLen))
		return header, 10, nil
	}
}
