// Package websocket implements RFC 6455 handshakes, incremental frame parsing,
// message reassembly, and response framing.
package websocket

import (
	"encoding/binary"
	"errors"
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

type Event struct {
	Opcode  Opcode
	Payload []byte
}

type Parser struct {
	maxMessageBytes int64
	buffer          []byte
	fragmentOpcode  Opcode
	fragment        []byte
}

func NewParser(maxMessageBytes int64) *Parser {
	if maxMessageBytes <= 0 {
		maxMessageBytes = 16 << 20
	}
	return &Parser{maxMessageBytes: maxMessageBytes}
}

// Feed parses masked client frames and returns complete messages and control
// frames. Fragmented data messages are reassembled before being returned.
func (p *Parser) Feed(data []byte) ([]Event, error) {
	p.buffer = append(p.buffer, data...)
	var events []Event
	for {
		event, emit, complete, err := p.next()
		if err != nil {
			p.buffer = nil
			p.fragment = nil
			p.fragmentOpcode = 0
			return events, err
		}
		if !complete {
			return events, nil
		}
		if emit {
			events = append(events, event)
		}
	}
}

func (p *Parser) next() (Event, bool, bool, error) {
	if len(p.buffer) < 2 {
		return Event{}, false, false, nil
	}
	first, second := p.buffer[0], p.buffer[1]
	fin := first&0x80 != 0
	opcode := Opcode(first & 0x0f)
	if first&0x70 != 0 || second&0x80 == 0 {
		return Event{}, false, false, ErrProtocol
	}
	control := opcode >= 0x8
	if control && (!fin || second&0x7f > 125) {
		return Event{}, false, false, ErrProtocol
	}
	switch opcode {
	case Continuation:
		if p.fragmentOpcode == 0 {
			return Event{}, false, false, ErrProtocol
		}
	case Text, Binary:
		if p.fragmentOpcode != 0 {
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
	current := int64(len(p.fragment))
	if !control && (payloadLen > uint64(p.maxMessageBytes) || current > p.maxMessageBytes-int64(payloadLen)) {
		return Event{}, false, false, ErrMessageTooBig
	}
	if len(p.buffer) < offset+4 {
		return Event{}, false, false, nil
	}
	mask := p.buffer[offset : offset+4]
	offset += 4
	if payloadLen > uint64(len(p.buffer)-offset) {
		return Event{}, false, false, nil
	}
	payload := make([]byte, int(payloadLen))
	for i := range payload {
		payload[i] = p.buffer[offset+i] ^ mask[i&3]
	}
	p.consume(offset + len(payload))

	if control {
		if opcode == Close {
			if len(payload) == 1 || (len(payload) >= 2 && !validCloseCode(binary.BigEndian.Uint16(payload[:2]))) {
				return Event{}, false, false, ErrProtocol
			}
			if len(payload) > 2 && !utf8.Valid(payload[2:]) {
				return Event{}, false, false, ErrInvalidPayload
			}
		}
		return Event{Opcode: opcode, Payload: payload}, true, true, nil
	}
	if opcode == Text || opcode == Binary {
		if fin {
			if opcode == Text && !utf8.Valid(payload) {
				return Event{}, false, false, ErrInvalidPayload
			}
			return Event{Opcode: opcode, Payload: payload}, true, true, nil
		}
		p.fragmentOpcode = opcode
		p.fragment = append(p.fragment[:0], payload...)
		return Event{}, false, true, nil
	}
	p.fragment = append(p.fragment, payload...)
	if !fin {
		return Event{}, false, true, nil
	}
	event := Event{Opcode: p.fragmentOpcode, Payload: p.fragment}
	if event.Opcode == Text && !utf8.Valid(event.Payload) {
		return Event{}, false, false, ErrInvalidPayload
	}
	p.fragmentOpcode = 0
	p.fragment = nil
	return event, true, true, nil
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		p.buffer = p.buffer[:0]
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
