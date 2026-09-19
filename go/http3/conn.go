//go:build linux || darwin || windows

package http3

import (
	"errors"

	"github.com/lesismal/fib/go/http3/internal/qpack"
	"github.com/lesismal/fib/go/http3/internal/quic"
)

// What both sides of an HTTP/3 connection do alike: the control stream
// each opens with its SETTINGS, and the unidirectional streams the peer
// opens.

// peerStreams tracks the unidirectional streams the peer has opened, of
// which there may be one of each critical kind.
type peerStreams struct {
	control, encoder, decoder bool
	// settings is whether the peer's SETTINGS have arrived.
	settings bool
	// onGoAway receives the peer's GOAWAY.
	onGoAway func(id uint64) error
}

// openControl opens this side's control stream and sends SETTINGS on it.
func openControl(qc *quic.Conn, maxFieldSection int) (*quic.Stream, error) {
	s, err := qc.OpenUniStream()
	if err != nil {
		return nil, err
	}
	b := quic.AppendVarint(nil, streamControl)
	// The dynamic table capacity and blocked streams are left at their
	// default of zero, which is what keeps the QPACK streams silent.
	b = appendSettings(b, [2]uint64{settingMaxFieldSectionSize, uint64(maxFieldSection)})
	return s, s.Write(b, false)
}

// uniStream is a unidirectional stream the peer opened.
type uniStream struct {
	peer   *peerStreams
	s      *quic.Stream
	typ    int64
	head   []byte
	parser frameParser
	// fail ends the connection; it is how errors on the stream are
	// reported.
	fail func(error)
}

func newUniStream(peer *peerStreams, s *quic.Stream, fail func(error)) *uniStream {
	return &uniStream{peer: peer, s: s, typ: -1, fail: fail, parser: frameParser{maxFrame: 16 << 10}}
}

func (u *uniStream) feed(data []byte, fin bool) {
	if err := u.handle(data, fin); err != nil {
		u.fail(err)
	}
}

func (u *uniStream) handle(data []byte, fin bool) error {
	if u.typ < 0 {
		u.head = append(u.head, data...)
		typ, n := quic.ReadVarint(u.head)
		if n == 0 {
			return nil
		}
		u.typ = int64(typ)
		data = u.head[n:]
		u.head = nil
		p := u.peer
		switch typ {
		case streamControl:
			if p.control {
				return connErr(ErrCodeStreamCreationError, "second control stream")
			}
			p.control = true
		case streamQPACKEncoder:
			if p.encoder {
				return connErr(ErrCodeStreamCreationError, "second QPACK encoder stream")
			}
			p.encoder = true
		case streamQPACKDecoder:
			if p.decoder {
				return connErr(ErrCodeStreamCreationError, "second QPACK decoder stream")
			}
			p.decoder = true
		case streamPush:
			// Push is never enabled: no MAX_PUSH_ID is sent, and a client
			// may not push at all.
			return connErr(ErrCodeIDError, "push stream without MAX_PUSH_ID")
		default:
			// Unknown stream types are for extensions; nothing here reads
			// them (RFC 9114 section 6.2).
			u.s.StopSending(uint64(ErrCodeStreamCreationError))
			return nil
		}
	}
	switch u.typ {
	case streamControl:
		if err := u.parser.feed(data, func([]byte) error {
			return connErr(ErrCodeFrameUnexpected, "DATA on the control stream")
		}, u.controlFrame); err != nil {
			return err
		}
	case streamQPACKEncoder, streamQPACKDecoder:
		// With no dynamic table, there is nothing these may say that needs
		// acting on.
	default:
		return nil
	}
	if fin {
		return connErr(ErrCodeClosedCriticalStream, "critical stream closed")
	}
	return nil
}

func (u *uniStream) controlFrame(typ uint64, payload []byte) error {
	p := u.peer
	if !p.settings {
		if typ != frameSettings {
			return connErr(ErrCodeMissingSettings, "control stream does not start with SETTINGS")
		}
		p.settings = true
		_, err := parseSettings(payload)
		return err
	}
	switch typ {
	case frameSettings:
		return connErr(ErrCodeFrameUnexpected, "second SETTINGS")
	case frameGoAway:
		id, n := quic.ReadVarint(payload)
		if n == 0 || n != len(payload) {
			return connErr(ErrCodeFrameError, "malformed GOAWAY")
		}
		if p.onGoAway != nil {
			return p.onGoAway(id)
		}
	case frameMaxPushID, frameCancelPush:
		if _, n := quic.ReadVarint(payload); n == 0 || n != len(payload) {
			return connErr(ErrCodeFrameError, "malformed frame")
		}
	case frameData, frameHeaders, framePushPromise:
		return connErr(ErrCodeFrameUnexpected, "request frame on the control stream")
	default:
		if reservedFrame(typ) {
			return connErr(ErrCodeFrameUnexpected, "HTTP/2 frame type")
		}
	}
	return nil
}

// closeWith ends a QUIC connection with the code an error calls for.
func closeWith(qc *quic.Conn, err error) {
	var ce *connError
	switch {
	case errors.As(err, &ce):
		qc.Close(uint64(ce.code), ce.reason)
	case errors.Is(err, qpack.ErrDecompression):
		qc.Close(uint64(ErrCodeQPACKDecompressionFailed), err.Error())
	default:
		qc.Close(uint64(ErrCodeInternalError), err.Error())
	}
}

// abortStream ends both directions of a request stream with code.
func abortStream(s *quic.Stream, code ErrorCode) {
	s.StopSending(uint64(code))
	s.Reset(uint64(code))
}
