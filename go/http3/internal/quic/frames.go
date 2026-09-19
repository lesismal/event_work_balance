package quic

import (
	"bytes"
	"time"
)

// Frame types (RFC 9000 section 19).
const (
	framePadding            = 0x00
	framePing               = 0x01
	frameAck                = 0x02
	frameAckECN             = 0x03
	frameResetStream        = 0x04
	frameStopSending        = 0x05
	frameCrypto             = 0x06
	frameNewToken           = 0x07
	frameStream             = 0x08 // to 0x0f
	frameMaxData            = 0x10
	frameMaxStreamData      = 0x11
	frameMaxStreamsBidi     = 0x12
	frameMaxStreamsUni      = 0x13
	frameDataBlocked        = 0x14
	frameStreamDataBlocked  = 0x15
	frameStreamsBlockedBidi = 0x16
	frameStreamsBlockedUni  = 0x17
	frameNewConnectionID    = 0x18
	frameRetireConnectionID = 0x19
	framePathChallenge      = 0x1a
	framePathResponse       = 0x1b
	frameConnectionClose    = 0x1c
	frameConnectionCloseApp = 0x1d
	frameHandshakeDone      = 0x1e
)

func (p *sentPacket) add(f sentFrame) {
	p.frames = append(p.frames, f)
	p.ackEliciting = true
}

func appendStopSending(b []byte, id, code uint64) []byte {
	b = append(b, frameStopSending)
	b = AppendVarint(b, id)
	return AppendVarint(b, code)
}

func appendMaxStreamData(b []byte, id, max uint64) []byte {
	b = append(b, frameMaxStreamData)
	b = AppendVarint(b, id)
	return AppendVarint(b, max)
}

func appendResetStream(b []byte, id, code, finalSize uint64) []byte {
	b = append(b, frameResetStream)
	b = AppendVarint(b, id)
	b = AppendVarint(b, code)
	return AppendVarint(b, finalSize)
}

// appendConnectionClose appends a CONNECTION_CLOSE frame for err. An
// application close outside 1-RTT packets would tell an unauthenticated
// observer what the application said, so it goes as a transport close with
// APPLICATION_ERROR instead (RFC 9000 section 10.2.3).
func appendConnectionClose(b []byte, err error, oneRTT bool) []byte {
	switch e := err.(type) {
	case *ApplicationError:
		if oneRTT {
			b = append(b, frameConnectionCloseApp)
			b = AppendVarint(b, e.Code)
			b = AppendVarint(b, uint64(len(e.Reason)))
			return append(b, e.Reason...)
		}
		b = append(b, frameConnectionClose)
		b = AppendVarint(b, errApplication)
		b = AppendVarint(b, 0)
		return AppendVarint(b, 0)
	case *TransportError:
		b = append(b, frameConnectionClose)
		b = AppendVarint(b, e.Code)
		b = AppendVarint(b, 0)
		reason := e.Reason
		if len(reason) > 256 {
			reason = reason[:256]
		}
		b = AppendVarint(b, uint64(len(reason)))
		return append(b, reason...)
	}
	b = append(b, frameConnectionClose)
	b = AppendVarint(b, errInternal)
	b = AppendVarint(b, 0)
	return AppendVarint(b, 0)
}

// appendAck appends an ACK frame for what the space has received.
func (c *Conn) appendAck(b []byte, s *pnSpace, now time.Time) []byte {
	r := s.recv
	largest := r[len(r)-1]
	var delay uint64
	if s == &c.spaces[spaceApp] {
		delay = uint64(now.Sub(s.largestRecvTime).Microseconds()) >> c.local.ackDelayExponent
	}
	b = append(b, frameAck)
	b = AppendVarint(b, largest.hi)
	b = AppendVarint(b, delay)
	b = AppendVarint(b, uint64(len(r)-1))
	b = AppendVarint(b, largest.hi-largest.lo)
	prev := largest.lo
	for i := len(r) - 2; i >= 0; i-- {
		b = AppendVarint(b, prev-r[i].hi-2)
		b = AppendVarint(b, r[i].hi-r[i].lo)
		prev = r[i].lo
	}
	return b
}

// handleFrames processes a packet's frames, reporting whether any of them
// asks for an acknowledgement.
func (c *Conn) handleFrames(space int, payload []byte, now time.Time) (bool, error) {
	if len(payload) == 0 {
		return false, transportErr(errProtocolViolation, "packet without frames")
	}
	r := reader{b: payload}
	ackEliciting := false
	for len(r.b) > 0 && !c.closed {
		typ := r.varint()
		if r.bad {
			return false, transportErr(errFrameEncoding, "truncated frame type")
		}
		if typ != framePadding && typ != frameAck && typ != frameAckECN &&
			typ != frameConnectionClose && typ != frameConnectionCloseApp {
			ackEliciting = true
		}
		// Initial and Handshake packets carry only the handshake itself.
		if space != spaceApp {
			switch typ {
			case framePadding, framePing, frameAck, frameAckECN, frameCrypto, frameConnectionClose:
			default:
				return false, &TransportError{Code: errProtocolViolation, Reason: "frame not allowed in handshake packets"}
			}
		}
		var err error
		switch {
		case typ == framePadding:
			for len(r.b) > 0 && r.b[0] == 0 {
				r.b = r.b[1:]
			}
		case typ == framePing:
		case typ == frameAck || typ == frameAckECN:
			err = c.handleAckFrame(space, &r, typ == frameAckECN, now)
		case typ == frameResetStream:
			id, code, size := r.varint(), r.varint(), r.varint()
			if r.bad {
				break
			}
			s, e := c.peerStream(id, true)
			if e != nil || s == nil {
				err = e
				break
			}
			err = s.onResetStream(code, size)
		case typ == frameStopSending:
			id, code := r.varint(), r.varint()
			if r.bad {
				break
			}
			s, e := c.peerStream(id, false)
			if e != nil || s == nil {
				err = e
				break
			}
			s.onStopSending(code)
		case typ == frameCrypto:
			off := r.varint()
			data := r.bytes(r.varint())
			if r.bad {
				break
			}
			err = c.handleCrypto(space, off, data)
		case typ == frameNewToken:
			token := r.bytes(r.varint())
			if !c.isClient {
				err = transportErr(errProtocolViolation, "NEW_TOKEN from a client")
			} else if !r.bad && len(token) == 0 {
				err = transportErr(errFrameEncoding, "empty NEW_TOKEN")
			}
		case typ >= frameStream && typ <= frameStream|7:
			id := r.varint()
			var off uint64
			if typ&0x04 != 0 {
				off = r.varint()
			}
			var data []byte
			if typ&0x02 != 0 {
				data = r.bytes(r.varint())
			} else {
				data = r.b
				r.b = nil
			}
			if r.bad {
				break
			}
			s, e := c.peerStream(id, true)
			if e != nil || s == nil {
				err = e
				break
			}
			err = s.onStreamFrame(off, data, typ&0x01 != 0)
		case typ == frameMaxData:
			if max := r.varint(); max > c.peerMaxData {
				c.peerMaxData = max
				for _, s := range c.streams {
					if s.sendable() {
						c.queueStream(s)
					}
				}
			}
		case typ == frameMaxStreamData:
			id, max := r.varint(), r.varint()
			if r.bad {
				break
			}
			s, e := c.peerStream(id, false)
			if e != nil || s == nil {
				err = e
				break
			}
			if max > s.sendMax {
				s.sendMax = max
				c.queueStream(s)
			}
		case typ == frameMaxStreamsBidi || typ == frameMaxStreamsUni:
			max := r.varint()
			if max > 1<<60 {
				err = transportErr(errFrameEncoding, "MAX_STREAMS too large")
				break
			}
			limit := &c.peerMaxBidi
			if typ == frameMaxStreamsUni {
				limit = &c.peerMaxUni
			}
			if max > *limit {
				*limit = max
				c.events = append(c.events, func() { c.handler.OnStreamsAvailable(c) })
			}
		case typ == frameDataBlocked:
			r.varint()
		case typ == frameStreamDataBlocked:
			r.varint()
			r.varint()
		case typ == frameStreamsBlockedBidi || typ == frameStreamsBlockedUni:
			r.varint()
		case typ == frameNewConnectionID:
			err = c.handleNewConnectionID(&r)
		case typ == frameRetireConnectionID:
			// Only the one connection ID was ever issued, and the peer
			// keeps using it.
			r.varint()
		case typ == framePathChallenge:
			if data := r.bytes(8); !r.bad {
				c.queuePathResponse([8]byte(data))
			}
		case typ == framePathResponse:
			r.bytes(8)
		case typ == frameConnectionClose || typ == frameConnectionCloseApp:
			code := r.varint()
			if typ == frameConnectionClose {
				r.varint()
			}
			reason := r.bytes(r.varint())
			if r.bad {
				break
			}
			if typ == frameConnectionClose {
				c.terminateLocked(&TransportError{Code: code, Reason: string(reason), Remote: true})
			} else {
				c.terminateLocked(&ApplicationError{Code: code, Reason: string(reason), Remote: true})
			}
		case typ == frameHandshakeDone:
			if !c.isClient {
				err = transportErr(errProtocolViolation, "HANDSHAKE_DONE from a client")
				break
			}
			c.confirmHandshake()
		default:
			err = transportErr(errFrameEncoding, "unknown frame type")
		}
		if err != nil {
			return false, err
		}
		if r.bad {
			return false, transportErr(errFrameEncoding, "malformed frame")
		}
	}
	return ackEliciting, nil
}

func (c *Conn) handleAckFrame(space int, r *reader, ecn bool, now time.Time) error {
	largest := r.varint()
	delay := r.varint()
	count := r.varint()
	first := r.varint()
	if r.bad || first > largest || count > 1<<16 {
		return transportErr(errFrameEncoding, "malformed ACK")
	}
	ranges := make([]pnRange, 0, min(count+1, 64))
	ranges = append(ranges, pnRange{largest - first, largest})
	smallest := largest - first
	for i := uint64(0); i < count; i++ {
		gap, length := r.varint(), r.varint()
		if r.bad || smallest < gap+2 || smallest-gap-2 < length {
			return transportErr(errFrameEncoding, "malformed ACK")
		}
		hi := smallest - gap - 2
		smallest = hi - length
		ranges = append(ranges, pnRange{smallest, hi})
	}
	if ecn {
		r.varint()
		r.varint()
		r.varint()
	}
	if r.bad {
		return transportErr(errFrameEncoding, "malformed ACK")
	}
	exp := uint64(3)
	if c.peerParams != nil {
		exp = c.peerParams.ackDelayExponent
	}
	ackDelay := time.Duration(delay<<exp) * time.Microsecond
	return c.onAckReceived(space, ranges, ackDelay, now)
}

// maxPathResponses bounds the PATH_RESPONSE frames waiting to be sent.
const maxPathResponses = 4

// queuePathResponse queues the answer to a PATH_CHALLENGE. Only the most
// recent challenges are answered: an endpoint need not answer every one
// (RFC 9000 section 8.2.2), and a peer sending them faster than they go out
// must not be able to make the queue grow without bound.
func (c *Conn) queuePathResponse(data [8]byte) {
	if len(c.pathResponses) >= maxPathResponses {
		n := copy(c.pathResponses, c.pathResponses[len(c.pathResponses)-maxPathResponses+1:])
		c.pathResponses = c.pathResponses[:n]
	}
	c.pathResponses = append(c.pathResponses, data)
}

// handleNewConnectionID keeps connection IDs the peer offers, and moves to a
// new one when it retires the one in use.
func (c *Conn) handleNewConnectionID(r *reader) error {
	seq, retirePrior := r.varint(), r.varint()
	n := r.byte()
	cid := r.bytes(uint64(n))
	token := r.bytes(statelessResetTokenLen)
	if r.bad {
		return nil
	}
	if n == 0 || n > maxCIDLen || retirePrior > seq {
		return transportErr(errFrameEncoding, "malformed NEW_CONNECTION_ID")
	}
	if len(c.dcid) == 0 {
		return transportErr(errProtocolViolation, "NEW_CONNECTION_ID with a zero-length connection ID")
	}
	if seq < c.retiredPrior {
		c.retireCIDs = append(c.retireCIDs, seq)
		return nil
	}
	if old, ok := c.peerCIDs[seq]; ok {
		if !bytes.Equal(old.cid, cid) {
			return transportErr(errProtocolViolation, "connection ID sequence number reused")
		}
		return nil
	}
	c.peerCIDs[seq] = peerCID{cid: bytes.Clone(cid), token: bytes.Clone(token)}
	if retirePrior > c.retiredPrior {
		c.retiredPrior = retirePrior
		for s := range c.peerCIDs {
			if s < retirePrior {
				delete(c.peerCIDs, s)
				c.retireCIDs = append(c.retireCIDs, s)
			}
		}
		if c.dcidSeq < retirePrior {
			// Move to the lowest connection ID still in force.
			next := ^uint64(0)
			for s := range c.peerCIDs {
				next = min(next, s)
			}
			if next != ^uint64(0) {
				c.dcidSeq = next
				c.dcid = c.peerCIDs[next].cid
				c.peerResetToken = c.peerCIDs[next].token
			}
		}
	}
	if uint64(len(c.peerCIDs)) > c.local.activeCIDLimit {
		return transportErr(errConnectionIDLimit, "too many connection IDs")
	}
	return nil
}
