package quic

import (
	"encoding/binary"
	"time"
)

// packetPlan is a packet whose frames are chosen but which is not yet
// sealed, so that the last packet of a datagram can still be padded.
type packetPlan struct {
	space   int
	pn      uint64
	pnLen   int
	payload []byte
	sp      *sentPacket
	// padded says the payload has PADDING, which makes the packet count as
	// in flight even without ack-eliciting frames.
	padded bool
}

// headerLen is the length of a packet header in a space.
func (c *Conn) headerLen(space, pnLen int) int {
	if space == spaceApp {
		return 1 + len(c.dcid) + pnLen
	}
	n := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 2 + pnLen
	if space == spaceInitial {
		n += VarintLen(uint64(len(c.token))) + len(c.token)
	}
	return n
}

func (c *Conn) packetSize(p *packetPlan) int {
	return c.headerLen(p.space, p.pnLen) + len(p.payload) + aeadOverhead
}

// sealPlan appends the sealed packet to out.
func (c *Conn) sealPlan(out []byte, p *packetPlan) []byte {
	start := len(out)
	s := &c.spaces[p.space]
	if p.space == spaceApp {
		first := byte(0x40) | byte(p.pnLen-1)
		if c.keyPhase {
			first |= 0x04
		}
		out = append(out, first)
		out = append(out, c.dcid...)
	} else {
		typ := byte(packetInitial)
		if p.space == spaceHandshake {
			typ = packetHandshake
		}
		out = append(out, 0xc0|typ<<4|byte(p.pnLen-1))
		out = binary.BigEndian.AppendUint32(out, Version1)
		out = append(out, byte(len(c.dcid)))
		out = append(out, c.dcid...)
		out = append(out, byte(len(c.scid)))
		out = append(out, c.scid...)
		if p.space == spaceInitial {
			out = AppendVarint(out, uint64(len(c.token)))
			out = append(out, c.token...)
		}
		out = appendVarint2(out, uint64(p.pnLen+len(p.payload)+aeadOverhead))
	}
	pnOffset := len(out) - start
	for i := p.pnLen - 1; i >= 0; i-- {
		out = append(out, byte(p.pn>>(8*i)))
	}
	out = append(out, p.payload...)
	if cap(out)-len(out) < aeadOverhead {
		grown := make([]byte, len(out), len(out)+aeadOverhead+maxDatagram)
		copy(grown, out)
		out = grown
	}
	sealed := s.tx.seal(out[start:], pnOffset, p.pnLen, p.pn)
	return out[:start+len(sealed)]
}

// flushLocked sends whatever there is to send and the limits allow, then
// sets the timer for what comes next.
func (c *Conn) flushLocked(now time.Time) {
	for !c.closed {
		d := c.buildDatagram(now)
		if d == nil {
			break
		}
		c.bytesSent += uint64(len(d))
		_ = c.pc.Send(d)
	}
	if c.closed {
		return
	}
	c.setLossDetectionTimer(now)
	c.armTimerLocked(now)
}

// buildDatagram fills one datagram with a packet from each space that has
// something to send, or returns nil when none has.
func (c *Conn) buildDatagram(now time.Time) []byte {
	limit := maxDatagram
	if !c.isClient && !c.addressValidated {
		// Until the client's address is proven, a server sends at most
		// three times what it received (RFC 9000 section 8.1).
		if c.bytesSent+maxDatagram > 3*c.bytesRecv {
			return nil
		}
	}
	congestionOK := c.cc.canSend()
	var plans [numSpaces]packetPlan
	n, size := 0, 0
	for space := range c.spaces {
		s := &c.spaces[space]
		if s.tx == nil || s.discarded {
			continue
		}
		plan, ok := c.buildPacket(space, limit-size, congestionOK || s.probes > 0, now)
		if !ok {
			continue
		}
		plans[n] = plan
		n++
		size += c.packetSize(&plan)
	}
	if n == 0 {
		return nil
	}
	pad := false
	for i := 0; i < n; i++ {
		if plans[i].space == spaceInitial && (c.isClient || plans[i].sp.ackEliciting) {
			// Datagrams with Initial packets are padded out so that the
			// server's replies cannot amplify much (RFC 9000 section 14.1).
			pad = true
		}
	}
	if pad && size < minInitialDatagram {
		last := &plans[n-1]
		last.payload = append(last.payload, make([]byte, minInitialDatagram-size)...)
		last.padded = true
		size = minInitialDatagram
	}
	out := make([]byte, 0, size)
	for i := 0; i < n; i++ {
		out = c.sealPlan(out, &plans[i])
	}
	for i := 0; i < n; i++ {
		p := &plans[i]
		p.sp.time = now
		p.sp.size = c.packetSize(p)
		p.sp.inFlight = p.sp.ackEliciting || p.padded
		c.onPacketSent(p.space, p.sp)
	}
	return out
}

// buildPacket chooses the frames for a packet in space of at most room
// bytes. mayElicit says whether congestion control allows ack-eliciting
// frames; without them, only an acknowledgement that is due goes out.
func (c *Conn) buildPacket(space, room int, mayElicit bool, now time.Time) (packetPlan, bool) {
	s := &c.spaces[space]
	pn := s.nextPN
	pl := pnLen(pn, s.largestAcked)
	max := room - c.headerLen(space, pl) - aeadOverhead
	if max < 16 {
		return packetPlan{}, false
	}
	sp := &sentPacket{pn: pn}
	payload := make([]byte, 0, max)
	ackDue := s.ackPending && (s.ackNow || !s.ackDeadline.IsZero() && !now.Before(s.ackDeadline))
	withAck := false
	if s.ackPending && len(s.recv) > 0 {
		if ack := c.appendAck(payload, s, now); len(ack) <= max/2 {
			payload = ack
			withAck = true
		}
	}
	if mayElicit {
		payload = c.appendFrames(payload, space, max, sp)
	}
	if s.probes > 0 && !sp.ackEliciting && len(payload) < max {
		payload = append(payload, framePing)
		sp.ackEliciting = true
	}
	if !sp.ackEliciting && !(withAck && ackDue) {
		return packetPlan{}, false
	}
	if withAck {
		s.ackPending = false
		s.ackNow = false
		s.unackedEliciting = 0
		s.ackDeadline = time.Time{}
	}
	if sp.ackEliciting && s.probes > 0 {
		s.probes--
	}
	// Header protection samples four bytes past the packet number's start.
	padded := false
	if short := 4 - pl - len(payload); short > 0 {
		payload = append(payload, make([]byte, short)...)
		padded = true
	}
	s.nextPN++
	return packetPlan{space: space, pn: pn, pnLen: pl, payload: payload, sp: sp, padded: padded}, true
}

// appendFrames adds a space's ack-eliciting frames, up to max bytes of
// payload.
func (c *Conn) appendFrames(b []byte, space, max int, sp *sentPacket) []byte {
	if space == spaceApp {
		b = c.appendControlFrames(b, max, sp)
	}
	b = c.appendCrypto(b, space, max, sp)
	if space == spaceApp {
		b = c.appendStreams(b, max, sp)
	}
	return b
}

func (c *Conn) appendControlFrames(b []byte, max int, sp *sentPacket) []byte {
	fits := func(n int) bool { return len(b)+n <= max }
	if c.handshakeDonePending && fits(1) {
		b = append(b, frameHandshakeDone)
		c.handshakeDonePending = false
		sp.add(sentFrame{kind: sfHandshakeDone})
	}
	if c.maxDataPending && fits(1+VarintLen(c.recvMaxData)) {
		b = append(b, frameMaxData)
		b = AppendVarint(b, c.recvMaxData)
		c.maxDataPending = false
		sp.add(sentFrame{kind: sfMaxData})
	}
	if c.maxStreamsBidiPending && fits(1+VarintLen(c.maxBidi)) {
		b = append(b, frameMaxStreamsBidi)
		b = AppendVarint(b, c.maxBidi)
		c.maxStreamsBidiPending = false
		sp.add(sentFrame{kind: sfMaxStreamsBidi})
	}
	if c.maxStreamsUniPending && fits(1+VarintLen(c.maxUni)) {
		b = append(b, frameMaxStreamsUni)
		b = AppendVarint(b, c.maxUni)
		c.maxStreamsUniPending = false
		sp.add(sentFrame{kind: sfMaxStreamsUni})
	}
	for len(c.pathResponses) > 0 && fits(9) {
		b = append(b, framePathResponse)
		b = append(b, c.pathResponses[0][:]...)
		c.pathResponses = c.pathResponses[1:]
		sp.ackEliciting = true
	}
	for len(c.retireCIDs) > 0 && fits(1+VarintLen(c.retireCIDs[0])) {
		seq := c.retireCIDs[0]
		c.retireCIDs = c.retireCIDs[1:]
		b = append(b, frameRetireConnectionID)
		b = AppendVarint(b, seq)
		sp.add(sentFrame{kind: sfRetireCID, off: seq})
	}
	if c.pingPending && fits(1) {
		b = append(b, framePing)
		c.pingPending = false
		sp.ackEliciting = true
	}
	return b
}

func (c *Conn) appendCrypto(b []byte, space, max int, sp *sentPacket) []byte {
	cs := &c.spaces[space].cryptoSend
	for {
		lost := cs.hasLost()
		if !lost && cs.next >= cs.end() {
			return b
		}
		off := cs.next
		if lost {
			off = max64(cs.lost[0].off, cs.base)
		}
		avail := max - len(b) - 1 - VarintLen(off) - 2
		if avail <= 0 {
			return b
		}
		var data []byte
		if lost {
			off, data = cs.popLost(uint64(avail))
		} else {
			off, data = cs.popNew(uint64(avail))
		}
		b = append(b, frameCrypto)
		b = AppendVarint(b, off)
		b = AppendVarint(b, uint64(len(data)))
		b = append(b, data...)
		sp.add(sentFrame{kind: sfCrypto, off: off, n: uint64(len(data))})
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// appendStreams adds stream frames, taking the queued streams in turn so
// that one busy stream does not starve the rest.
func (c *Conn) appendStreams(b []byte, max int, sp *sentPacket) []byte {
	i := 0
	for i < len(c.sendQueue) && max-len(b) > 16 {
		s := c.sendQueue[i]
		if !s.done {
			b = s.appendFrames(b, max, sp)
		}
		if s.done || !s.sendable() {
			s.queued = false
			c.sendQueue = append(c.sendQueue[:i], c.sendQueue[i+1:]...)
			continue
		}
		i++
	}
	if len(c.sendQueue) > 1 && i > 0 {
		// The stream that filled the packet goes to the back of the line.
		first := c.sendQueue[0]
		copy(c.sendQueue, c.sendQueue[1:])
		c.sendQueue[len(c.sendQueue)-1] = first
	}
	return b
}
