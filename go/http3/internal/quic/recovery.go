package quic

import (
	"time"
)

// Loss detection and congestion control follow RFC 9002, with NewReno for
// the congestion controller.

const (
	packetThreshold  = 3
	timerGranularity = time.Millisecond
	initialRTT       = 333 * time.Millisecond
	// minCongestionWindow and initialCongestionWindow are in bytes.
	minCongestionWindow     = 2 * maxDatagram
	initialCongestionWindow = 10 * maxDatagram
	// maxProbes is how many probe packets a PTO sends.
	maxProbes = 2
)

// What a sent packet carried that loss or acknowledgement matters to.
const (
	sfCrypto = iota
	sfStream
	sfMaxData
	sfMaxStreamData
	sfMaxStreamsBidi
	sfMaxStreamsUni
	sfResetStream
	sfStopSending
	sfHandshakeDone
	sfRetireCID
)

type sentFrame struct {
	kind   uint8
	fin    bool
	stream *Stream
	off, n uint64
}

type sentPacket struct {
	pn           uint64
	time         time.Time
	size         int
	ackEliciting bool
	inFlight     bool
	frames       []sentFrame
}

type rttStats struct {
	latest, smoothed, rttvar, min time.Duration
	hasSample                     bool
}

func newRTTStats() rttStats {
	return rttStats{smoothed: initialRTT, rttvar: initialRTT / 2}
}

func (r *rttStats) update(latest, ackDelay time.Duration) {
	r.latest = latest
	if !r.hasSample {
		r.hasSample = true
		r.min = latest
		r.smoothed = latest
		r.rttvar = latest / 2
		return
	}
	r.min = min(r.min, latest)
	adjusted := latest
	if latest >= r.min+ackDelay {
		adjusted = latest - ackDelay
	}
	diff := r.smoothed - adjusted
	if diff < 0 {
		diff = -diff
	}
	r.rttvar = (3*r.rttvar + diff) / 4
	r.smoothed = (7*r.smoothed + adjusted) / 8
}

// pto is the probe timeout before backoff, without the peer's ack delay.
func (r *rttStats) pto() time.Duration {
	return r.smoothed + max(4*r.rttvar, timerGranularity)
}

// newReno is the congestion controller of RFC 9002 section 7.
type newReno struct {
	cwnd, ssthresh, bytesInFlight uint64
	recoveryStart                 time.Time
}

func newNewReno() newReno {
	return newReno{cwnd: initialCongestionWindow, ssthresh: ^uint64(0)}
}

func (cc *newReno) canSend() bool { return cc.bytesInFlight+maxDatagram <= cc.cwnd }

func (cc *newReno) onAcked(p *sentPacket) {
	cc.bytesInFlight -= uint64(p.size)
	if !p.time.After(cc.recoveryStart) {
		return
	}
	if cc.cwnd < cc.ssthresh {
		cc.cwnd += uint64(p.size)
	} else {
		cc.cwnd += maxDatagram * uint64(p.size) / cc.cwnd
	}
}

func (cc *newReno) onCongestion(sent, now time.Time) {
	if !sent.After(cc.recoveryStart) {
		return
	}
	cc.recoveryStart = now
	cc.ssthresh = max(cc.cwnd/2, minCongestionWindow)
	cc.cwnd = cc.ssthresh
}

// peerMaxAckDelay is how long the peer may hold back an acknowledgement.
func (c *Conn) peerMaxAckDelay() time.Duration {
	if c.peerParams != nil {
		return c.peerParams.maxAckDelay
	}
	return 25 * time.Millisecond
}

// onPacketSent records a packet as it leaves.
func (c *Conn) onPacketSent(space int, p *sentPacket) {
	s := &c.spaces[space]
	s.sent = append(s.sent, p)
	if p.ackEliciting {
		s.lastAckElicitingSent = p.time
		s.ackElicitingInFlight++
		if !c.sentSinceRecv {
			c.sentSinceRecv = true
			c.lastActivity = p.time
		}
	}
	if p.inFlight {
		c.cc.bytesInFlight += uint64(p.size)
	}
	if space == spaceHandshake && c.isClient && !c.spaces[spaceInitial].discarded {
		// A client stops using Initial packets once it sends a Handshake
		// one (RFC 9001 section 4.9.1).
		c.discardSpace(spaceInitial)
	}
}

// onAckReceived processes an ACK frame's ranges, largest first.
func (c *Conn) onAckReceived(space int, ranges []pnRange, ackDelay time.Duration, now time.Time) error {
	s := &c.spaces[space]
	largest := ranges[0].hi
	if largest >= s.nextPN {
		return transportErr(errProtocolViolation, "acknowledgement of an unsent packet")
	}
	if int64(largest) > s.largestAcked {
		s.largestAcked = int64(largest)
	}
	var acked []*sentPacket
	kept := s.sent[:0]
	ri := len(ranges) - 1
	for _, p := range s.sent {
		for ri >= 0 && ranges[ri].hi < p.pn {
			ri--
		}
		if ri >= 0 && ranges[ri].lo <= p.pn {
			acked = append(acked, p)
			continue
		}
		kept = append(kept, p)
	}
	for i := len(kept); i < len(s.sent); i++ {
		s.sent[i] = nil
	}
	s.sent = kept
	if len(acked) == 0 {
		return nil
	}
	newest := acked[len(acked)-1]
	eliciting := false
	for _, p := range acked {
		eliciting = eliciting || p.ackEliciting
	}
	if newest.pn == largest && eliciting {
		if space == spaceInitial {
			ackDelay = 0
		} else if c.handshakeConfirmed {
			ackDelay = min(ackDelay, c.peerMaxAckDelay())
		}
		c.rtt.update(now.Sub(newest.time), ackDelay)
	}
	c.detectLost(space, now)
	for _, p := range acked {
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		if p.inFlight {
			c.cc.onAcked(p)
		}
		for i := range p.frames {
			c.frameAcked(space, &p.frames[i])
		}
	}
	if space == spaceApp && c.isClient && !c.handshakeConfirmed {
		// An acknowledged 1-RTT packet confirms the handshake as well as
		// HANDSHAKE_DONE would (RFC 9001 section 4.1.2).
		c.confirmHandshake()
	}
	if c.peerCompletedAddressValidation() {
		c.ptoCount = 0
	}
	return nil
}

// detectLost declares packets lost that are too far behind the largest
// acknowledged one, by count or by time.
func (c *Conn) detectLost(space int, now time.Time) {
	s := &c.spaces[space]
	s.lossTime = time.Time{}
	if s.largestAcked < 0 {
		return
	}
	lossDelay := max(9*max(c.rtt.latest, c.rtt.smoothed)/8, timerGranularity)
	lostSendTime := now.Add(-lossDelay)
	var lost []*sentPacket
	kept := s.sent[:0]
	for _, p := range s.sent {
		if p.pn > uint64(s.largestAcked) {
			kept = append(kept, p)
			continue
		}
		if !p.time.After(lostSendTime) || uint64(s.largestAcked) >= p.pn+packetThreshold {
			lost = append(lost, p)
			continue
		}
		if t := p.time.Add(lossDelay); s.lossTime.IsZero() || t.Before(s.lossTime) {
			s.lossTime = t
		}
		kept = append(kept, p)
	}
	for i := len(kept); i < len(s.sent); i++ {
		s.sent[i] = nil
	}
	s.sent = kept
	if len(lost) == 0 {
		return
	}
	var latest time.Time
	for _, p := range lost {
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
			latest = p.time
		}
		for i := range p.frames {
			c.frameLost(space, &p.frames[i])
		}
	}
	if !latest.IsZero() {
		c.cc.onCongestion(latest, now)
	}
}

func (c *Conn) frameAcked(space int, f *sentFrame) {
	switch f.kind {
	case sfCrypto:
		c.spaces[space].cryptoSend.onAck(f.off, f.n)
	case sfStream:
		f.stream.onDataAcked(f.off, f.n, f.fin)
	case sfResetStream:
		f.stream.onResetAcked()
	}
}

func (c *Conn) frameLost(space int, f *sentFrame) {
	switch f.kind {
	case sfCrypto:
		c.spaces[space].cryptoSend.onLost(f.off, f.n)
	case sfStream:
		f.stream.onDataLost(f.off, f.n, f.fin)
	case sfMaxData:
		c.maxDataPending = true
	case sfMaxStreamData:
		f.stream.onMaxDataLost()
	case sfMaxStreamsBidi:
		c.maxStreamsBidiPending = true
	case sfMaxStreamsUni:
		c.maxStreamsUniPending = true
	case sfResetStream:
		f.stream.onResetLost()
	case sfStopSending:
		f.stream.onStopSendingLost()
	case sfHandshakeDone:
		c.handshakeDonePending = true
	case sfRetireCID:
		c.retireCIDs = append(c.retireCIDs, f.off)
	}
}

// peerCompletedAddressValidation reports whether the peer can no longer be
// stuck waiting for this side to prove its address, which only a client
// has to (RFC 9002 section 6.2.2.1).
func (c *Conn) peerCompletedAddressValidation() bool {
	return !c.isClient || c.handshakeConfirmed || c.spaces[spaceHandshake].largestAcked >= 0
}

func (c *Conn) atAmplificationLimit() bool {
	return !c.isClient && !c.addressValidated && c.bytesSent+maxDatagram > 3*c.bytesRecv
}

// lossTimeSpace is the space with the earliest time-threshold loss timer.
func (c *Conn) lossTimeSpace() (time.Time, int) {
	var t time.Time
	space := 0
	for i := range c.spaces {
		if lt := c.spaces[i].lossTime; !lt.IsZero() && (t.IsZero() || lt.Before(t)) {
			t, space = lt, i
		}
	}
	return t, space
}

func (c *Conn) ackElicitingInFlight() bool {
	for i := range c.spaces {
		if c.spaces[i].ackElicitingInFlight > 0 {
			return true
		}
	}
	return false
}

// ptoTimeSpace is when the probe timeout fires and for which space.
func (c *Conn) ptoTimeSpace(now time.Time) (time.Time, int) {
	duration := c.rtt.pto() << c.ptoCount
	if !c.ackElicitingInFlight() {
		if c.spaces[spaceHandshake].tx != nil {
			return now.Add(duration), spaceHandshake
		}
		return now.Add(duration), spaceInitial
	}
	var t time.Time
	space := spaceInitial
	for i := range c.spaces {
		s := &c.spaces[i]
		if s.ackElicitingInFlight == 0 {
			continue
		}
		d := duration
		if i == spaceApp {
			// 1-RTT packets wait for the handshake to be confirmed, unless
			// they are all that is left in flight: the handshake's own
			// packets may have been declared lost and be waiting behind
			// them for room in the congestion window.
			if !c.handshakeConfirmed && !t.IsZero() {
				break
			}
			if c.handshakeConfirmed {
				d += c.peerMaxAckDelay() << c.ptoCount
			}
		}
		if pt := s.lastAckElicitingSent.Add(d); t.IsZero() || pt.Before(t) {
			t, space = pt, i
		}
	}
	return t, space
}

// setLossDetectionTimer works out when loss detection next has something to
// do.
func (c *Conn) setLossDetectionTimer(now time.Time) {
	if t, _ := c.lossTimeSpace(); !t.IsZero() {
		c.lossDeadline = t
		return
	}
	if c.atAmplificationLimit() || !c.ackElicitingInFlight() && c.peerCompletedAddressValidation() {
		c.lossDeadline = time.Time{}
		return
	}
	c.lossDeadline, _ = c.ptoTimeSpace(now)
}

func (c *Conn) onLossDetectionTimeout(now time.Time) {
	if t, space := c.lossTimeSpace(); !t.IsZero() {
		c.detectLost(space, now)
		return
	}
	if !c.ackElicitingInFlight() {
		// The client's handshake packets may all have arrived while the
		// server, not yet sure of its address, waits: prompt it.
		if c.spaces[spaceHandshake].tx != nil {
			c.spaces[spaceHandshake].probes = 1
		} else {
			c.spaces[spaceInitial].probes = 1
		}
	} else {
		_, space := c.ptoTimeSpace(now)
		c.probe(space)
	}
	c.ptoCount++
}

// probe gets a space ready to send probe packets, carrying again what its
// oldest packets in flight carried, since that is what the peer most likely
// lacks.
func (c *Conn) probe(space int) {
	s := &c.spaces[space]
	s.probes = maxProbes
	// Handshake data waiting to be sent again goes out with the probes,
	// whatever the congestion window says, or the handshake could wait
	// forever behind packets that cannot be acknowledged without it.
	for i := spaceInitial; i < spaceApp; i++ {
		hs := &c.spaces[i]
		if i != space && hs.tx != nil && !hs.discarded && (hs.cryptoSend.hasLost() || hs.cryptoSend.next < hs.cryptoSend.end()) {
			hs.probes = max(hs.probes, 1)
		}
	}
	n := 0
	for _, p := range s.sent {
		if !p.ackEliciting {
			continue
		}
		for i := range p.frames {
			c.frameLost(space, &p.frames[i])
		}
		if n++; space == spaceApp && n == maxProbes {
			break
		}
	}
}

// discardSpace drops a space's keys and what it had in flight, once the
// handshake has moved past it.
func (c *Conn) discardSpace(space int) {
	s := &c.spaces[space]
	if s.discarded {
		return
	}
	for _, p := range s.sent {
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
		}
	}
	*s = pnSpace{discarded: true, largestAcked: s.largestAcked}
	c.ptoCount = 0
}
