package quic

import "sort"

// span is the byte range [off, end) of a stream.
type span struct{ off, end uint64 }

// addSpan merges s into spans, which are sorted and disjoint.
func addSpan(spans []span, s span) []span {
	if s.end <= s.off {
		return spans
	}
	i := sort.Search(len(spans), func(i int) bool { return spans[i].end >= s.off })
	j := i
	for j < len(spans) && spans[j].off <= s.end {
		s.off = min(s.off, spans[j].off)
		s.end = max(s.end, spans[j].end)
		j++
	}
	if i == j {
		spans = append(spans, span{})
		copy(spans[i+1:], spans[i:])
		spans[i] = s
		return spans
	}
	spans[i] = s
	return append(spans[:i+1], spans[j:]...)
}

// sendBuffer holds what a stream, or a packet number space's CRYPTO stream,
// has been given to send, until the peer acknowledges it.
type sendBuffer struct {
	// buf holds the bytes from offset base on; everything before base has
	// been acknowledged.
	base uint64
	buf  []byte
	// next is the offset of the first byte never sent.
	next uint64
	// lost are sent ranges to send again, and acked the acknowledged ones
	// beyond base.
	lost  []span
	acked []span
}

func (b *sendBuffer) end() uint64 { return b.base + uint64(len(b.buf)) }

func (b *sendBuffer) write(p []byte) { b.buf = append(b.buf, p...) }

// hasLost reports whether some sent data needs sending again.
func (b *sendBuffer) hasLost() bool {
	for len(b.lost) > 0 && b.lost[0].end <= b.base {
		b.lost = b.lost[1:]
	}
	return len(b.lost) > 0
}

// popLost takes up to n bytes of the first lost range.
func (b *sendBuffer) popLost(n uint64) (uint64, []byte) {
	if !b.hasLost() || n == 0 {
		return 0, nil
	}
	s := &b.lost[0]
	s.off = max(s.off, b.base)
	off := s.off
	end := min(s.end, off+n)
	data := b.buf[off-b.base : end-b.base]
	s.off = end
	if s.off >= s.end {
		b.lost = b.lost[1:]
	}
	return off, data
}

// popNew takes up to n bytes never sent before.
func (b *sendBuffer) popNew(n uint64) (uint64, []byte) {
	off := b.next
	end := min(b.end(), off+n)
	b.next = end
	return off, b.buf[off-b.base : end-b.base]
}

// onAck records that [off, off+n) arrived, and lets go of what now needs no
// keeping.
func (b *sendBuffer) onAck(off, n uint64) {
	b.acked = addSpan(b.acked, span{off, off + n})
	if len(b.acked) == 0 || b.acked[0].off > b.base {
		return
	}
	newBase := b.acked[0].end
	b.acked = b.acked[1:]
	if newBase <= b.base {
		return
	}
	b.buf = b.buf[newBase-b.base:]
	b.base = newBase
	if len(b.buf) == 0 {
		// Let the array go rather than grow it from its tail forever.
		b.buf = nil
	}
}

// onLost queues [off, off+n) to be sent again, if it is still wanted.
func (b *sendBuffer) onLost(off, n uint64) {
	end := off + n
	off = max(off, b.base)
	if end <= off {
		return
	}
	b.lost = addSpan(b.lost, span{off, end})
}

// allAcked reports whether everything written has been acknowledged.
func (b *sendBuffer) allAcked() bool { return len(b.buf) == 0 }

// segment is received data waiting for what comes before it.
type segment struct {
	off  uint64
	data []byte
}

// recvBuffer reassembles a stream from frames that may arrive out of order,
// overlap or repeat.
type recvBuffer struct {
	// offset is how far the data has been delivered in order.
	offset   uint64
	segments []segment
	// buffered is how many bytes the segments hold.
	buffered int
}

// push stores a frame's data, less what has been delivered already; pop
// then hands it on in order.
func (r *recvBuffer) push(off uint64, data []byte) {
	end := off + uint64(len(data))
	if end <= r.offset || len(data) == 0 {
		return
	}
	if off < r.offset {
		data = data[r.offset-off:]
		off = r.offset
	}
	i := sort.Search(len(r.segments), func(i int) bool { return r.segments[i].off > off })
	r.segments = append(r.segments, segment{})
	copy(r.segments[i+1:], r.segments[i:])
	r.segments[i] = segment{off: off, data: data}
	r.buffered += len(data)
}

// pop returns the next data in order, or nil when there is a gap first.
func (r *recvBuffer) pop() []byte {
	for len(r.segments) > 0 {
		s := r.segments[0]
		if s.off > r.offset {
			return nil
		}
		r.segments = r.segments[1:]
		r.buffered -= len(s.data)
		end := s.off + uint64(len(s.data))
		if end <= r.offset {
			continue
		}
		data := s.data[r.offset-s.off:]
		r.offset = end
		return data
	}
	return nil
}

// pnRange is the packet numbers [lo, hi].
type pnRange struct{ lo, hi uint64 }

// maxAckRanges bounds the ranges kept for acknowledgement; the oldest go
// first, and the peer has long since given up on them.
const maxAckRanges = 32

// rangeSet is the packet numbers received in a space, as ascending, disjoint
// and non-adjacent ranges.
type rangeSet []pnRange

// add records pn, reporting false if it was already there or is too old to
// tell.
func (s *rangeSet) add(pn uint64) bool {
	r := *s
	if len(r) > 0 && pn < r[0].lo && len(r) == maxAckRanges {
		return false
	}
	i := sort.Search(len(r), func(i int) bool { return r[i].hi+1 >= pn })
	if i < len(r) && r[i].lo <= pn && pn <= r[i].hi {
		return false
	}
	switch {
	case i < len(r) && r[i].hi+1 == pn:
		r[i].hi = pn
		if i+1 < len(r) && r[i+1].lo == pn+1 {
			r[i].hi = r[i+1].hi
			r = append(r[:i+1], r[i+2:]...)
		}
	case i < len(r) && r[i].lo == pn+1:
		r[i].lo = pn
	default:
		r = append(r, pnRange{})
		copy(r[i+1:], r[i:])
		r[i] = pnRange{pn, pn}
	}
	if len(r) > maxAckRanges {
		r = r[1:]
	}
	*s = r
	return true
}

func (s rangeSet) largest() uint64 { return s[len(s)-1].hi }
