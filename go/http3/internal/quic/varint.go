package quic

// maxVarint is the largest value a variable-length integer holds (RFC 9000
// section 16).
const maxVarint = 1<<62 - 1

// AppendVarint appends v in the shortest variable-length encoding.
func AppendVarint(b []byte, v uint64) []byte {
	switch {
	case v < 1<<6:
		return append(b, byte(v))
	case v < 1<<14:
		return append(b, byte(v>>8)|0x40, byte(v))
	case v < 1<<30:
		return append(b, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(b, byte(v>>56)|0xc0, byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

// VarintLen is how many bytes AppendVarint takes for v.
func VarintLen(v uint64) int {
	switch {
	case v < 1<<6:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<30:
		return 4
	default:
		return 8
	}
}

// ReadVarint reads a variable-length integer from the front of b, returning
// it and its length, or a length of zero when b is too short.
func ReadVarint(b []byte) (uint64, int) {
	if len(b) == 0 {
		return 0, 0
	}
	n := 1 << (b[0] >> 6)
	if len(b) < n {
		return 0, 0
	}
	v := uint64(b[0] & 0x3f)
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, n
}

// appendVarint2 appends v, which must be below 2^14, in exactly two bytes,
// for a length written before what it measures is known.
func appendVarint2(b []byte, v uint64) []byte {
	return append(b, byte(v>>8)|0x40, byte(v))
}

// reader takes frames and parameters apart. The first failure sticks, so a
// run of reads is checked once at the end.
type reader struct {
	b   []byte
	bad bool
}

func (r *reader) varint() uint64 {
	v, n := ReadVarint(r.b)
	if n == 0 {
		r.bad = true
		r.b = nil
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if uint64(len(r.b)) < n {
		r.bad = true
		r.b = nil
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if len(r.b) == 0 {
		r.bad = true
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}
