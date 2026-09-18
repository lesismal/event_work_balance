package websocket

import "unicode/utf8"

// utf8Validator checks UTF-8 text that arrives in pieces: the fragments of a
// message, or the parts of one frame that are split across reads. It rejects
// a byte as soon as no valid sequence can continue with it, which is what lets
// the parser fail a text message before the rest of it arrives (RFC 6455
// section 8.1).
type utf8Validator struct {
	// need is the number of continuation bytes the current sequence still
	// needs, and lo and hi bound the next one of them.
	need   uint8
	lo, hi byte
}

// write validates p as the continuation of the bytes seen so far.
func (v *utf8Validator) write(p []byte) bool {
	for v.need != 0 {
		if len(p) == 0 {
			return true
		}
		if !v.step(p[0]) {
			return false
		}
		p = p[1:]
	}
	// Only the last three bytes can start a sequence cut off by the end of p;
	// everything before them is checked in bulk.
	tail := len(p)
	for i := len(p) - 1; i >= 0 && i >= len(p)-3; i-- {
		if p[i]&0xc0 != 0x80 {
			if p[i] >= 0xc0 {
				tail = i
			}
			break
		}
	}
	if !utf8.Valid(p[:tail]) {
		return false
	}
	for _, b := range p[tail:] {
		if !v.step(b) {
			return false
		}
	}
	return true
}

// complete reports whether the bytes seen so far end on a sequence boundary.
func (v *utf8Validator) complete() bool { return v.need == 0 }

func (v *utf8Validator) reset() { *v = utf8Validator{} }

func (v *utf8Validator) step(b byte) bool {
	if v.need != 0 {
		if b < v.lo || b > v.hi {
			return false
		}
		v.need--
		v.lo, v.hi = 0x80, 0xbf
		return true
	}
	v.lo, v.hi = 0x80, 0xbf
	switch {
	case b < 0x80:
	case b >= 0xc2 && b <= 0xdf:
		v.need = 1
	case b == 0xe0:
		v.need, v.lo = 2, 0xa0
	case b == 0xed:
		v.need, v.hi = 2, 0x9f
	case b >= 0xe1 && b <= 0xef:
		v.need = 2
	case b == 0xf0:
		v.need, v.lo = 3, 0x90
	case b >= 0xf1 && b <= 0xf3:
		v.need = 3
	case b == 0xf4:
		v.need, v.hi = 3, 0x8f
	default:
		return false
	}
	return true
}
