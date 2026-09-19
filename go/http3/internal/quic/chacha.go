package quic

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"math/bits"
)

// TLS 1.3 may settle on TLS_CHACHA20_POLY1305_SHA256, and QUIC then protects
// packets and headers with ChaCha20 (RFC 9001 section 5.4.4). The standard
// library keeps its implementation internal, so this is a plain one of RFC
// 8439: correct and constant-time, if not as fast as assembly.

const (
	chachaKeySize   = 32
	chachaNonceSize = 12
	poly1305TagSize = 16
)

// chachaBlock computes the ChaCha20 block for key, counter and nonce.
func chachaBlock(out *[64]byte, key *[8]uint32, counter uint32, nonce *[3]uint32) {
	s := [16]uint32{
		0x61707865, 0x3320646e, 0x79622d32, 0x6b206574,
		key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
		counter, nonce[0], nonce[1], nonce[2],
	}
	x := s
	qr := func(a, b, c, d int) {
		x[a] += x[b]
		x[d] = bits.RotateLeft32(x[d]^x[a], 16)
		x[c] += x[d]
		x[b] = bits.RotateLeft32(x[b]^x[c], 12)
		x[a] += x[b]
		x[d] = bits.RotateLeft32(x[d]^x[a], 8)
		x[c] += x[d]
		x[b] = bits.RotateLeft32(x[b]^x[c], 7)
	}
	for i := 0; i < 10; i++ {
		qr(0, 4, 8, 12)
		qr(1, 5, 9, 13)
		qr(2, 6, 10, 14)
		qr(3, 7, 11, 15)
		qr(0, 5, 10, 15)
		qr(1, 6, 11, 12)
		qr(2, 7, 8, 13)
		qr(3, 4, 9, 14)
	}
	for i := range x {
		binary.LittleEndian.PutUint32(out[4*i:], x[i]+s[i])
	}
}

func chachaKey(key []byte) (k [8]uint32) {
	for i := range k {
		k[i] = binary.LittleEndian.Uint32(key[4*i:])
	}
	return k
}

func chachaNonce(nonce []byte) (n [3]uint32) {
	for i := range n {
		n[i] = binary.LittleEndian.Uint32(nonce[4*i:])
	}
	return n
}

// chachaXOR XORs src with the key stream starting at block counter into dst,
// which may be src itself.
func chachaXOR(dst, src []byte, key *[8]uint32, counter uint32, nonce *[3]uint32) {
	var block [64]byte
	for len(src) > 0 {
		chachaBlock(&block, key, counter, nonce)
		counter++
		n := subtle.XORBytes(dst, src, block[:])
		dst, src = dst[n:], src[n:]
	}
}

// poly1305 is the one-time authenticator of RFC 8439 section 2.5, with the
// accumulator in three 64-bit limbs.
type poly1305 struct {
	r0, r1     uint64
	s0, s1     uint64
	h0, h1, h2 uint64
}

func newPoly1305(key *[32]byte) *poly1305 {
	return &poly1305{
		r0: binary.LittleEndian.Uint64(key[0:]) & 0x0ffffffc0fffffff,
		r1: binary.LittleEndian.Uint64(key[8:]) & 0x0ffffffc0ffffffc,
		s0: binary.LittleEndian.Uint64(key[16:]),
		s1: binary.LittleEndian.Uint64(key[24:]),
	}
}

// block absorbs one 16-byte block, full or already padded; hibit is 1 for a
// full block and 0 for a padded final one.
func (p *poly1305) block(m []byte, hibit uint64) {
	var c uint64
	p.h0, c = bits.Add64(p.h0, binary.LittleEndian.Uint64(m[0:]), 0)
	p.h1, c = bits.Add64(p.h1, binary.LittleEndian.Uint64(m[8:]), c)
	p.h2 += c + hibit

	// h *= r, with h2 small enough that its products fit in 64 bits.
	h0r0hi, h0r0lo := bits.Mul64(p.h0, p.r0)
	h1r0hi, h1r0lo := bits.Mul64(p.h1, p.r0)
	h0r1hi, h0r1lo := bits.Mul64(p.h0, p.r1)
	h1r1hi, h1r1lo := bits.Mul64(p.h1, p.r1)
	h2r0 := p.h2 * p.r0
	h2r1 := p.h2 * p.r1

	m1lo, c := bits.Add64(h1r0lo, h0r1lo, 0)
	m1hi, _ := bits.Add64(h1r0hi, h0r1hi, c)
	m2lo, c := bits.Add64(h2r0, h1r1lo, 0)
	m2hi, _ := bits.Add64(0, h1r1hi, c)
	m3 := h2r1

	t0 := h0r0lo
	t1, c := bits.Add64(m1lo, h0r0hi, 0)
	t2, c := bits.Add64(m2lo, m1hi, c)
	t3, _ := bits.Add64(m3, m2hi, c)

	// Reduce modulo 2^130 - 5: what lies above bit 130 comes back times 5,
	// as 4x plus x.
	p.h0, p.h1, p.h2 = t0, t1, t2&3
	cc0, cc1 := t2&^3, t3
	p.h0, c = bits.Add64(p.h0, cc0, 0)
	p.h1, c = bits.Add64(p.h1, cc1, c)
	p.h2 += c
	cc0 = cc0>>2 | cc1<<62
	cc1 >>= 2
	p.h0, c = bits.Add64(p.h0, cc0, 0)
	p.h1, c = bits.Add64(p.h1, cc1, c)
	p.h2 += c
}

// write absorbs data, padding a final partial block with zeros, as the AEAD
// construction does for each of its parts.
func (p *poly1305) writePadded(data []byte) {
	for len(data) >= 16 {
		p.block(data[:16], 1)
		data = data[16:]
	}
	if len(data) > 0 {
		var buf [16]byte
		copy(buf[:], data)
		p.block(buf[:], 1)
	}
}

func (p *poly1305) sum(out *[16]byte) {
	// Subtract p = 2^130 - 5 once if h >= p.
	t0, b := bits.Sub64(p.h0, 0xfffffffffffffffb, 0)
	t1, b := bits.Sub64(p.h1, 0xffffffffffffffff, b)
	_, b = bits.Sub64(p.h2, 3, b)
	mask := b - 1 // all ones when there was no borrow, so h >= p
	h0 := p.h0&^mask | t0&mask
	h1 := p.h1&^mask | t1&mask
	h0, c := bits.Add64(h0, p.s0, 0)
	h1, _ = bits.Add64(h1, p.s1, c)
	binary.LittleEndian.PutUint64(out[0:], h0)
	binary.LittleEndian.PutUint64(out[8:], h1)
}

// chacha20Poly1305 is the AEAD of RFC 8439 section 2.8.
type chacha20Poly1305 struct {
	key [8]uint32
}

func newChaCha20Poly1305(key []byte) (cipher.AEAD, error) {
	if len(key) != chachaKeySize {
		return nil, errors.New("quic: bad ChaCha20-Poly1305 key size")
	}
	return &chacha20Poly1305{key: chachaKey(key)}, nil
}

func (*chacha20Poly1305) NonceSize() int { return chachaNonceSize }
func (*chacha20Poly1305) Overhead() int  { return poly1305TagSize }

func (a *chacha20Poly1305) tag(out *[16]byte, nonce *[3]uint32, ciphertext, additionalData []byte) {
	var block [64]byte
	chachaBlock(&block, &a.key, 0, nonce)
	var polyKey [32]byte
	copy(polyKey[:], block[:32])
	p := newPoly1305(&polyKey)
	p.writePadded(additionalData)
	p.writePadded(ciphertext)
	var lens [16]byte
	binary.LittleEndian.PutUint64(lens[0:], uint64(len(additionalData)))
	binary.LittleEndian.PutUint64(lens[8:], uint64(len(ciphertext)))
	p.block(lens[:], 1)
	p.sum(out)
}

func (a *chacha20Poly1305) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	n := chachaNonce(nonce)
	ret, out := sliceForAppend(dst, len(plaintext)+poly1305TagSize)
	chachaXOR(out, plaintext, &a.key, 1, &n)
	var tag [16]byte
	a.tag(&tag, &n, out[:len(plaintext)], additionalData)
	copy(out[len(plaintext):], tag[:])
	return ret
}

var errOpen = errors.New("quic: message authentication failed")

func (a *chacha20Poly1305) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if len(ciphertext) < poly1305TagSize {
		return nil, errOpen
	}
	n := chachaNonce(nonce)
	body := ciphertext[:len(ciphertext)-poly1305TagSize]
	var tag [16]byte
	a.tag(&tag, &n, body, additionalData)
	if subtle.ConstantTimeCompare(tag[:], ciphertext[len(body):]) != 1 {
		return nil, errOpen
	}
	ret, out := sliceForAppend(dst, len(body))
	chachaXOR(out, body, &a.key, 1, &n)
	return ret, nil
}

// sliceForAppend extends in by n bytes, returning the whole and the tail.
func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}
	return head, head[len(in):]
}
