package quic

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// Version1 is QUIC version 1 (RFC 9000), the only version spoken here.
const Version1 = 0x00000001

// Packet types. The first four are the long header types as the first byte
// carries them; packet1RTT has the short header.
const (
	packetInitial   = 0
	packet0RTT      = 1
	packetHandshake = 2
	packetRetry     = 3
	packet1RTT      = 4
)

const (
	// cidLen is the length of the connection IDs this side chooses.
	cidLen = 8
	// maxCIDLen is the longest connection ID version 1 allows.
	maxCIDLen = 20
	// minInitialDatagram is how large a datagram carrying a client's
	// Initial packet has to be (RFC 9000 section 14.1).
	minInitialDatagram = 1200
	// maxDatagram is the largest datagram sent. It is the size every IPv6
	// path carries, which spares path MTU discovery.
	maxDatagram = 1200
	// aeadOverhead is the tag every supported AEAD appends.
	aeadOverhead = 16
	// statelessResetTokenLen is the length of a stateless reset token.
	statelessResetTokenLen = 16
)

var (
	errBadPacket       = errors.New("quic: malformed packet")
	errUnknownVersion  = errors.New("quic: unsupported version")
	errDecryptionFails = errors.New("quic: packet decryption failed")
)

// header is a packet's header as far as it can be read before header
// protection comes off.
type header struct {
	typ     int
	version uint32
	dcid    []byte
	scid    []byte
	token   []byte
	// pnOffset is where the packet number starts, and end where the packet
	// ends within the datagram.
	pnOffset int
	end      int
}

// parseHeader reads the header of the packet at the front of b. A short
// header does not say how long its destination connection ID is, so the
// caller does. A packet of a version other than 1 is returned with
// errUnknownVersion and only its version and connection IDs filled in; so is
// a version negotiation packet, whose version is 0.
func parseHeader(b []byte, shortDCIDLen int) (header, error) {
	var h header
	if len(b) == 0 {
		return h, errBadPacket
	}
	if b[0]&0x80 == 0 {
		if len(b) < 1+shortDCIDLen {
			return h, errBadPacket
		}
		h.typ = packet1RTT
		h.dcid = b[1 : 1+shortDCIDLen]
		h.pnOffset = 1 + shortDCIDLen
		h.end = len(b)
		return h, nil
	}
	if len(b) < 7 {
		return h, errBadPacket
	}
	h.version = binary.BigEndian.Uint32(b[1:5])
	p := 5
	dl := int(b[p])
	p++
	if dl > maxCIDLen && h.version == Version1 || len(b) < p+dl+1 {
		return h, errBadPacket
	}
	h.dcid = b[p : p+dl]
	p += dl
	sl := int(b[p])
	p++
	if sl > maxCIDLen && h.version == Version1 || len(b) < p+sl {
		return h, errBadPacket
	}
	h.scid = b[p : p+sl]
	p += sl
	if h.version != Version1 {
		return h, errUnknownVersion
	}
	if b[0]&0x40 == 0 {
		return h, errBadPacket
	}
	h.typ = int(b[0]>>4) & 3
	switch h.typ {
	case packetRetry:
		h.token = b[p:]
		h.end = len(b)
		return h, nil
	case packetInitial:
		n, l := ReadVarint(b[p:])
		if l == 0 || uint64(len(b)-p-l) < n {
			return h, errBadPacket
		}
		p += l
		h.token = b[p : p+int(n)]
		p += int(n)
	}
	length, l := ReadVarint(b[p:])
	if l == 0 {
		return h, errBadPacket
	}
	p += l
	if uint64(len(b)-p) < length {
		return h, errBadPacket
	}
	h.pnOffset = p
	h.end = p + int(length)
	return h, nil
}

// pnLen is how many bytes packet number pn takes, given the largest
// acknowledged one (RFC 9000 appendix A.2).
func pnLen(pn uint64, largestAcked int64) int {
	var unacked uint64
	if largestAcked < 0 {
		unacked = pn + 1
	} else {
		unacked = pn - uint64(largestAcked)
	}
	switch {
	case unacked < 1<<7:
		return 1
	case unacked < 1<<15:
		return 2
	case unacked < 1<<23:
		return 3
	}
	return 4
}

// decodePN recovers a full packet number from its truncated form (RFC 9000
// appendix A.3).
func decodePN(largest int64, truncated uint64, n int) uint64 {
	expected := uint64(largest + 1)
	win := uint64(1) << (8 * n)
	hwin := win / 2
	mask := win - 1
	candidate := expected&^mask | truncated
	switch {
	case candidate+hwin <= expected && candidate < 1<<62-win:
		return candidate + win
	case candidate > expected+hwin && candidate >= win:
		return candidate - win
	}
	return candidate
}

// unprotect removes header protection from the packet pkt, whose packet
// number starts at pnOffset, and returns the packet number and its length.
func unprotect(pkt []byte, pnOffset int, hp headerProtector, largest int64) (uint64, int, bool) {
	if len(pkt) < pnOffset+4+16 {
		return 0, 0, false
	}
	mask := hp.mask(pkt[pnOffset+4 : pnOffset+20])
	if pkt[0]&0x80 != 0 {
		pkt[0] ^= mask[0] & 0x0f
	} else {
		pkt[0] ^= mask[0] & 0x1f
	}
	n := int(pkt[0]&3) + 1
	var truncated uint64
	for i := 0; i < n; i++ {
		pkt[pnOffset+i] ^= mask[1+i]
		truncated = truncated<<8 | uint64(pkt[pnOffset+i])
	}
	return decodePN(largest, truncated, n), n, true
}

// seal encrypts pkt in place, a header whose packet number of length n
// starts at pnOffset followed by the plaintext payload, and protects the
// header. pkt must have room for the tag; the sealed packet is returned.
func (k *keys) seal(pkt []byte, pnOffset, n int, pn uint64) []byte {
	hdr := pkt[:pnOffset+n]
	payload := pkt[pnOffset+n:]
	sealed := k.aead.Seal(payload[:0], k.nonce(pn), payload, hdr)
	pkt = pkt[:len(hdr)+len(sealed)]
	mask := k.hp.mask(pkt[pnOffset+4 : pnOffset+20])
	if pkt[0]&0x80 != 0 {
		pkt[0] ^= mask[0] & 0x0f
	} else {
		pkt[0] ^= mask[0] & 0x1f
	}
	for i := 0; i < n; i++ {
		pkt[pnOffset+i] ^= mask[1+i]
	}
	return pkt
}

// open decrypts the payload of pkt, whose header, now unprotected, ends at
// hdrEnd. The plaintext overwrites the ciphertext.
func (k *keys) open(pkt []byte, hdrEnd int, pn uint64) ([]byte, error) {
	payload, err := k.aead.Open(pkt[hdrEnd:hdrEnd], k.nonce(pn), pkt[hdrEnd:], pkt[:hdrEnd])
	if err != nil {
		return nil, errDecryptionFails
	}
	return payload, nil
}

// IsInitial reports whether datagram opens with a version 1 Initial packet
// large enough to start a connection, which is what a server should see
// before it sets up any state for a peer.
func IsInitial(datagram []byte) bool {
	if len(datagram) < minInitialDatagram {
		return false
	}
	h, err := parseHeader(datagram, 0)
	return err == nil && h.typ == packetInitial
}

// VersionNegotiation returns the version negotiation packet to answer
// datagram with, or nil if datagram needs none: it answers a long header
// packet of a version this side does not speak (RFC 9000 section 6).
func VersionNegotiation(datagram []byte) []byte {
	h, err := parseHeader(datagram, 0)
	if err != errUnknownVersion || h.version == 0 || len(datagram) < minInitialDatagram {
		return nil
	}
	out := make([]byte, 0, 7+len(h.dcid)+len(h.scid)+8)
	var r [1]byte
	_, _ = rand.Read(r[:])
	out = append(out, 0x80|r[0])
	out = append(out, 0, 0, 0, 0)
	out = append(out, byte(len(h.scid)))
	out = append(out, h.scid...)
	out = append(out, byte(len(h.dcid)))
	out = append(out, h.dcid...)
	out = binary.BigEndian.AppendUint32(out, Version1)
	// A reserved version (RFC 9000 section 15) keeps clients honest.
	out = binary.BigEndian.AppendUint32(out, 0x1a2a3a4a)
	return out
}

// ResetKey derives stateless reset tokens (RFC 9000 section 10.3): a server
// that has lost a connection's state can still prove to the peer that it
// once issued the connection ID the peer is sending to, and so end the
// connection at once instead of leaving the peer to time out.
type ResetKey [32]byte

// NewResetKey returns a random key.
func NewResetKey() ResetKey {
	var k ResetKey
	_, _ = rand.Read(k[:])
	return k
}

func (k *ResetKey) token(cid []byte) []byte {
	mac := hmac.New(sha256.New, k[:])
	mac.Write(cid)
	return mac.Sum(nil)[:statelessResetTokenLen]
}

// StatelessReset returns the stateless reset to answer datagram with, or nil
// when datagram is not a short header packet to one of this side's
// connection IDs, or is too small to be answered without the answer being
// usable to amplify.
func (k *ResetKey) StatelessReset(datagram []byte) []byte {
	if len(datagram) < 1+cidLen+21 || datagram[0]&0x80 != 0 {
		return nil
	}
	// The reset is smaller than what it answers, so two endpoints resetting
	// each other's resets run out of bytes (RFC 9000 section 10.3.3).
	size := min(len(datagram)-1, 64)
	out := make([]byte, size)
	_, _ = rand.Read(out[:size-statelessResetTokenLen])
	out[0] = out[0]&0x3f | 0x40
	copy(out[size-statelessResetTokenLen:], k.token(datagram[1:1+cidLen]))
	return out
}
