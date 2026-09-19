package quic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"hash"
)

// Packet protection (RFC 9001 section 5): each packet number space has a key
// per direction, derived from a TLS secret, and a header protection key that
// hides the packet number and the low bits of the first byte.

// initialSalt derives the Initial secrets of QUIC version 1.
var initialSalt = []byte{
	0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17,
	0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a,
}

// headerProtector computes the header protection mask from a sample of the
// ciphertext.
type headerProtector interface {
	mask(sample []byte) [5]byte
}

type aesHeaderProtector struct{ block cipher.Block }

func (h aesHeaderProtector) mask(sample []byte) (m [5]byte) {
	var out [16]byte
	h.block.Encrypt(out[:], sample[:16])
	copy(m[:], out[:])
	return m
}

type chachaHeaderProtector struct{ key [8]uint32 }

func (h chachaHeaderProtector) mask(sample []byte) (m [5]byte) {
	counter := binary.LittleEndian.Uint32(sample[0:4])
	nonce := chachaNonce(sample[4:16])
	var block [64]byte
	chachaBlock(&block, &h.key, counter, &nonce)
	copy(m[:], block[:])
	return m
}

// keys protects the packets of one direction of one packet number space.
type keys struct {
	suite  uint16
	secret []byte
	aead   cipher.AEAD
	iv     [12]byte
	hp     headerProtector
}

func suiteHash(suite uint16) func() hash.Hash {
	if suite == tls.TLS_AES_256_GCM_SHA384 {
		return sha512.New384
	}
	return sha256.New
}

func suiteKeyLen(suite uint16) int {
	if suite == tls.TLS_AES_128_GCM_SHA256 {
		return 16
	}
	return 32
}

// expandLabel is HKDF-Expand-Label from TLS 1.3 (RFC 8446 section 7.1), with
// an empty context as every QUIC label uses.
func expandLabel(h func() hash.Hash, secret []byte, label string, length int) []byte {
	info := make([]byte, 0, 4+6+len(label))
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	info = append(info, byte(6+len(label)))
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, 0)
	out, err := hkdf.Expand(h, secret, string(info), length)
	if err != nil {
		panic("quic: hkdf: " + err.Error())
	}
	return out
}

func newAEAD(suite uint16, key []byte) (cipher.AEAD, error) {
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		return newChaCha20Poly1305(key)
	}
	return nil, errors.New("quic: unsupported cipher suite")
}

// newKeys derives packet protection from a TLS traffic secret.
func newKeys(suite uint16, secret []byte) (*keys, error) {
	h := suiteHash(suite)
	keyLen := suiteKeyLen(suite)
	k := &keys{suite: suite, secret: secret}
	var err error
	if k.aead, err = newAEAD(suite, expandLabel(h, secret, "quic key", keyLen)); err != nil {
		return nil, err
	}
	copy(k.iv[:], expandLabel(h, secret, "quic iv", 12))
	hpKey := expandLabel(h, secret, "quic hp", keyLen)
	if suite == tls.TLS_CHACHA20_POLY1305_SHA256 {
		k.hp = chachaHeaderProtector{key: chachaKey(hpKey)}
	} else {
		block, err := aes.NewCipher(hpKey)
		if err != nil {
			return nil, err
		}
		k.hp = aesHeaderProtector{block: block}
	}
	return k, nil
}

// next derives the keys of the following key phase (RFC 9001 section 6).
// Header protection does not change with the phase.
func (k *keys) next() *keys {
	h := suiteHash(k.suite)
	secret := expandLabel(h, k.secret, "quic ku", h().Size())
	aead, err := newAEAD(k.suite, expandLabel(h, secret, "quic key", suiteKeyLen(k.suite)))
	if err != nil {
		panic(err)
	}
	n := &keys{suite: k.suite, secret: secret, aead: aead, hp: k.hp}
	copy(n.iv[:], expandLabel(h, secret, "quic iv", 12))
	return n
}

func (k *keys) nonce(pn uint64) []byte {
	n := k.iv
	for i := 0; i < 8; i++ {
		n[11-i] ^= byte(pn >> (8 * i))
	}
	return n[:]
}

// initialKeys derives the Initial keys both sides compute from the
// connection ID the client first sends to (RFC 9001 section 5.2).
func initialKeys(dcid []byte, isClient bool) (tx, rx *keys) {
	initial, err := hkdf.Extract(sha256.New, dcid, initialSalt)
	if err != nil {
		panic(err)
	}
	client := expandLabel(sha256.New, initial, "client in", 32)
	server := expandLabel(sha256.New, initial, "server in", 32)
	ck, err := newKeys(tls.TLS_AES_128_GCM_SHA256, client)
	if err != nil {
		panic(err)
	}
	sk, err := newKeys(tls.TLS_AES_128_GCM_SHA256, server)
	if err != nil {
		panic(err)
	}
	if isClient {
		return ck, sk
	}
	return sk, ck
}

// retryAEAD computes the integrity tag of Retry packets (RFC 9001 section
// 5.8).
var retryAEAD = func() cipher.AEAD {
	key := []byte{0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e}
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return aead
}()

var retryNonce = []byte{0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb}

// retryTag is the tag a Retry packet, given without its tag, should carry
// for a client whose first destination connection ID was odcid.
func retryTag(odcid, packet []byte) []byte {
	pseudo := make([]byte, 0, 1+len(odcid)+len(packet))
	pseudo = append(pseudo, byte(len(odcid)))
	pseudo = append(pseudo, odcid...)
	pseudo = append(pseudo, packet...)
	return retryAEAD.Seal(nil, retryNonce, nil, pseudo)
}
