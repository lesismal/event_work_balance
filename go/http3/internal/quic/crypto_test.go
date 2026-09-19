package quic

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"testing"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestInitialKeys checks the Initial keys of RFC 9001 appendix A.1.
func TestInitialKeys(t *testing.T) {
	tx, rx := initialKeys(unhex(t, "8394c8f03e515708"), true)
	iv := tx.nonce(0)
	if !bytes.Equal(iv, unhex(t, "fa044b2f42a3fd3b46fb255c")) {
		t.Fatalf("client iv %x", iv)
	}
	if iv := rx.nonce(0); !bytes.Equal(iv, unhex(t, "0ac1493ca1905853b0bba03e")) {
		t.Fatalf("server iv %x", iv)
	}
	// The client's header protection key, checked through the mask it gives
	// the sample of appendix A.2.
	mask := tx.hp.mask(unhex(t, "d1b1c98dd7689fb8ec11d242b123dc9b"))
	if !bytes.Equal(mask[:], unhex(t, "437b9aec36")) {
		t.Fatalf("client hp mask %x", mask)
	}
}

// TestChaChaShortHeader protects the packet of RFC 9001 appendix A.5.
func TestChaChaShortHeader(t *testing.T) {
	secret := unhex(t, "9ac312a7f877468ebe69422748ad00a15443f18203a07d6060f688f30f21632b")
	k, err := newKeys(tls.TLS_CHACHA20_POLY1305_SHA256, secret)
	if err != nil {
		t.Fatal(err)
	}
	if iv := k.nonce(0); !bytes.Equal(iv, unhex(t, "e0459b3474bdd0e44a41c144")) {
		t.Fatalf("iv %x", iv)
	}
	pn := uint64(654360564)
	pkt := append(unhex(t, "4200bff4"), 0x01)
	pkt = append(pkt, make([]byte, aeadOverhead)...)[:5]
	sealed := k.seal(pkt, 1, 3, pn)
	want := unhex(t, "4cfe4189655e5cd55c41f69080575d7999c25a5bfb")
	if !bytes.Equal(sealed, want) {
		t.Fatalf("sealed %x, want %x", sealed, want)
	}
	gotPN, n, ok := unprotect(sealed, 1, k.hp, int64(pn-1))
	if !ok || n != 3 || gotPN != pn {
		t.Fatalf("unprotect: pn %d len %d ok %v", gotPN, n, ok)
	}
	payload, err := k.open(sealed, 1+n, pn)
	if err != nil || !bytes.Equal(payload, []byte{0x01}) {
		t.Fatalf("open: %x %v", payload, err)
	}
	next := k.next()
	if want := unhex(t, "1223504755036d556342ee9361d253421a826c9ecdf3c7148684b36b714881f9"); !bytes.Equal(next.secret, want) {
		t.Fatalf("key update secret %x", next.secret)
	}
}

// TestChaCha20Poly1305 checks the AEAD against RFC 8439 section 2.8.2.
func TestChaCha20Poly1305(t *testing.T) {
	key := unhex(t, "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f")
	nonce := unhex(t, "070000004041424344454647")
	aad := unhex(t, "50515253c0c1c2c3c4c5c6c7")
	plaintext := []byte("Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.")
	aead, err := newChaCha20Poly1305(key)
	if err != nil {
		t.Fatal(err)
	}
	sealed := aead.Seal(nil, nonce, plaintext, aad)
	if !bytes.HasPrefix(sealed, unhex(t, "d31a8d34648e60db7b86afbc53ef7ec2")) {
		t.Fatalf("ciphertext %x", sealed[:16])
	}
	if tag := sealed[len(sealed)-16:]; !bytes.Equal(tag, unhex(t, "1ae10b594f09e26a7e902ecbd0600691")) {
		t.Fatalf("tag %x", tag)
	}
	opened, err := aead.Open(nil, nonce, sealed, aad)
	if err != nil || !bytes.Equal(opened, plaintext) {
		t.Fatalf("open: %v", err)
	}
	sealed[3] ^= 1
	if _, err := aead.Open(nil, nonce, sealed, aad); err == nil {
		t.Fatal("tampered ciphertext opened")
	}
}

func TestRetryTag(t *testing.T) {
	// RFC 9001 appendix A.4.
	retry := unhex(t, "ff000000010008f067a5502a4262b5746f6b656e04a265ba2eff4d829058fb3f0f2496ba")
	tag := retryTag(unhex(t, "8394c8f03e515708"), retry[:len(retry)-16])
	if !bytes.Equal(tag, retry[len(retry)-16:]) {
		t.Fatalf("retry tag %x", tag)
	}
}

func TestVarint(t *testing.T) {
	for _, v := range []uint64{0, 63, 64, 16383, 16384, 1<<30 - 1, 1 << 30, maxVarint} {
		b := AppendVarint(nil, v)
		if len(b) != VarintLen(v) {
			t.Fatalf("%d: length %d, want %d", v, len(b), VarintLen(v))
		}
		got, n := ReadVarint(b)
		if got != v || n != len(b) {
			t.Fatalf("%d: read %d (%d bytes)", v, got, n)
		}
	}
	// RFC 9000 appendix A.1.
	if v, _ := ReadVarint(unhex(t, "c2197c5eff14e88c")); v != 151288809941952652 {
		t.Fatalf("got %d", v)
	}
}

func TestDecodePN(t *testing.T) {
	// RFC 9000 appendix A.3.
	if pn := decodePN(0xa82f30ea, 0x9b32, 2); pn != 0xa82f9b32 {
		t.Fatalf("got %x", pn)
	}
}

func TestRangeSet(t *testing.T) {
	var s rangeSet
	for _, pn := range []uint64{5, 1, 2, 3, 9, 4, 7} {
		if !s.add(pn) {
			t.Fatalf("%d reported duplicate", pn)
		}
	}
	if s.add(3) {
		t.Fatal("duplicate accepted")
	}
	want := rangeSet{{1, 5}, {7, 7}, {9, 9}}
	if len(s) != len(want) {
		t.Fatalf("got %v", s)
	}
	for i := range want {
		if s[i] != want[i] {
			t.Fatalf("got %v", s)
		}
	}
}

func TestSendBuffer(t *testing.T) {
	var b sendBuffer
	b.write([]byte("0123456789"))
	off, data := b.popNew(4)
	if off != 0 || string(data) != "0123" {
		t.Fatalf("%d %q", off, data)
	}
	b.popNew(100)
	b.onAck(4, 3)
	if b.base != 0 {
		t.Fatal("base moved past a gap")
	}
	b.onLost(0, 4)
	off, data = b.popLost(10)
	if off != 0 || string(data) != "0123" {
		t.Fatalf("%d %q", off, data)
	}
	b.onAck(0, 4)
	if b.base != 7 || string(b.buf) != "789" {
		t.Fatalf("base %d buf %q", b.base, b.buf)
	}
}

func TestRecvBuffer(t *testing.T) {
	var r recvBuffer
	r.push(5, []byte("56789"))
	if r.pop() != nil {
		t.Fatal("delivered past a gap")
	}
	r.push(0, []byte("0123"))
	r.push(2, []byte("2345"))
	var got []byte
	for d := r.pop(); d != nil; d = r.pop() {
		got = append(got, d...)
	}
	if string(got) != "0123456789" {
		t.Fatalf("got %q", got)
	}
}
