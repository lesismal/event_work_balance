// Package qpack implements the parts of QPACK (RFC 9204) that HTTP/3 needs
// without a dynamic table: field sections encoded with the static table and
// literals, and decoded likewise. Both sides here advertise a dynamic table
// capacity of zero, so a peer may not refer to one, and neither the encoder
// nor the decoder stream carries anything.
package qpack

import (
	"errors"

	"github.com/lesismal/fib/go/internal/hpack"
)

// HeaderField is one field line, with its name lowercased.
type HeaderField struct {
	Name, Value string
}

var (
	// ErrDecompression is a field section that cannot be decoded, which
	// HTTP/3 treats as QPACK_DECOMPRESSION_FAILED.
	ErrDecompression = errors.New("qpack: decompression failed")
	// ErrTooLarge is a field section beyond the decoder's limit.
	ErrTooLarge = errors.New("qpack: field section too large")
)

// AppendField appends one field line; a section starts with Prefix.
func AppendField(dst []byte, name, value string, neverIndex bool) []byte {
	if i, ok := staticPairIndex[HeaderField{name, value}]; ok && !neverIndex {
		// Indexed field line, static table.
		return appendInt(dst, 0xc0, 6, uint64(i))
	}
	if i, ok := staticNameIndex[name]; ok {
		// Literal field line with a static name reference.
		first := byte(0x50)
		if neverIndex {
			first |= 0x20
		}
		dst = appendInt(dst, first, 4, uint64(i))
		return appendString(dst, 0, 7, value)
	}
	// Literal field line with a literal name.
	first := byte(0x20)
	if neverIndex {
		first |= 0x10
	}
	dst = appendString(dst, first, 3, name)
	return appendString(dst, 0, 7, value)
}

// Prefix is the section prefix AppendField expects to follow.
var Prefix = []byte{0, 0}

// Decode decodes a field section, calling emit for each field in order. It
// stops with ErrTooLarge once the fields' size, counted as RFC 9114 section
// 4.2.2 does, would pass maxSize, if maxSize is positive.
func Decode(block []byte, maxSize int, emit func(HeaderField) error) error {
	ric, rest, err := readInt(block, 8)
	if err != nil {
		return err
	}
	if ric != 0 {
		// A reference to a dynamic table this side said it does not have.
		return ErrDecompression
	}
	if _, rest, err = readInt(rest, 7); err != nil {
		return err
	}
	size := 0
	for len(rest) > 0 {
		var f HeaderField
		b := rest[0]
		switch {
		case b&0x80 != 0:
			// Indexed field line.
			if b&0x40 == 0 {
				return ErrDecompression
			}
			var i uint64
			if i, rest, err = readInt(rest, 6); err != nil {
				return err
			}
			if i >= uint64(len(staticTable)) {
				return ErrDecompression
			}
			f = staticTable[i]
		case b&0xc0 == 0x40:
			// Literal field line with a name reference.
			if b&0x10 == 0 {
				return ErrDecompression
			}
			var i uint64
			if i, rest, err = readInt(rest, 4); err != nil {
				return err
			}
			if i >= uint64(len(staticTable)) {
				return ErrDecompression
			}
			f.Name = staticTable[i].Name
			if f.Value, rest, err = readString(rest, 7, maxSize); err != nil {
				return err
			}
		case b&0xe0 == 0x20:
			// Literal field line with a literal name.
			if f.Name, rest, err = readString(rest, 3, maxSize); err != nil {
				return err
			}
			if f.Value, rest, err = readString(rest, 7, maxSize); err != nil {
				return err
			}
		default:
			// Post-base references, which need a dynamic table.
			return ErrDecompression
		}
		size += len(f.Name) + len(f.Value) + 32
		if maxSize > 0 && size > maxSize {
			return ErrTooLarge
		}
		if err := emit(f); err != nil {
			return err
		}
	}
	return nil
}

// readInt reads an integer with an n-bit prefix (RFC 7541 section 5.1).
func readInt(b []byte, n uint8) (uint64, []byte, error) {
	if len(b) == 0 {
		return 0, nil, ErrDecompression
	}
	mask := uint64(1)<<n - 1
	v := uint64(b[0]) & mask
	b = b[1:]
	if v < mask {
		return v, b, nil
	}
	var shift uint
	for i, c := range b {
		v += uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, b[i+1:], nil
		}
		if shift += 7; shift >= 56 {
			return 0, nil, ErrDecompression
		}
	}
	return 0, nil, ErrDecompression
}

func appendInt(dst []byte, first byte, n uint8, v uint64) []byte {
	mask := uint64(1)<<n - 1
	if v < mask {
		return append(dst, first|byte(v))
	}
	dst = append(dst, first|byte(mask))
	v -= mask
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// readString reads a string whose length has an n-bit prefix, with the
// Huffman flag in the bit just above it.
func readString(b []byte, n uint8, maxLen int) (string, []byte, error) {
	if len(b) == 0 {
		return "", nil, ErrDecompression
	}
	huffman := b[0]&(1<<n) != 0
	length, rest, err := readInt(b, n)
	if err != nil {
		return "", nil, err
	}
	if length > uint64(len(rest)) {
		return "", nil, ErrDecompression
	}
	raw := rest[:length]
	rest = rest[length:]
	if maxLen > 0 && len(raw) > maxLen {
		return "", nil, ErrTooLarge
	}
	if !huffman {
		return string(raw), rest, nil
	}
	s, err := hpack.HuffmanDecode(raw, maxLen)
	if errors.Is(err, hpack.ErrStringTooLong) {
		return "", nil, ErrTooLarge
	}
	if err != nil {
		return "", nil, ErrDecompression
	}
	return s, rest, nil
}

// appendString appends s with an n-bit length prefix after the bits of
// first, Huffman coded when that is shorter.
func appendString(dst []byte, first byte, n uint8, s string) []byte {
	if h := hpack.HuffmanLen(s); h < len(s) {
		dst = appendInt(dst, first|1<<n, n, uint64(h))
		return hpack.AppendHuffman(dst, s)
	}
	dst = appendInt(dst, first, n, uint64(len(s)))
	return append(dst, s...)
}

var (
	staticPairIndex = make(map[HeaderField]int, len(staticTable))
	staticNameIndex = make(map[string]int, len(staticTable))
)

func init() {
	for i := len(staticTable) - 1; i >= 0; i-- {
		f := staticTable[i]
		staticPairIndex[f] = i
		staticNameIndex[f.Name] = i
	}
}

// staticTable is RFC 9204 appendix A.
var staticTable = [...]HeaderField{
	{":authority", ""},
	{":path", "/"},
	{"age", "0"},
	{"content-disposition", ""},
	{"content-length", "0"},
	{"cookie", ""},
	{"date", ""},
	{"etag", ""},
	{"if-modified-since", ""},
	{"if-none-match", ""},
	{"last-modified", ""},
	{"link", ""},
	{"location", ""},
	{"referer", ""},
	{"set-cookie", ""},
	{":method", "CONNECT"},
	{":method", "DELETE"},
	{":method", "GET"},
	{":method", "HEAD"},
	{":method", "OPTIONS"},
	{":method", "POST"},
	{":method", "PUT"},
	{":scheme", "http"},
	{":scheme", "https"},
	{":status", "103"},
	{":status", "200"},
	{":status", "304"},
	{":status", "404"},
	{":status", "503"},
	{"accept", "*/*"},
	{"accept", "application/dns-message"},
	{"accept-encoding", "gzip, deflate, br"},
	{"accept-ranges", "bytes"},
	{"access-control-allow-headers", "cache-control"},
	{"access-control-allow-headers", "content-type"},
	{"access-control-allow-origin", "*"},
	{"cache-control", "max-age=0"},
	{"cache-control", "max-age=2592000"},
	{"cache-control", "max-age=604800"},
	{"cache-control", "no-cache"},
	{"cache-control", "no-store"},
	{"cache-control", "public, max-age=31536000"},
	{"content-encoding", "br"},
	{"content-encoding", "gzip"},
	{"content-type", "application/dns-message"},
	{"content-type", "application/javascript"},
	{"content-type", "application/json"},
	{"content-type", "application/x-www-form-urlencoded"},
	{"content-type", "image/gif"},
	{"content-type", "image/jpeg"},
	{"content-type", "image/png"},
	{"content-type", "text/css"},
	{"content-type", "text/html; charset=utf-8"},
	{"content-type", "text/plain"},
	{"content-type", "text/plain;charset=utf-8"},
	{"range", "bytes=0-"},
	{"strict-transport-security", "max-age=31536000"},
	{"strict-transport-security", "max-age=31536000; includesubdomains"},
	{"strict-transport-security", "max-age=31536000; includesubdomains; preload"},
	{"vary", "accept-encoding"},
	{"vary", "origin"},
	{"x-content-type-options", "nosniff"},
	{"x-xss-protection", "1; mode=block"},
	{":status", "100"},
	{":status", "204"},
	{":status", "206"},
	{":status", "302"},
	{":status", "400"},
	{":status", "403"},
	{":status", "421"},
	{":status", "425"},
	{":status", "500"},
	{"accept-language", ""},
	{"access-control-allow-credentials", "FALSE"},
	{"access-control-allow-credentials", "TRUE"},
	{"access-control-allow-headers", "*"},
	{"access-control-allow-methods", "get"},
	{"access-control-allow-methods", "get, post, options"},
	{"access-control-allow-methods", "options"},
	{"access-control-expose-headers", "content-length"},
	{"access-control-request-headers", "content-type"},
	{"access-control-request-method", "get"},
	{"access-control-request-method", "post"},
	{"alt-svc", "clear"},
	{"authorization", ""},
	{"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"},
	{"early-data", "1"},
	{"expect-ct", ""},
	{"forwarded", ""},
	{"if-range", ""},
	{"origin", ""},
	{"purpose", "prefetch"},
	{"server", ""},
	{"timing-allow-origin", "*"},
	{"upgrade-insecure-requests", "1"},
	{"user-agent", ""},
	{"x-forwarded-for", ""},
	{"x-frame-options", "deny"},
	{"x-frame-options", "sameorigin"},
}
