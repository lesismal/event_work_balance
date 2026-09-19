package qpack

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func encode(fields []HeaderField) []byte {
	b := append([]byte(nil), Prefix...)
	for _, f := range fields {
		b = AppendField(b, f.Name, f.Value, f.Name == "authorization")
	}
	return b
}

func decode(t *testing.T, block []byte, maxSize int) ([]HeaderField, error) {
	t.Helper()
	var fields []HeaderField
	err := Decode(block, maxSize, func(f HeaderField) error {
		fields = append(fields, f)
		return nil
	})
	return fields, err
}

func TestRoundTrip(t *testing.T) {
	fields := []HeaderField{
		{":method", "GET"},                    // static pair
		{":path", "/index.html?q=1"},          // static name
		{":status", "200"},                    // static pair
		{"content-type", "text/html; x=y"},    // static name, other value
		{"x-custom", "value"},                 // literal name
		{"authorization", "Bearer secret"},    // never indexed
		{"x-empty", ""},                       // empty value
		{"x-long", strings.Repeat("ab", 300)}, // long, multi-byte length
	}
	block := encode(fields)
	got, err := decode(t, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(fields) {
		t.Fatalf("got %d fields", len(got))
	}
	for i := range fields {
		if got[i] != fields[i] {
			t.Fatalf("field %d: got %v, want %v", i, got[i], fields[i])
		}
	}
	// A static pair takes one byte.
	if b := AppendField(nil, ":method", "GET", false); !bytes.Equal(b, []byte{0xc0 | 17}) {
		t.Fatalf(":method GET encoded as %x", b)
	}
}

// TestDecodeExample decodes RFC 9204 appendix B.1, a field section that
// uses only the static table.
func TestDecodeExample(t *testing.T) {
	block := []byte{0x00, 0x00, 0x51, 0x0b, '/', 'i', 'n', 'd', 'e', 'x', '.', 'h', 't', 'm', 'l'}
	got, err := decode(t, block, 0)
	if err != nil || len(got) != 1 || got[0] != (HeaderField{":path", "/index.html"}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestDecodeErrors(t *testing.T) {
	cases := map[string][]byte{
		"dynamic insert count": {0x01, 0x00},
		"dynamic index":        {0x00, 0x00, 0x80},
		"post-base index":      {0x00, 0x00, 0x10},
		"static out of range":  {0x00, 0x00, 0xff, 0x40},
		"truncated string":     {0x00, 0x00, 0x51, 0x05, 'a'},
		"empty":                {},
	}
	for name, block := range cases {
		if _, err := decode(t, block, 0); !errors.Is(err, ErrDecompression) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	big := encode([]HeaderField{{"x-a", strings.Repeat("v", 100)}})
	if _, err := decode(t, big, 64); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("size limit: got %v", err)
	}
}
