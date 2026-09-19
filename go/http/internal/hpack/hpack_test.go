package hpack

import (
	"encoding/hex"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func decodeAll(t *testing.T, d *Decoder, block []byte) []HeaderField {
	t.Helper()
	var fields []HeaderField
	if err := d.Decode(block, func(f HeaderField) error {
		fields = append(fields, f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return fields
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDecodeRFCExamples decodes RFC 7541 Appendix C.4: three requests on one
// connection, Huffman coded, sharing a dynamic table.
func TestDecodeRFCExamples(t *testing.T) {
	d := NewDecoder(DefaultTableSize)
	steps := []struct {
		block string
		want  []HeaderField
		size  int
	}{
		{"8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff", []HeaderField{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"},
		}, 57},
		{"8286 84be 5886 a8eb 1064 9cbf", []HeaderField{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"},
			{"cache-control", "no-cache"},
		}, 110},
		{"8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf", []HeaderField{
			{":method", "GET"}, {":scheme", "https"}, {":path", "/index.html"}, {":authority", "www.example.com"},
			{"custom-key", "custom-value"},
		}, 164},
	}
	for i, step := range steps {
		got := decodeAll(t, d, unhex(t, step.block))
		if !reflect.DeepEqual(got, step.want) {
			t.Fatalf("step %d: got %v, want %v", i, got, step.want)
		}
		if d.table.size != step.size {
			t.Fatalf("step %d: table size %d, want %d", i, d.table.size, step.size)
		}
	}
}

// TestDecodeEviction decodes RFC 7541 Appendix C.6, whose 256-byte table
// evicts entries as the responses go.
func TestDecodeEviction(t *testing.T) {
	d := NewDecoder(256)
	d.table.setMaxSize(256)
	blocks := []string{
		"4882 6402 5885 aec3 771a 4b61 96d0 7abe 9410 54d4 44a8 2005 9504 0b81 66e0 82a6 2d1b ff6e 919d 29ad 1718 63c7 8f0b 97c8 e9ae 82ae 43d3",
		"4883 640e ff c1 c0 bf",
		"88c1 6196 d07a be94 1054 d444 a820 0595 040b 8166 e084 a62d 1bff c05a 839b d9ab 77ad 94e7 821d d7f2 e6c7 b335 dfdf cd5b 3960 d5af 2708 7f36 72c1 ab27 0fb5 291f 9587 3160 65c0 03ed 4ee5 b106 3d50 07",
	}
	last := []HeaderField{
		{":status", "200"}, {"cache-control", "private"}, {"date", "Mon, 21 Oct 2013 20:13:22 GMT"},
		{"location", "https://www.example.com"}, {"content-encoding", "gzip"},
		{"set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"},
	}
	var got []HeaderField
	for _, block := range blocks {
		got = decodeAll(t, d, unhex(t, block))
	}
	if !reflect.DeepEqual(got, last) {
		t.Fatalf("got %v, want %v", got, last)
	}
	if d.table.size != 215 {
		t.Fatalf("table size %d, want 215", d.table.size)
	}
}

func TestRoundTrip(t *testing.T) {
	e := NewEncoder()
	d := NewDecoder(DefaultTableSize)
	rng := rand.New(rand.NewSource(1))
	names := []string{":status", "content-type", "x-custom", "set-cookie", "cache-control", "x-big"}
	for round := 0; round < 200; round++ {
		if round == 50 {
			e.SetMaxTableSize(100)
		}
		if round == 51 {
			e.SetMaxTableSize(DefaultTableSize)
		}
		var want []HeaderField
		block := e.Begin(nil)
		for i := rng.Intn(8); i >= 0; i-- {
			name := names[rng.Intn(len(names))]
			value := strings.Repeat(string(rune('a'+rng.Intn(26))), rng.Intn(40))
			if name == "x-big" {
				value = strings.Repeat("z", 3000)
			}
			want = append(want, HeaderField{name, value})
			block = e.AppendField(block, name, value, name == "set-cookie" && round%2 == 0)
		}
		got := decodeAll(t, d, block)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: got %v, want %v", round, got, want)
		}
	}
}

func TestHuffmanRoundTrip(t *testing.T) {
	var all []byte
	for i := 0; i < 256; i++ {
		all = append(all, byte(i))
	}
	for _, s := range []string{"", "a", "www.example.com", "no-cache", string(all)} {
		encoded := huffmanAppend(nil, s)
		if len(encoded) != huffmanLen(s) {
			t.Fatalf("%q: length %d, want %d", s, len(encoded), huffmanLen(s))
		}
		got, err := huffmanDecode(encoded, 0)
		if err != nil || got != s {
			t.Fatalf("%q: got %q, %v", s, got, err)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	cases := map[string][]byte{
		"index zero":          {0x80},
		"index past table":    {0xff, 0x00},
		"truncated string":    {0x40, 0x05, 'a'},
		"long padding":        {0x00, 0x81, 'a' | 0x80, 0x82, 0xff, 0xff},
		"size update late":    {0x82, 0x20},
		"size update too big": {0x3f, 0xe2, 0x1f},
	}
	for name, block := range cases {
		d := NewDecoder(DefaultTableSize)
		if err := d.Decode(block, func(HeaderField) error { return nil }); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}
