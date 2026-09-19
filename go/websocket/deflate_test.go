package websocket

import (
	"bytes"
	"compress/flate"
	"errors"
	"strconv"
	"testing"
)

func TestAcceptDeflateOffer(t *testing.T) {
	tests := []struct {
		offers   []string
		response string
		params   deflateParams
	}{
		{nil, "", deflateParams{}},
		{[]string{"x-webkit-deflate-frame"}, "", deflateParams{}},
		{[]string{"permessage-deflate"},
			"permessage-deflate; server_no_context_takeover", deflateParams{15, true}},
		{[]string{"permessage-deflate; client_max_window_bits"},
			"permessage-deflate; server_no_context_takeover", deflateParams{15, true}},
		{[]string{"permessage-deflate; client_no_context_takeover; client_max_window_bits"},
			"permessage-deflate; server_no_context_takeover; client_no_context_takeover", deflateParams{15, false}},
		{[]string{`permessage-deflate; server_no_context_takeover; server_max_window_bits="9"`},
			"permessage-deflate; server_no_context_takeover; server_max_window_bits=9", deflateParams{9, true}},
		// An offer the server cannot take is skipped for the next one.
		{[]string{"permessage-deflate; server_max_window_bits=7, permessage-deflate; client_no_context_takeover"},
			"permessage-deflate; server_no_context_takeover; client_no_context_takeover", deflateParams{15, false}},
		{[]string{"foo, permessage-deflate; server_max_window_bits=10", "permessage-deflate"},
			"permessage-deflate; server_no_context_takeover; server_max_window_bits=10", deflateParams{10, true}},
		{[]string{"permessage-deflate; server_max_window_bits"}, "", deflateParams{}},
		{[]string{"permessage-deflate; server_max_window_bits=09"}, "", deflateParams{}},
		{[]string{"permessage-deflate; server_max_window_bits=16"}, "", deflateParams{}},
		{[]string{"permessage-deflate; client_max_window_bits=20"}, "", deflateParams{}},
		{[]string{"permessage-deflate; server_no_context_takeover=1"}, "", deflateParams{}},
		{[]string{"permessage-deflate; server_no_context_takeover; server_no_context_takeover"}, "", deflateParams{}},
		{[]string{"permessage-deflate; unknown"}, "", deflateParams{}},
		{[]string{"permessage-deflate;"}, "", deflateParams{}},
		{[]string{"permessage-deflate,"}, "", deflateParams{}},
		{[]string{`permessage-deflate; server_max_window_bits="9`}, "", deflateParams{}},
	}
	for _, test := range tests {
		response, params := acceptDeflateOffer(test.offers)
		if response != test.response || params != test.params {
			t.Errorf("%q: got %q %+v, want %q %+v", test.offers, response, params, test.response, test.params)
		}
	}
}

func TestAcceptDeflateResponse(t *testing.T) {
	tests := []struct {
		response []string
		params   deflateParams
		enabled  bool
		ok       bool
	}{
		{nil, deflateParams{}, false, true},
		{[]string{"permessage-deflate"}, deflateParams{15, true}, true, true},
		{[]string{"permessage-deflate; server_no_context_takeover; client_no_context_takeover"},
			deflateParams{15, false}, true, true},
		{[]string{"permessage-deflate; client_max_window_bits=10; server_max_window_bits=12"},
			deflateParams{10, true}, true, true},
		{[]string{"permessage-deflate; client_max_window_bits"}, deflateParams{}, false, false},
		{[]string{"permessage-deflate; server_max_window_bits=7"}, deflateParams{}, false, false},
		{[]string{"permessage-deflate; foo"}, deflateParams{}, false, false},
		{[]string{"permessage-deflate", "permessage-deflate"}, deflateParams{}, false, false},
		{[]string{"x-other"}, deflateParams{}, false, false},
	}
	for _, test := range tests {
		params, enabled, ok := acceptDeflateResponse(test.response)
		if params != test.params || enabled != test.enabled || ok != test.ok {
			t.Errorf("%q: got %+v %v %v, want %+v %v %v", test.response, params, enabled, ok,
				test.params, test.enabled, test.ok)
		}
	}
}

func compressiblePayload(size int) []byte {
	payload := make([]byte, 0, size+32)
	for i := 0; len(payload) < size; i++ {
		payload = append(payload, strconv.Itoa(i%977)...)
		payload = append(payload, " lorem ipsum "...)
	}
	return payload[:size]
}

func TestCompressMessageRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 255, 256, 257, 4096, 100000} {
		payload := compressiblePayload(size)
		for bits := minWindowBits; bits <= maxWindowBits; bits++ {
			c, compressed := compressMessage(payload, bits)
			got, err := decompressMessage(nil, compressed, nil, int64(size))
			c.release()
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("size %d window %d: round trip err=%v equal=%v", size, bits, err, bytes.Equal(got, payload))
			}
		}
	}
}

// deflateFrame builds a masked compressed client frame, with RSV1 set when
// first is true.
func deflateFrame(opcode Opcode, fin, first bool, payload []byte) []byte {
	frame := clientFrame(opcode, fin, payload)
	if first {
		frame[0] |= 0x40
	}
	return frame
}

func compressed(t *testing.T, payload []byte) []byte {
	t.Helper()
	c, data := compressMessage(payload, maxWindowBits)
	defer c.release()
	return append([]byte(nil), data...)
}

func deflateParser(max int64) *Parser {
	p := NewParser(max)
	p.deflate = true
	return p
}

func TestParserDecompresses(t *testing.T) {
	payload := compressiblePayload(5000)
	data := compressed(t, payload)

	t.Run("single frame", func(t *testing.T) {
		for _, borrowed := range []bool{false, true} {
			parser := deflateParser(1 << 20)
			var event Event
			var complete bool
			var err error
			if borrowed {
				event, complete, err = parser.FeedOneBorrowed(deflateFrame(Text, true, true, data))
			} else {
				event, complete, err = parser.FeedOne(deflateFrame(Text, true, true, data))
			}
			if err != nil || !complete || event.Opcode != Text || !bytes.Equal(event.Payload, payload) {
				t.Fatalf("borrowed=%v: complete=%v err=%v", borrowed, complete, err)
			}
			parser.ReleaseBorrowed()
		}
	})

	t.Run("fragments and interleaved ping", func(t *testing.T) {
		parser := deflateParser(1 << 20)
		third := len(data) / 3
		var stream []byte
		stream = append(stream, deflateFrame(Binary, false, true, data[:third])...)
		stream = append(stream, clientFrame(Ping, true, []byte("p"))...)
		stream = append(stream, clientFrame(Continuation, false, data[third:2*third])...)
		stream = append(stream, clientFrame(Continuation, true, data[2*third:])...)
		stream = append(stream, clientFrame(Binary, true, []byte("plain"))...)
		events, err := parser.Feed(stream)
		if err != nil || len(events) != 3 {
			t.Fatalf("events=%d err=%v", len(events), err)
		}
		if events[0].Opcode != Ping || events[1].Opcode != Binary || !bytes.Equal(events[1].Payload, payload) ||
			string(events[2].Payload) != "plain" {
			t.Fatalf("unexpected events %v %v %q", events[0].Opcode, events[1].Opcode, events[2].Payload)
		}
	})

	t.Run("context takeover", func(t *testing.T) {
		// A peer that keeps its context refers back into earlier messages.
		var buf bytes.Buffer
		w, _ := flate.NewWriter(&buf, flate.BestCompression)
		parser := deflateParser(1 << 20)
		parser.contextTakeover = true
		for i, message := range [][]byte{payload, payload, compressiblePayload(40000), payload} {
			buf.Reset()
			_, _ = w.Write(message)
			_ = w.Flush()
			wire := bytes.TrimSuffix(buf.Bytes(), []byte{0, 0, 0xff, 0xff})
			if i == 1 && len(wire) > 100 {
				t.Fatalf("second copy compressed to %d bytes, so it does not refer back", len(wire))
			}
			event, complete, err := parser.FeedOneBorrowed(deflateFrame(Binary, true, true, wire))
			if err != nil || !complete || !bytes.Equal(event.Payload, message) {
				t.Fatalf("message %d: complete=%v err=%v", i, complete, err)
			}
			parser.ReleaseBorrowed()
		}
	})
}

func TestParserRejectsBadCompressedFrames(t *testing.T) {
	data := compressed(t, []byte("hello"))
	tests := []struct {
		name   string
		parser *Parser
		stream []byte
		want   error
	}{
		{"not negotiated", NewParser(1 << 10), deflateFrame(Text, true, true, data), ErrProtocol},
		{"on control frame", deflateParser(1 << 10), deflateFrame(Ping, true, true, nil), ErrProtocol},
		{"on continuation", deflateParser(1 << 10),
			append(deflateFrame(Text, false, true, data[:2]), deflateFrame(Continuation, true, true, data[2:])...),
			ErrProtocol},
		{"rsv2", deflateParser(1 << 10), func() []byte {
			frame := clientFrame(Text, true, data)
			frame[0] |= 0x20
			return frame
		}(), ErrProtocol},
		{"corrupt", deflateParser(1 << 10), deflateFrame(Binary, true, true, []byte{0xff, 0xff, 0xff}), ErrInvalidPayload},
		{"invalid utf8", deflateParser(1 << 10), deflateFrame(Text, true, true, compressed(t, []byte{0xce, 0xba, 0xff})),
			ErrInvalidPayload},
		{"too big once inflated", deflateParser(1 << 10),
			deflateFrame(Binary, true, true, compressed(t, make([]byte, 1<<11))), ErrMessageTooBig},
	}
	for _, test := range tests {
		if _, err := test.parser.Feed(test.stream); !errors.Is(err, test.want) {
			t.Errorf("%s: error = %v, want %v", test.name, err, test.want)
		}
	}
}

func BenchmarkCompressMessage(b *testing.B) {
	payload := compressiblePayload(4096)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		c, _ := compressMessage(payload, maxWindowBits)
		c.release()
	}
}

func BenchmarkDecompressMessage(b *testing.B) {
	payload := compressiblePayload(4096)
	c, data := compressMessage(payload, maxWindowBits)
	data = append([]byte(nil), data...)
	c.release()
	dst := make([]byte, 0, len(payload))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := decompressMessage(dst, data, nil, 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
