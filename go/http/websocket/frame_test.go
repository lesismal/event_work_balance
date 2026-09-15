package websocket

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func clientFrame(opcode Opcode, fin bool, payload []byte) []byte {
	first := byte(opcode)
	if fin {
		first |= 0x80
	}
	mask := [4]byte{1, 2, 3, 4}
	header := []byte{first, 0x80}
	if len(payload) < 126 {
		header[1] |= byte(len(payload))
	} else {
		header[1] |= 126
		header = append(header, byte(len(payload)>>8), byte(len(payload)))
	}
	frame := append(header, mask[:]...)
	for i, value := range payload {
		frame = append(frame, value^mask[i&3])
	}
	return frame
}

func TestParserFragmentedMessageAndPing(t *testing.T) {
	parser := NewParser(1024)
	data := append(clientFrame(Text, false, []byte("hel")), clientFrame(Ping, true, []byte("?"))...)
	data = append(data, clientFrame(Continuation, true, []byte("lo"))...)
	var events []Event
	for _, chunk := range [][]byte{data[:3], data[3:9], data[9:]} {
		got, err := parser.Feed(chunk)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, got...)
	}
	if len(events) != 2 || events[0].Opcode != Ping || string(events[0].Payload) != "?" ||
		events[1].Opcode != Text || string(events[1].Payload) != "hello" {
		t.Fatalf("events = %#v", events)
	}
}

func TestParserRejectsProtocolViolations(t *testing.T) {
	t.Run("unmasked", func(t *testing.T) {
		parser := NewParser(100)
		_, err := parser.Feed([]byte{0x81, 0x01, 'x'})
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("large", func(t *testing.T) {
		parser := NewParser(3)
		_, err := parser.Feed(clientFrame(Binary, true, []byte("four")))
		if !errors.Is(err, ErrMessageTooBig) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("utf8", func(t *testing.T) {
		parser := NewParser(100)
		_, err := parser.Feed(clientFrame(Text, true, []byte{0xff}))
		if !errors.Is(err, ErrInvalidPayload) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestMarshalFrameLengths(t *testing.T) {
	for _, size := range []int{0, 125, 126, 65535, 65536} {
		payload := bytes.Repeat([]byte{'x'}, size)
		frame, err := MarshalFrame(Binary, payload)
		if err != nil {
			t.Fatal(err)
		}
		if frame[0] != 0x82 || frame[1]&0x80 != 0 {
			t.Fatalf("invalid header for size %d", size)
		}
		var got uint64
		switch frame[1] {
		case 126:
			got = uint64(binary.BigEndian.Uint16(frame[2:4]))
		case 127:
			got = binary.BigEndian.Uint64(frame[2:10])
		default:
			got = uint64(frame[1])
		}
		if got != uint64(size) {
			t.Fatalf("encoded length = %d, want %d", got, size)
		}
	}
}
