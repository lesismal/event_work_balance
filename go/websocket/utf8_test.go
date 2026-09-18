package websocket

import (
	"errors"
	"testing"
	"unicode/utf8"
)

func TestUTF8ValidatorMatchesStdlibAcrossSplits(t *testing.T) {
	inputs := [][]byte{
		[]byte("hello"),
		[]byte("κόσμε"),
		[]byte("\xf0\x90\x80\x80\xf4\x8f\xbf\xbf"),
		[]byte("\xed\x9f\xbf\xee\x80\x80"),
		[]byte("\xe0\xa0\x80"),
		[]byte("κόσμε\xf4\x90\x80\x80edited"),
		[]byte("\xed\xa0\x80"),
		[]byte("\xe0\x80\xaf"),
		[]byte("\xc0\xaf"),
		[]byte("\xf5\x80\x80\x80"),
		[]byte("\x80"),
		[]byte("ab\xce"),
		[]byte("\xf0\x90\x80"),
	}
	for _, input := range inputs {
		want := utf8.Valid(input)
		for i := 0; i <= len(input); i++ {
			for j := i; j <= len(input); j++ {
				var v utf8Validator
				got := v.write(input[:i]) && v.write(input[i:j]) && v.write(input[j:]) && v.complete()
				if got != want {
					t.Fatalf("%q split at %d,%d: valid = %v, want %v", input, i, j, got, want)
				}
			}
		}
	}
}

// These follow Autobahn cases 6.4.1-6.4.4: the parser must reject invalid
// UTF-8 as soon as it arrives, not when the message or frame completes.
func TestParserFailsFastOnInvalidUTF8(t *testing.T) {
	head, bad, rest := []byte("κόσμε"), []byte("\xf4\x90\x80\x80"), []byte("edited")

	t.Run("fragments", func(t *testing.T) {
		for _, split := range []int{0, 1} {
			parser := NewParser(1 << 10)
			first := append(append([]byte(nil), head...), bad[:split]...)
			if _, err := parser.Feed(clientFrame(Text, false, first)); err != nil {
				t.Fatalf("first fragment: %v", err)
			}
			second := bad[split:]
			if _, err := parser.Feed(clientFrame(Continuation, false, second)); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("second fragment error = %v", err)
			}
		}
	})

	t.Run("chunks", func(t *testing.T) {
		payload := append(append(append([]byte(nil), head...), bad...), rest...)
		for _, borrowed := range []bool{false, true} {
			for _, split := range []int{0, 1} {
				parser := NewParser(1 << 10)
				frame := clientFrame(Text, true, payload)
				headerLen := len(frame) - len(payload)
				cut1 := headerLen + len(head) + split
				cut2 := headerLen + len(head) + len(bad)
				feed := func(data []byte) error {
					if borrowed {
						_, _, err := parser.FeedOneBorrowed(data)
						parser.ReleaseBorrowed()
						return err
					}
					_, err := parser.Feed(data)
					return err
				}
				if err := feed(frame[:cut1]); err != nil {
					t.Fatalf("first chunk: %v", err)
				}
				if err := feed(frame[cut1:cut2]); !errors.Is(err, ErrInvalidPayload) {
					t.Fatalf("borrowed=%v split=%d: second chunk error = %v", borrowed, split, err)
				}
			}
		}
	})

	t.Run("valid chunks", func(t *testing.T) {
		payload := []byte("κόσμε\xf0\x9f\x98\x80κόσμε")
		frame := clientFrame(Text, true, payload)
		parser := NewParser(1 << 10)
		var events []Event
		for i := range frame {
			got, err := parser.Feed(frame[i : i+1])
			if err != nil {
				t.Fatalf("byte %d: %v", i, err)
			}
			events = append(events, got...)
		}
		if len(events) != 1 || string(events[0].Payload) != string(payload) {
			t.Fatalf("events = %+v", events)
		}
	})
}
