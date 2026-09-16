package websocket

import (
	"bytes"
	"testing"
)

func BenchmarkParserSmallText(b *testing.B) {
	parser := NewParser(1 << 20)
	frame := clientFrame(Text, true, []byte("hello websocket"))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		events, err := parser.Feed(frame)
		if err != nil || len(events) != 1 {
			b.Fatalf("Feed returned %d events, %v", len(events), err)
		}
	}
}

func BenchmarkParserSmallTextFeedOne(b *testing.B) {
	parser := NewParser(1 << 20)
	frame := clientFrame(Text, true, []byte("hello websocket"))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		_, complete, err := parser.FeedOne(frame)
		if err != nil || !complete {
			b.Fatalf("FeedOne returned complete=%v, %v", complete, err)
		}
	}
}

func BenchmarkParserSmallTextBorrowed(b *testing.B) {
	parser := NewParser(1 << 20)
	frame := clientFrame(Text, true, []byte("hello websocket"))
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for i := 0; i < b.N; i++ {
		_, complete, err := parser.FeedOneBorrowed(frame)
		if err != nil || !complete {
			b.Fatalf("FeedOneBorrowed returned complete=%v, %v", complete, err)
		}
	}
}

func BenchmarkParser1KiBBorrowed(b *testing.B) {
	parser := NewParser(1 << 20)
	frame := clientFrame(Binary, true, make([]byte, 1024))
	b.ReportAllocs()
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		_, complete, err := parser.FeedOneBorrowed(frame)
		if err != nil || !complete {
			b.Fatalf("FeedOneBorrowed returned complete=%v, %v", complete, err)
		}
	}
}

// BenchmarkParserBorrowedSplitFrames mirrors a socket read that does not end on
// a frame boundary, which is the normal case whenever the read buffer size and
// the payload size are unrelated. Every other round leaves a partial frame that
// the parser must take ownership of before the read buffer goes back to its
// pool, so this is the path that dominates a busy server's allocations.
func BenchmarkParserBorrowedSplitFrames(b *testing.B) {
	frame := clientFrame(Binary, true, make([]byte, 1024))
	// Each read carries one and a half frames; two reads span three frames, so
	// the stream repeats on a frame boundary.
	readSize := len(frame) * 3 / 2
	stream := bytes.Repeat(frame, 3)
	parser := NewParser(1 << 20)
	buf := make([]byte, readSize)
	offset := 0
	b.ReportAllocs()
	b.SetBytes(int64(readSize))
	for i := 0; i < b.N; i++ {
		// Copy into a reusable buffer the way drainInput hands one to OnData.
		copy(buf, stream[offset:offset+readSize])
		offset = (offset + readSize) % len(stream)
		data := buf
		for {
			_, complete, err := parser.FeedOneBorrowed(data)
			data = nil
			if err != nil {
				b.Fatal(err)
			}
			if !complete {
				break
			}
		}
		parser.ReleaseBorrowed()
	}
}

func BenchmarkMarshalFrame(b *testing.B) {
	payload := make([]byte, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		if _, err := MarshalFrame(Binary, payload); err != nil {
			b.Fatal(err)
		}
	}
}
