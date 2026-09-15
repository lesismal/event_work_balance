package websocket

import "testing"

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
