//go:build linux || darwin || windows

package http3

import (
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/http3/internal/qpack"
	"github.com/lesismal/fib/go/http3/internal/quic"
	"github.com/lesismal/fib/go/internal/tlstest"
)

// rawConn is a bare QUIC connection to the server, for sending what a
// well-behaved client never would.
type rawConn struct {
	mu        sync.Mutex
	qc        *quic.Conn
	handshake chan struct{}
	closed    chan error
	resets    chan uint64
}

func (r *rawConn) OnOpen(*fib.Connection)                 {}
func (r *rawConn) OnPriorityData(*fib.Connection, []byte) {}
func (r *rawConn) OnData(_ *fib.Connection, data []byte) {
	r.mu.Lock()
	qc := r.qc
	r.mu.Unlock()
	if qc != nil {
		qc.HandleDatagram(data)
	}
}
func (r *rawConn) OnClose(*fib.Connection, error) {}

type rawHandler struct{ r *rawConn }

func (h rawHandler) OnHandshake(*quic.Conn)                  { close(h.r.handshake) }
func (h rawHandler) OnStreamData(*quic.Stream, []byte, bool) {}
func (h rawHandler) OnStreamReset(_ *quic.Stream, code uint64) {
	h.r.resets <- code
}
func (h rawHandler) OnStopSending(*quic.Stream, uint64) {}
func (h rawHandler) OnStreamsAvailable(*quic.Conn)      {}
func (h rawHandler) OnClose(_ *quic.Conn, err error)    { h.r.closed <- err }

func dialRaw(t *testing.T, base string) *rawConn {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	r := &rawConn{handshake: make(chan struct{}), closed: make(chan error, 1), resets: make(chan uint64, 16)}
	dialed := make(chan error, 1)
	err = engine.DialWithHandler("udp", "127.0.0.1:"+u.Port(), time.Second, r, func(fc *fib.Connection, err error) {
		if err != nil {
			dialed <- err
			return
		}
		r.mu.Lock()
		r.qc, err = quic.Dial(fc, fc.RemoteAddr(), quic.Config{TLSConfig: clientTLS}, rawHandler{r})
		r.mu.Unlock()
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.handshake:
	case <-time.After(5 * time.Second):
		t.Fatal("no handshake")
	}
	return r
}

// expectClose waits for the server to close the connection with code.
func (r *rawConn) expectClose(t *testing.T, code ErrorCode) {
	t.Helper()
	select {
	case err := <-r.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || ErrorCode(appErr.Code) != code {
			t.Fatalf("closed with %v, want %v", err, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("not closed, want %v", code)
	}
}

func headersFrame(fields ...qpack.HeaderField) []byte {
	block := append([]byte(nil), qpack.Prefix...)
	for _, f := range fields {
		block = qpack.AppendField(block, f.Name, f.Value, false)
	}
	return appendHeadersFrame(nil, block)
}

func TestDataBeforeHeaders(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(append(appendFrameHeader(nil, frameData, 2), 'h', 'i'), true)
	r.expectClose(t, ErrCodeFrameUnexpected)
}

func TestControlStreamWithoutSettings(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := quic.AppendVarint(nil, 0)
	frame := append(appendFrameHeader(nil, frameGoAway, len(payload)), payload...)
	_ = s.Write(append([]byte{streamControl}, frame...), false)
	r.expectClose(t, ErrCodeMissingSettings)
}

func TestSecondControlStream(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	for i := 0; i < 2; i++ {
		s, err := r.qc.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write(appendSettings([]byte{streamControl}), false)
	}
	r.expectClose(t, ErrCodeStreamCreationError)
}

func TestMalformedRequestIsReset(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// No :path.
	_ = s.Write(headersFrame(
		qpack.HeaderField{Name: ":method", Value: "GET"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
	), true)
	select {
	case code := <-r.resets:
		if ErrorCode(code) != ErrCodeMessageError {
			t.Fatalf("reset with %v", ErrorCode(code))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not reset")
	}
}

func TestDynamicTableReferenceFails(t *testing.T) {
	r := dialRaw(t, startServer(t, Config{}, echo))
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// A Required Insert Count of one refers to a table the server never
	// allowed.
	block := []byte{0x02, 0x00, 0x80}
	_ = s.Write(appendHeadersFrame(nil, block), true)
	r.expectClose(t, ErrCodeQPACKDecompressionFailed)
}
