//go:build linux || darwin || windows

package http

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib/go"
)

// ErrResponseWritten is what writing to a response returns once the response
// has been sent whole, by WriteResponse, Respond or Finish.
var ErrResponseWritten = errors.New("http: response already written")

const (
	// writerBufferSize is how much body the ResponseWriter methods hold back
	// before sending the header. A handler that writes no more than this and
	// returns gets a Content-Length response rather than a chunked one.
	writerBufferSize = 4 << 10
	// minSendFileSize is the smallest file range ReadFrom hands to sendfile.
	// Below it, copying the bytes costs less than the extra system calls.
	minSendFileSize = 16 << 10
)

// responseWriter is a response written through Context's ResponseWriter
// methods, as net/http's handlers write theirs.
type responseWriter struct {
	// header is what Header returns, and sent the copy WriteHeader took of
	// it, which is what goes out: later changes only count for trailers.
	header stdhttp.Header
	sent   stdhttp.Header
	status int
	// declared is the Content-Length the handler set, or -1, and written how
	// much body it has written so far.
	declared int64
	written  int64
	// buf holds body not yet sent: on HTTP/1 until the header goes out, and
	// on HTTP/2 and HTTP/3 until the handler is done.
	buf []byte
	// committed records that the HTTP/1 header has been sent, and chunked
	// and closeAfter how its body is framed and whether the connection ends
	// with it. trailers are the names the header announced.
	committed  bool
	chunked    bool
	closeAfter bool
	trailers   []string
	finished   bool
}

// Header returns the header of the response written through Write, as
// http.ResponseWriter's does. Changing it after WriteHeader or the first
// Write has no effect, except for trailers: values for names announced in a
// "Trailer" header, and keys prefixed with http.TrailerPrefix, are sent
// after the body.
func (c *Context) Header() stdhttp.Header {
	w := c.writer()
	if w.header == nil {
		w.header = make(stdhttp.Header)
	}
	return w.header
}

func (c *Context) writer() *responseWriter {
	if c.w == nil {
		c.w = &responseWriter{declared: -1}
	}
	return c.w
}

// WriteHeader sends the response's status with the header Header returned,
// as http.ResponseWriter's does. A 1xx status other than 101 is sent at once
// as an interim response, and may be followed by others; the final status
// may only be set once, and Write sets 200 if it has not been set.
//
// On HTTP/1 the header goes out with the first part of the body. A handler
// whose body is no longer than a few kilobytes, or that set Content-Length
// itself, gets a response with Content-Length; a longer one is chunked, which
// is also what carries trailers. An HTTP/1.0 client, which does not know
// chunked framing, gets a body that ends when the connection closes.
//
// On HTTP/2 and HTTP/3 the whole response is held until the handler returns
// and then sent at once, trailers included.
func (c *Context) WriteHeader(status int) {
	if status < 100 || status > 999 {
		panic(fmt.Sprintf("http: invalid WriteHeader code %d", status))
	}
	w := c.writer()
	if c.wrote || w.status != 0 {
		return
	}
	if status < 200 && status != stdhttp.StatusSwitchingProtocols {
		_ = c.WriteInterim(status, w.header)
		return
	}
	w.status = status
	w.sent = w.header.Clone()
	if w.sent == nil {
		w.sent = make(stdhttp.Header)
	}
	if values := w.sent["Content-Length"]; len(values) == 1 {
		if n, err := strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64); err == nil && n >= 0 {
			w.declared = n
		}
	}
	if w.declared < 0 {
		delete(w.sent, "Content-Length")
	}
}

// Write adds p to the response body, as http.ResponseWriter's does, sending
// the header first if it has not gone yet. A response to HEAD counts what is
// written but sends none of it.
func (c *Context) Write(p []byte) (int, error) {
	if c.wrote {
		return 0, ErrResponseWritten
	}
	w := c.writer()
	if w.status == 0 {
		c.WriteHeader(stdhttp.StatusOK)
	}
	if w.finished {
		return 0, ErrResponseWritten
	}
	if !statusHasBody(w.status) {
		return 0, stdhttp.ErrBodyNotAllowed
	}
	if w.declared >= 0 && w.written+int64(len(p)) > w.declared {
		return 0, stdhttp.ErrContentLength
	}
	w.written += int64(len(p))
	if c.Request.Method == stdhttp.MethodHead || len(p) == 0 {
		return len(p), nil
	}
	if !c.isHTTP1() || !w.committed && len(w.buf)+len(p) <= writerBufferSize {
		w.buf = append(w.buf, p...)
		return len(p), nil
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return 0, err
		}
	}
	if err := c.sendBody(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// WriteString is Write for a string.
func (c *Context) WriteString(s string) (int, error) { return c.Write([]byte(s)) }

// Flush sends the header and whatever body is held back, as http.Flusher's
// does, and hands it to the socket at once rather than when the handler
// returns. It does nothing on HTTP/2 and HTTP/3, whose responses go out
// whole.
func (c *Context) Flush() { _ = c.FlushError() }

// FlushError is Flush reporting what went wrong, which is what
// http.ResponseController looks for.
func (c *Context) FlushError() error {
	if c.wrote {
		return ErrResponseWritten
	}
	w := c.writer()
	if w.status == 0 {
		c.WriteHeader(stdhttp.StatusOK)
	}
	if w.finished || !c.isHTTP1() {
		return nil
	}
	if !w.committed {
		return c.commit(false)
	}
	return c.Conn.Flush()
}

// ReadFrom copies src into the response body, as io.Copy does through
// io.ReaderFrom. On an HTTP/1 connection a regular file, or an
// *io.LimitedReader over one as http.ServeContent passes, goes from the file
// to the socket by sendfile(2) where the platform has it, without being
// read into memory; so http.ServeFile and http.ServeContent, given the
// Context as their ResponseWriter, serve files that way. src is left
// positioned after what was sent.
func (c *Context) ReadFrom(src io.Reader) (int64, error) {
	if c.wrote {
		return 0, ErrResponseWritten
	}
	w := c.writer()
	file, ok := sendableFileOf(src)
	if !ok || !c.isHTTP1() || c.Request.Method == stdhttp.MethodHead || w.finished ||
		w.status != 0 && !statusHasBody(w.status) || file.size < minSendFileSize ||
		w.declared >= 0 && w.written+file.size > w.declared {
		return io.Copy(writerOnly{c}, src)
	}
	if w.status == 0 {
		if _, set := w.header["Content-Type"]; !set && w.written == 0 {
			// Sniff the type as a first Write would have.
			sniff := make([]byte, 512)
			n, _ := file.f.ReadAt(sniff, file.offset)
			c.Header().Set("Content-Type", stdhttp.DetectContentType(sniff[:n]))
		}
		c.WriteHeader(stdhttp.StatusOK)
	}
	if !w.committed {
		if err := c.commit(false); err != nil {
			return 0, err
		}
	}
	if w.chunked {
		if err := c.Conn.SendOwned(fmt.Appendf(nil, "%x\r\n", file.size)); err != nil {
			return 0, err
		}
	}
	if err := c.Conn.SendFile(file.f, file.offset, file.size); err != nil {
		return 0, err
	}
	if w.chunked {
		if err := c.Conn.Send(crlf); err != nil {
			return 0, err
		}
	}
	w.written += file.size
	file.advance()
	return file.size, nil
}

// writerOnly hides a Context's ReadFrom from io.Copy, which would otherwise
// call it back.
type writerOnly struct{ io.Writer }

// sendableFile is a stretch of a regular file ReadFrom can hand to SendFile.
type sendableFile struct {
	f       fileLike
	offset  int64
	size    int64
	limited *io.LimitedReader
}

// fileLike is an *os.File, or a type wrapping one, such as the one
// os.File.WriteTo passes to io.Copy.
type fileLike interface {
	fib.File
	io.Seeker
	Stat() (fs.FileInfo, error)
}

func sendableFileOf(src io.Reader) (sendableFile, bool) {
	limit := int64(-1)
	limited, _ := src.(*io.LimitedReader)
	if limited != nil {
		src, limit = limited.R, limited.N
	}
	f, ok := src.(fileLike)
	if !ok {
		return sendableFile{}, false
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return sendableFile{}, false
	}
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return sendableFile{}, false
	}
	size := max(info.Size()-offset, 0)
	if limit >= 0 {
		size = min(size, limit)
	}
	return sendableFile{f: f, offset: offset, size: size, limited: limited}, true
}

// advance moves the source past what was sent, as reading it would have.
func (s sendableFile) advance() {
	_, _ = s.f.Seek(s.offset+s.size, io.SeekStart)
	if s.limited != nil {
		s.limited.N -= s.size
	}
}

var crlf = []byte("\r\n")

// isHTTP1 reports whether the response goes straight onto an HTTP/1
// connection, rather than onto a stream of a multiplexed one.
func (c *Context) isHTTP1() bool { return c.stream == nil && c.external == nil }

// statusHasBody reports whether a response with status may carry a body.
func statusHasBody(status int) bool {
	return status >= 200 && status != stdhttp.StatusNoContent && status != stdhttp.StatusNotModified
}

// commit sends the HTTP/1 header, with whatever body is held back. final
// says the handler is done, so that a body held back whole can be sent with
// its length.
func (c *Context) commit(final bool) error {
	w := c.w
	req := c.Request
	head := responseHead{status: w.status, header: w.sent, contentLength: -1}
	head.close = c.Request.Close || headerHasToken(w.sent, "Connection", "close")
	http11 := req.ProtoAtLeast(1, 1)
	w.trailers = announcedTrailers(w.sent)
	w.sniff()
	switch {
	case !statusHasBody(w.status):
		if w.status == stdhttp.StatusNotModified && w.declared >= 0 {
			head.contentLength = w.declared
		}
	case w.declared >= 0:
		head.contentLength = w.declared
	case final && (len(w.trailers) == 0 && !hasPrefixedTrailers(w.header) || !http11):
		if req.Method != stdhttp.MethodHead || w.written > 0 {
			head.contentLength = w.written
		}
	case req.Method == stdhttp.MethodHead:
	case http11:
		head.chunked = true
		head.trailers = w.trailers
	default:
		// HTTP/1.0 has no chunks: the body runs until the connection closes.
		head.close = true
	}
	out := make([]byte, 0, 256+len(w.buf))
	out, err := appendResponseHead(out, req, head)
	if err != nil {
		return err
	}
	w.committed = true
	w.chunked = head.chunked
	w.closeAfter = head.close
	c.closing = head.close
	if len(w.buf) > 0 {
		if w.chunked {
			out = appendChunk(out, w.buf)
		} else {
			out = append(out, w.buf...)
		}
	}
	w.buf = nil
	if err := c.Conn.SendOwned(out); err != nil {
		return err
	}
	if final {
		// The whole response is out, and goes to the socket with whatever
		// else this read round answers.
		return nil
	}
	// A body too long to hold back is streamed: from here on it goes to the
	// socket as it is written, rather than piling up until the handler
	// returns.
	return c.Conn.Flush()
}

// sendBody sends part of a committed HTTP/1 body.
func (c *Context) sendBody(p []byte) error {
	if !c.w.chunked {
		return c.Conn.Send(p)
	}
	return c.Conn.SendOwned(appendChunk(make([]byte, 0, len(p)+20), p))
}

func appendChunk(out, p []byte) []byte {
	out = strconv.AppendInt(out, int64(len(p)), 16)
	out = append(out, crlf...)
	out = append(out, p...)
	return append(out, crlf...)
}

// Finish ends a response written through Write: it sends the header if it
// has not gone yet, what body is held back, and the trailers. The server
// calls it when the handler returns, as net/http ends a response then, so a
// handler only calls it to end the response sooner. A response that was never
// begun is left alone, since the handler may answer it later with
// WriteResponse.
func (c *Context) Finish() error {
	w := c.w
	if w == nil || w.status == 0 || w.finished || c.wrote {
		return nil
	}
	w.finished = true
	if !c.isHTTP1() {
		w.sniff()
		response := Response{StatusCode: w.status, Header: w.sent, Body: w.buf, Trailer: w.trailer()}
		if w.declared >= 0 {
			// The stream's own framing reports the length.
			delete(response.Header, "Content-Length")
		}
		return c.writeResponse(response)
	}
	if !w.committed {
		if err := c.commit(true); err != nil {
			return err
		}
	}
	if w.chunked {
		out := append([]byte(nil), "0\r\n"...)
		out, err := appendHeaderLines(out, w.trailer(), nil)
		if err != nil {
			// The body is already out; end it without the bad trailer.
			out = append(out[:0], "0\r\n"...)
		}
		if err := c.Conn.SendOwned(append(out, crlf...)); err != nil {
			return err
		}
	}
	c.wrote = true
	if w.closeAfter || w.declared >= 0 && w.written < w.declared && c.Request.Method != stdhttp.MethodHead &&
		statusHasBody(w.status) {
		// A body shorter than its Content-Length leaves the client waiting
		// for bytes that will never come, so the connection has to end.
		c.closing = true
		c.Conn.CloseAfterSend()
	}
	return nil
}

// sniff sets the Content-Type of a response whose handler set none from the
// body it holds back, as net/http does.
func (w *responseWriter) sniff() {
	if _, set := w.sent["Content-Type"]; !set && len(w.buf) > 0 && statusHasBody(w.status) {
		w.sent.Set("Content-Type", stdhttp.DetectContentType(w.buf[:min(len(w.buf), 512)]))
	}
}

// trailer gathers the trailers the handler set: values for the names its
// header announced, and keys it prefixed with http.TrailerPrefix.
func (w *responseWriter) trailer() stdhttp.Header {
	var trailer stdhttp.Header
	add := func(key string, values []string) {
		if trailer == nil {
			trailer = make(stdhttp.Header)
		}
		trailer[key] = append(trailer[key], values...)
	}
	names := w.trailers
	if !w.committed {
		names = announcedTrailers(w.sent)
	}
	for _, name := range names {
		if values := w.header[name]; len(values) > 0 {
			add(name, values)
		}
	}
	for key, values := range w.header {
		if name := strings.TrimPrefix(key, stdhttp.TrailerPrefix); name != key && !forbiddenTrailer(name) {
			add(stdhttp.CanonicalHeaderKey(name), values)
		}
	}
	return trailer
}

// announcedTrailers lists the names a "Trailer" header announces.
func announcedTrailers(header stdhttp.Header) []string {
	var names []string
	for _, value := range header["Trailer"] {
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" && !forbiddenTrailer(name) {
				names = append(names, stdhttp.CanonicalHeaderKey(name))
			}
		}
	}
	return names
}

func hasPrefixedTrailers(header stdhttp.Header) bool {
	for key := range header {
		if strings.HasPrefix(key, stdhttp.TrailerPrefix) {
			return true
		}
	}
	return false
}

// forbiddenTrailer reports fields a trailer may not carry, since they frame,
// route or authenticate the message (RFC 9110 section 6.5.1).
func forbiddenTrailer(name string) bool {
	switch stdhttp.CanonicalHeaderKey(name) {
	case "Content-Length", "Transfer-Encoding", "Trailer", "Host", "Content-Type", "Content-Encoding",
		"Content-Range", "Cache-Control", "Expect", "Max-Forwards", "Pragma", "Range", "Te",
		"Authorization", "Set-Cookie", "Connection", "Keep-Alive", "Upgrade":
		return true
	}
	return false
}

// responseHead is how an HTTP/1 response's header frames it.
type responseHead struct {
	status int
	header stdhttp.Header
	// contentLength is sent unless it is negative. chunked sends
	// Transfer-Encoding: chunked, with trailers announced in a Trailer
	// header. close says the connection ends after the response.
	contentLength int64
	chunked       bool
	trailers      []string
	close         bool
}

// appendResponseHead appends the status line and header of an HTTP/1
// response to req. The framing fields it sets itself replace any the header
// holds, and a Date is added unless the header has one.
func appendResponseHead(out []byte, req *stdhttp.Request, head responseHead) ([]byte, error) {
	if head.status < 100 || head.status > 999 {
		return nil, fmt.Errorf("http: invalid status code %d", head.status)
	}
	http10 := req.ProtoMajor == 1 && req.ProtoMinor == 0
	connection := ""
	switch {
	case head.close && !http10:
		connection = "close"
	case !head.close && http10:
		connection = "keep-alive"
	}
	statusText := stdhttp.StatusText(head.status)
	if statusText == "" {
		statusText = "Status"
	}
	if http10 {
		out = append(out, "HTTP/1.0 "...)
	} else {
		out = append(out, "HTTP/1.1 "...)
	}
	out = strconv.AppendInt(out, int64(head.status), 10)
	out = append(out, ' ')
	out = append(out, statusText...)
	out = append(out, crlf...)
	if _, ok := head.header["Date"]; !ok {
		out = appendDate(out)
	}
	if head.contentLength >= 0 {
		out = append(out, "Content-Length: "...)
		out = strconv.AppendInt(out, head.contentLength, 10)
		out = append(out, crlf...)
	}
	if head.chunked {
		out = append(out, "Transfer-Encoding: chunked\r\n"...)
		if len(head.trailers) > 0 {
			out = append(out, "Trailer: "...)
			out = append(out, strings.Join(head.trailers, ", ")...)
			out = append(out, crlf...)
		}
	}
	if connection != "" {
		out = append(out, "Connection: "...)
		out = append(out, connection...)
		out = append(out, crlf...)
	}
	out, err := appendHeaderLines(out, head.header, func(key string) bool {
		switch {
		case strings.EqualFold(key, "Content-Length"), strings.EqualFold(key, "Transfer-Encoding"),
			strings.EqualFold(key, "Trailer"), strings.HasPrefix(key, stdhttp.TrailerPrefix):
			return true
		case strings.EqualFold(key, "Connection"):
			return connection != ""
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return append(out, crlf...), nil
}

// cachedDate is the Date header value for one second, formatted once.
type cachedDate struct {
	second int64
	value  []byte
}

var dateCache atomic.Pointer[cachedDate]

// appendDate appends a Date header for now, which RFC 9110 section 6.6.1 asks
// an origin server with a clock to send.
func appendDate(out []byte) []byte {
	now := time.Now()
	second := now.Unix()
	date := dateCache.Load()
	if date == nil || date.second != second {
		date = &cachedDate{second: second, value: now.UTC().AppendFormat(nil, stdhttp.TimeFormat)}
		dateCache.Store(date)
	}
	out = append(out, "Date: "...)
	out = append(out, date.value...)
	return append(out, crlf...)
}

var (
	_ stdhttp.ResponseWriter = (*Context)(nil)
	_ stdhttp.Flusher        = (*Context)(nil)
	_ io.ReaderFrom          = (*Context)(nil)
	_ io.StringWriter        = (*Context)(nil)
)
