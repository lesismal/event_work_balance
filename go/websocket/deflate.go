package websocket

import (
	"bytes"
	"compress/flate"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// permessage-deflate (RFC 7692) compresses the payload of each data message
// with DEFLATE and marks its first frame with RSV1.
//
// This side never carries its compression context from one message to the
// next, which is always something the peer can decode and lets every send
// borrow a pooled compressor instead of each connection keeping its own. The
// peer may carry its context, so a connection whose peer does keeps the last
// 32 KiB it received as the dictionary for the next message.

const (
	deflateExtension = "permessage-deflate"
	// maxWindowBits is DEFLATE's largest window, 32 KiB, and the one
	// compress/flate always uses.
	maxWindowBits = 15
	minWindowBits = 8
	maxWindowSize = 1 << maxWindowBits
)

// deflateTail is the empty stored block a sender strips from the end of every
// compressed message (RFC 7692 section 7.2.1), followed by an empty final
// block so the decompressor reports the end of the message as io.EOF.
var deflateTail = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

// deflateParams are the permessage-deflate parameters of one direction of a
// connection.
type deflateParams struct {
	// windowBits bounds how far back what this side sends may refer.
	windowBits int
	// peerContextTakeover says the peer compresses each message with the
	// context of the ones before it, so decompressing needs their tail.
	peerContextTakeover bool
}

// extensionParam is one parameter of an extension offer or response.
type extensionParam struct {
	name, value string
	hasValue    bool
}

// parseExtensions splits Sec-WebSocket-Extensions values into extensions, each
// a name and its parameters. ok is false for a value that is not the grammar
// of RFC 6455 section 9.1.
func parseExtensions(values []string) (extensions [][]extensionParam, ok bool) {
	for _, value := range values {
		for len(value) != 0 {
			var extension []extensionParam
			for {
				var param extensionParam
				value = strings.TrimLeft(value, " \t")
				end := strings.IndexAny(value, ",;= \t")
				if end < 0 {
					end = len(value)
				}
				param.name, value = value[:end], strings.TrimLeft(value[end:], " \t")
				if !validToken(param.name) {
					return nil, false
				}
				if len(value) != 0 && value[0] == '=' {
					if len(extension) == 0 {
						return nil, false
					}
					param.hasValue = true
					value = strings.TrimLeft(value[1:], " \t")
					var valid bool
					param.value, value, valid = parseParamValue(value)
					if !valid {
						return nil, false
					}
					value = strings.TrimLeft(value, " \t")
				}
				extension = append(extension, param)
				if len(value) == 0 || value[0] == ',' {
					break
				}
				if value[0] != ';' {
					return nil, false
				}
				value = value[1:]
			}
			extensions = append(extensions, extension)
			if len(value) != 0 {
				value = value[1:]
				if strings.TrimLeft(value, " \t") == "" {
					return nil, false
				}
			}
		}
	}
	return extensions, true
}

// parseParamValue reads a token or a quoted string holding one (RFC 6455
// section 9.1).
func parseParamValue(value string) (string, string, bool) {
	if len(value) != 0 && value[0] == '"' {
		var unquoted strings.Builder
		for i := 1; i < len(value); i++ {
			switch c := value[i]; c {
			case '"':
				return unquoted.String(), value[i+1:], validToken(unquoted.String())
			case '\\':
				if i++; i == len(value) {
					return "", "", false
				}
				unquoted.WriteByte(value[i])
			default:
				unquoted.WriteByte(c)
			}
		}
		return "", "", false
	}
	end := strings.IndexAny(value, ",; \t")
	if end < 0 {
		end = len(value)
	}
	return value[:end], value[end:], validToken(value[:end])
}

// parseWindowBits reads a max_window_bits value, which is 8 to 15 written
// without leading zeros.
func parseWindowBits(value string) (int, bool) {
	if len(value) == 0 || value[0] == '0' {
		return 0, false
	}
	bits, err := strconv.Atoi(value)
	return bits, err == nil && bits >= minWindowBits && bits <= maxWindowBits
}

// acceptDeflateOffer picks the first permessage-deflate offer this server can
// accept and returns the response header value for it, or "" to decline them
// all, which leaves the connection uncompressed.
func acceptDeflateOffer(values []string) (string, deflateParams) {
	extensions, ok := parseExtensions(values)
	if !ok {
		return "", deflateParams{}
	}
offers:
	for _, extension := range extensions {
		if extension[0].name != deflateExtension || extension[0].hasValue {
			continue
		}
		params := deflateParams{windowBits: maxWindowBits, peerContextTakeover: true}
		seen := make(map[string]bool, len(extension)-1)
		for _, param := range extension[1:] {
			if seen[param.name] {
				continue offers
			}
			seen[param.name] = true
			switch param.name {
			case "server_no_context_takeover", "client_no_context_takeover":
				if param.hasValue {
					continue offers
				}
				if param.name == "client_no_context_takeover" {
					params.peerContextTakeover = false
				}
			case "server_max_window_bits":
				bits, valid := parseWindowBits(param.value)
				if !valid {
					continue offers
				}
				params.windowBits = bits
			case "client_max_window_bits":
				// Any window the client picks fits the decompressor, so the
				// response need not limit it.
				if param.hasValue {
					if _, valid := parseWindowBits(param.value); !valid {
						continue offers
					}
				}
			default:
				continue offers
			}
		}
		// Sending without context takeover is announced whether or not it
		// was asked for (RFC 7692 section 7.1.1.1).
		response := deflateExtension + "; server_no_context_takeover"
		if !params.peerContextTakeover {
			response += "; client_no_context_takeover"
		}
		if params.windowBits != maxWindowBits {
			response += "; server_max_window_bits=" + strconv.Itoa(params.windowBits)
		}
		return response, params
	}
	return "", deflateParams{}
}

// deflateOffer is what a client offers: it never keeps its own context, and
// lets the server bound its window.
const deflateOffer = deflateExtension + "; client_no_context_takeover; client_max_window_bits"

// acceptDeflateResponse checks the server's answer to deflateOffer. A server
// that did not accept it answers with no extension at all, and enabled is
// false.
func acceptDeflateResponse(values []string) (params deflateParams, enabled, ok bool) {
	extensions, ok := parseExtensions(values)
	if !ok || len(extensions) > 1 {
		return deflateParams{}, false, false
	}
	if len(extensions) == 0 {
		return deflateParams{}, false, true
	}
	extension := extensions[0]
	if extension[0].name != deflateExtension || extension[0].hasValue {
		return deflateParams{}, false, false
	}
	params = deflateParams{windowBits: maxWindowBits, peerContextTakeover: true}
	seen := make(map[string]bool, len(extension)-1)
	for _, param := range extension[1:] {
		if seen[param.name] {
			return deflateParams{}, false, false
		}
		seen[param.name] = true
		switch param.name {
		case "server_no_context_takeover", "client_no_context_takeover":
			if param.hasValue {
				return deflateParams{}, false, false
			}
			if param.name == "server_no_context_takeover" {
				params.peerContextTakeover = false
			}
		case "server_max_window_bits":
			if _, valid := parseWindowBits(param.value); !valid {
				return deflateParams{}, false, false
			}
		case "client_max_window_bits":
			bits, valid := parseWindowBits(param.value)
			if !valid {
				return deflateParams{}, false, false
			}
			params.windowBits = bits
		default:
			return deflateParams{}, false, false
		}
	}
	return params, true, true
}

type compressor struct {
	w   *flate.Writer
	buf bytes.Buffer
}

var compressors = sync.Pool{New: func() any {
	c := new(compressor)
	c.w, _ = flate.NewWriter(nil, flate.BestSpeed)
	return c
}}

// release returns c to the pool, unless one large message grew its buffer
// past what is worth keeping.
func (c *compressor) release() {
	if c.buf.Cap() > maxRetainedFrameBuffer {
		c.buf = bytes.Buffer{}
	}
	compressors.Put(c)
}

// compressMessage deflates payload into a pooled compressor's buffer, without
// the trailing empty block RFC 7692 has senders strip. The caller returns the
// compressor to the pool once the bytes are sent.
//
// compress/flate always looks back as far as 32 KiB, so for a smaller window
// the payload is compressed in pieces no longer than that window, each by a
// fresh compressor. Every piece ends on a byte boundary with a flush, so the
// pieces join into one valid stream that never refers further back than the
// window. At BestSpeed a fresh start is cheap.
func compressMessage(payload []byte, windowBits int) (*compressor, []byte) {
	c := compressors.Get().(*compressor)
	c.buf.Reset()
	chunk := len(payload)
	if windowBits < maxWindowBits {
		chunk = 1 << windowBits
	}
	for first := true; first || len(payload) != 0; first = false {
		piece := payload
		if len(piece) > chunk {
			piece = piece[:chunk]
		}
		payload = payload[len(piece):]
		c.w.Reset(&c.buf)
		_, _ = c.w.Write(piece)
		_ = c.w.Flush()
	}
	// Flush always ends with the empty stored block 00 00 ff ff.
	out := c.buf.Bytes()
	return c, out[:len(out)-4]
}

// sliceReader feeds a decompressor a compressed message and deflateTail. It
// is an io.ByteReader so flate reads it directly instead of through a
// bufio.Reader of its own.
type sliceReader struct {
	parts [2][]byte
}

func (r *sliceReader) Read(p []byte) (int, error) {
	for i := range r.parts {
		if len(r.parts[i]) != 0 {
			n := copy(p, r.parts[i])
			r.parts[i] = r.parts[i][n:]
			return n, nil
		}
	}
	return 0, io.EOF
}

func (r *sliceReader) ReadByte() (byte, error) {
	for i := range r.parts {
		if len(r.parts[i]) != 0 {
			b := r.parts[i][0]
			r.parts[i] = r.parts[i][1:]
			return b, nil
		}
	}
	return 0, io.EOF
}

type decompressor struct {
	r   io.ReadCloser
	src sliceReader
	// probe takes what follows once the destination is full.
	probe [512]byte
}

var decompressors sync.Pool

// decompressMessage inflates a compressed message onto dst, with dict as the
// history it may refer back to. A message that inflates past limit bytes
// fails with ErrMessageTooBig, and data that is not DEFLATE with
// ErrInvalidPayload.
func decompressMessage(dst, compressed, dict []byte, limit int64) ([]byte, error) {
	d, _ := decompressors.Get().(*decompressor)
	if d == nil {
		d = new(decompressor)
	}
	d.src.parts = [2][]byte{compressed, deflateTail}
	if d.r == nil {
		d.r = flate.NewReaderDict(&d.src, dict)
	} else {
		_ = d.r.(flate.Resetter).Reset(&d.src, dict)
	}
	defer func() {
		d.src.parts = [2][]byte{}
		decompressors.Put(d)
	}()
	start := len(dst)
	for {
		var n int
		var err error
		if len(dst) < cap(dst) {
			n, err = d.r.Read(dst[len(dst):cap(dst)])
			dst = dst[:len(dst)+n]
		} else {
			// dst is full, but the message may have ended with it: read into
			// the probe so dst grows only if there is more.
			n, err = d.r.Read(d.probe[:])
			if n != 0 {
				grow := cap(dst) - start
				if remaining := limit + 1 - int64(len(dst)-start); int64(grow) > remaining {
					grow = int(remaining)
				}
				dst = append(slices.Grow(dst, max(grow, n)), d.probe[:n]...)
			}
		}
		if int64(len(dst)-start) > limit {
			return nil, ErrMessageTooBig
		}
		if err == io.EOF {
			return dst, nil
		}
		if err != nil {
			return nil, ErrInvalidPayload
		}
	}
}

// keepWindow appends message to the history the peer's next message may refer
// back to and trims it to DEFLATE's largest window.
func keepWindow(window, message []byte) []byte {
	if len(message) >= maxWindowSize {
		return append(window[:0], message[len(message)-maxWindowSize:]...)
	}
	if excess := len(window) + len(message) - maxWindowSize; excess > 0 {
		window = window[:copy(window, window[excess:])]
	}
	return append(window, message...)
}
