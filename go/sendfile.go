package fib

import (
	"errors"
	"fmt"
	"io"
	"syscall"
)

// File is what SendFile sends from: an *os.File, or any type that wraps one
// and passes its descriptor on through SyscallConn.
type File interface {
	io.ReaderAt
	SyscallConn() (syscall.RawConn, error)
}

// ErrSendFileDatagram is what SendFile returns on a UDP connection, whose
// sends are datagrams rather than a stream a file could be copied into.
var ErrSendFileDatagram = errors.New("fib: SendFile needs a stream connection")

// sendFileChunk is how much of a file one read stages when the file cannot go
// to the socket directly: through a layer, or on a platform without sendfile.
const sendFileChunk = 64 << 10

func checkSendFileRange(offset, count int64) error {
	if offset < 0 || count < 0 {
		return fmt.Errorf("fib: invalid SendFile range %d+%d", offset, count)
	}
	return nil
}

// sendFileCopy sends count bytes of f from offset by reading them and handing
// each chunk to send, which must copy it. It is the fallback for connections
// whose bytes are transformed on the way out, as TLS does, and for the
// portable backend. A file that fails part way, such as one shorter than the
// range, leaves the peer short of bytes it was promised, so the connection is
// closed; one that fails before anything was sent leaves it as it was.
func sendFileCopy(c *Connection, f File, offset, count int64, send func([]byte) error) error {
	buf := make([]byte, min(count, sendFileChunk))
	sent := false
	for count > 0 {
		n, err := f.ReadAt(buf[:min(count, int64(len(buf)))], offset)
		if n > 0 {
			if sendErr := send(buf[:n]); sendErr != nil {
				return sendErr
			}
			sent = true
			offset += int64(n)
			count -= int64(n)
		}
		if count == 0 {
			return nil
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if sent {
			c.closeWithError(err)
		}
		return err
	}
	return nil
}
