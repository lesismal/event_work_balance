//go:build linux || darwin

package fib

import (
	"io"
	"syscall"
)

// maxSendFileCall bounds one sendfile call, below the 2GB Linux takes at most.
const maxSendFileCall = 1 << 30

// fileSegment is the part of a file SendFile has yet to send, read through
// the connection's own duplicate of the caller's descriptor.
type fileSegment struct {
	fd        int
	offset    int64
	remaining int64
	// copyBuf is set once sendfile has refused this pair of descriptors, as
	// macOS does for anything but a TCP socket, after which the segment is
	// copied through it instead.
	copyBuf []byte
}

func newFileSegment(f File, offset, count int64) (*fileSegment, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return nil, err
	}
	dup := -1
	var dupErr error
	err = rc.Control(func(fd uintptr) {
		syscall.ForkLock.RLock()
		dup, dupErr = syscall.Dup(int(fd))
		if dupErr == nil {
			syscall.CloseOnExec(dup)
		}
		syscall.ForkLock.RUnlock()
	})
	if err == nil {
		err = dupErr
	}
	if err != nil {
		return nil, err
	}
	return &fileSegment{fd: dup, offset: offset, remaining: count}, nil
}

func (s *fileSegment) advance(n int64) {
	s.offset += n
	s.remaining -= n
}

func (s *fileSegment) close() {
	if s.fd >= 0 {
		_ = syscall.Close(s.fd)
		s.fd = -1
	}
}

// sysSendFileLocked sends what it can of the file item at the head of the
// queue. fromFile reports that the bytes went by sendfile, from the file
// rather than from memory. Callers hold c.mu.
func (c *Connection) sysSendFileLocked(item *sendItem) (n, attempted int, fromFile bool, err error) {
	s := item.file
	attempted = int(min(s.remaining, maxSendFileCall))
	if s.copyBuf == nil {
		for {
			offset := s.offset
			n, err = syscall.Sendfile(c.FD(), s.fd, &offset, attempted)
			n = max(n, 0)
			if err == syscall.EINTR && n == 0 {
				continue
			}
			if n > 0 && err != nil {
				// macOS reports what it sent along with EAGAIN or EINTR. A
				// partial count stands, and a full socket raises the write
				// edge that resumes the rest; an interrupted call is short
				// only because it was interrupted, so it must not wait for an
				// edge that may never come.
				if err == syscall.EINTR {
					attempted = n
				}
				err = nil
			}
			break
		}
		switch {
		case err == nil && n == 0:
			return 0, attempted, true, io.ErrUnexpectedEOF
		case err == syscall.EINVAL || err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP || err == syscall.ENOTSOCK:
			// This kind of socket or file cannot be spliced; copy it.
			s.copyBuf = make([]byte, sendFileChunk)
		default:
			return n, attempted, true, err
		}
	}
	chunk := s.copyBuf[:min(s.remaining, int64(len(s.copyBuf)))]
	read, err := syscall.Pread(s.fd, chunk, s.offset)
	if err != nil {
		return 0, len(chunk), true, err
	}
	if read == 0 {
		return 0, len(chunk), true, io.ErrUnexpectedEOF
	}
	n, err = c.sysWrite(chunk[:read])
	// The bytes were read for this write alone; what it did not take is read
	// again next time, so none of it is ever counted as pending.
	return max(n, 0), read, true, err
}
