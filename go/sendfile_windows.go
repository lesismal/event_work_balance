//go:build windows

package fib

import (
	"io"
	"os"
	"syscall"
)

// fileSegment is the part of a file SendFile has yet to send, read through
// the connection's own duplicate of the caller's handle. Windows has no
// sendfile for an overlapped socket the engine could use, so the file is read
// a chunk at a time, each chunk once the socket has taken the one before.
type fileSegment struct {
	// file owns the duplicate handle. Keeping it an *os.File lets the
	// garbage collector close it if the connection drops its queue while an
	// overlapped send still holds a chunk.
	file      *os.File
	offset    int64
	remaining int64
	chunk     []byte
}

func newFileSegment(f File, offset, count int64) (*fileSegment, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return nil, err
	}
	var dup syscall.Handle
	var dupErr error
	err = rc.Control(func(fd uintptr) {
		process, _ := syscall.GetCurrentProcess()
		dupErr = syscall.DuplicateHandle(process, syscall.Handle(fd), process, &dup, 0, false, syscall.DUPLICATE_SAME_ACCESS)
	})
	if err == nil {
		err = dupErr
	}
	if err != nil {
		return nil, err
	}
	return &fileSegment{file: os.NewFile(uintptr(dup), ""), offset: offset, remaining: count}, nil
}

func (s *fileSegment) advance(n int64) {
	s.offset += n
	s.remaining -= n
}

func (s *fileSegment) close() {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

// stageLocked reads the file item's next chunk into its data, unless one is
// already waiting there. Callers hold c.mu.
func (c *Connection) stageLocked(item *sendItem) error {
	if item.data != nil {
		return nil
	}
	s := item.file
	if s.chunk == nil {
		s.chunk = make([]byte, min(s.remaining, sendFileChunk))
	}
	n, err := s.file.ReadAt(s.chunk[:min(s.remaining, int64(len(s.chunk)))], s.offset)
	if n == 0 {
		if err == nil || err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	item.data, item.offset = s.chunk[:n], 0
	s.advance(int64(n))
	c.addPending(int64(n))
	return nil
}

// sysSendFileLocked sends what it can of the file item at the head of the
// queue, from a chunk staged in memory, which counts as pending like any
// other queued bytes. Callers hold c.mu.
func (c *Connection) sysSendFileLocked(item *sendItem) (n, attempted int, fromFile bool, err error) {
	if err = c.stageLocked(item); err != nil {
		return 0, 0, false, err
	}
	data := item.data[item.offset:]
	n, err = c.sysWrite(data)
	return n, len(data), false, err
}
