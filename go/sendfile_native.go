//go:build linux || darwin || windows

package fib

import "syscall"

// SendFile sends count bytes of f, starting at offset, after whatever was
// sent before it and ahead of whatever is sent after. On Linux and macOS the
// bytes go from the file to the socket by sendfile(2), without passing
// through user space; on Windows they are read a chunk at a time as the
// socket takes them. Either way the file is read only as the peer keeps up,
// so a large file costs no more memory than a small one.
//
// The connection sends from its own duplicate of f's descriptor, so the
// caller may close f as soon as SendFile returns. The file must hold the
// whole range: a file that turns out shorter closes the connection, since the
// peer was promised count bytes.
//
// A connection with a layer, such as TLS, cannot hand the file to the kernel,
// since the layer has to transform its bytes: the range is read and sent
// through the layer before SendFile returns.
func (c *Connection) SendFile(f File, offset, count int64) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp != nil {
		return ErrSendFileDatagram
	}
	if count == 0 {
		return nil
	}
	if l := c.layer; l != nil {
		return sendFileCopy(c, f, offset, count, func(b []byte) error { return l.Send(b, nil) })
	}
	seg, err := newFileSegment(f, offset, count)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		seg.close()
		return syscall.EPIPE
	}
	direct := c.canWriteDirectlyLocked()
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{file: seg})
	c.mu.Unlock()
	if !direct {
		// Whatever is queued ahead of the file is being flushed, or will be
		// when the round that corked the connection ends, and the file goes
		// out after it.
		return nil
	}
	if err := c.flushOutput(); err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}
