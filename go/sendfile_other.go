//go:build !linux && !darwin && !windows

package fib

// SendFile sends count bytes of f, starting at offset. The portable backend
// writes synchronously, so the range is read and written before SendFile
// returns, and the caller may close f afterwards. A file shorter than the
// range is an error, since the peer was promised count bytes.
func (c *Connection) SendFile(f File, offset, count int64) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp {
		return ErrSendFileDatagram
	}
	return sendFileCopy(c, f, offset, count, c.Send)
}
