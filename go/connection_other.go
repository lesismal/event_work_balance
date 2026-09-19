//go:build !linux && !darwin && !windows

package fib

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type portableEvent struct {
	data     []byte
	closeErr error
	closing  bool
}

type readBuffer struct{ data []byte }

type connectionAttachment struct{ value any }

type Connection struct {
	engine                             *Engine
	handler                            Handler
	conn                               net.Conn
	fd                                 atomic.Int64
	mu                                 sync.Mutex
	events                             []portableEvent
	scheduled, closing, closeDelivered bool
	writeMu                            sync.Mutex
	attachment                         atomic.Pointer[connectionAttachment]
	layer                              Layer
	// udp marks a connection that exchanges datagrams: a peer of a UDP
	// listener, whose conn is a udpPeerConn, or a dialed UDP socket.
	udp bool
	// udpActive is when a listener's peer last sent or was sent a datagram,
	// in nanoseconds, for the idle timeout.
	udpActive atomic.Int64
}

// IsUDP reports whether the connection exchanges datagrams.
func (c *Connection) IsUDP() bool { return c.udp }

// RemoteAddr returns the peer's address.
func (c *Connection) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

func (c *Connection) FD() int { return int(c.fd.Load()) }

func (c *Connection) Close() { c.closeWithError(nil) }

func (c *Connection) Attachment() any {
	if value := c.attachment.Load(); value != nil {
		return value.value
	}
	return nil
}

func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
	} else {
		c.attachment.Store(&connectionAttachment{value: value})
	}
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	c.events = append(c.events, portableEvent{closing: true, closeErr: err})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	_ = c.conn.Close()
	if submit && !c.engine.submit(c) {
		c.engine.finishConnection(c, err)
	}
}

func (c *Connection) Send(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.sendRaw(data)
}

// sendRaw writes bytes to the socket below any layer.
func (c *Connection) sendRaw(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return io.ErrClosedPipe
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			c.closeWithError(err)
			return err
		}
		if n == 0 {
			c.closeWithError(io.ErrNoProgress)
			return io.ErrNoProgress
		}
		data = data[n:]
	}
	if c.udp {
		c.udpActive.Store(time.Now().UnixNano())
	}
	return nil
}

// Flush does nothing on the portable backend, whose sends have reached the
// socket by the time they return.
func (c *Connection) Flush() error { return nil }

// SendOwned is equivalent to Send on the synchronous portable backend.
func (c *Connection) SendOwned(data []byte) error { return c.Send(data) }

func (c *Connection) SendParts(first, second []byte) error {
	if l := c.layer; l != nil {
		return l.Send(first, second)
	}
	data := make([]byte, len(first)+len(second))
	n := copy(data, first)
	copy(data[n:], second)
	return c.Send(data)
}

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	return closing
}

// closeAfterSendRaw closes the connection. Sends on this backend have already
// reached the socket by the time they return, so nothing is left to wait for.
func (c *Connection) closeAfterSendRaw() { c.closeWithError(nil) }

func (c *Connection) enqueueData(data []byte) bool {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return false
	}
	c.events = append(c.events, portableEvent{data: append([]byte(nil), data...)})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if submit && !c.engine.submit(c) {
		c.closeWithError(errors.New("task pool stopped"))
		return false
	}
	return true
}

func (c *Connection) process() {
	defer func() {
		if r := recover(); r != nil {
			c.engine.finishConnection(c, fmt.Errorf("handler panic: %v", r))
		}
	}()
	for {
		c.mu.Lock()
		if len(c.events) == 0 {
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		event := c.events[0]
		c.events[0] = portableEvent{}
		c.events = c.events[1:]
		c.mu.Unlock()
		if event.closing {
			c.engine.finishConnection(c, event.closeErr)
			return
		}
		c.handler.OnData(c, event.data)
	}
}

func (c *Connection) RunTask() {
	defer c.engine.taskWG.Done()
	c.process()
}
