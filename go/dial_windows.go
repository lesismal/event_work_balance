//go:build windows

package fib

import (
	"net"
	"syscall"
)

// connectSocket opens a socket and starts an overlapped ConnectEx on it, whose
// completion reports the connect's outcome. ConnectEx always completes through
// the port, even when it finishes at once, so connected is always false here.
// Callers run on the event loop.
func (e *Engine) connectSocket(d *dialRequest) (c *Connection, connected bool, err error) {
	if isUDPNetwork(d.network) {
		return e.connectDatagram(d)
	}
	if isUnixNetwork(d.network) {
		return e.connectUnix(d)
	}
	s, err := newSocket(d.family)
	if err != nil {
		return nil, false, err
	}
	// ConnectEx requires a bound socket, and binding the wildcard address leaves
	// the choice of interface and port to the kernel, as connect() would.
	var local syscall.Sockaddr = &syscall.SockaddrInet4{}
	if d.family == syscall.AF_INET6 {
		local = &syscall.SockaddrInet6{}
	}
	err = syscall.Bind(s, local)
	if err == nil {
		err = setNonblock(s)
	}
	if err == nil {
		_, err = syscall.CreateIoCompletionPort(s, e.port, 0, 0)
	}
	if err != nil {
		syscall.Closesocket(s)
		return nil, false, err
	}
	c = &Connection{engine: e, handler: d.handler, dialing: d}
	c.handle.Store(uintptr(s))
	c.readOp = ioOp{kind: opRead, conn: c}
	// The connect borrows the write operation: no write can be posted before
	// the connection exists, and completeConnect hands it back to writes.
	c.writeOp = ioOp{kind: opConnect, conn: c}
	e.conns[c] = struct{}{}
	c.outstanding.Add(1)
	err = syscall.ConnectEx(s, d.sa, nil, 0, &c.connectSent, &c.writeOp.ov)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		c.outstanding.Add(-1)
		delete(e.conns, c)
		syscall.Closesocket(s)
		return nil, false, err
	}
	return c, false, nil
}

// connectDatagram opens a UDP socket connected to the dialed peer and posts
// its first receive. Connecting a UDP socket only records the peer, so it is
// done on the spot; the receive cannot complete before the dial is reported,
// since its completion is handled on the event loop that is reporting it.
func (e *Engine) connectDatagram(d *dialRequest) (c *Connection, connected bool, err error) {
	s, err := newDatagramSocket(d.family)
	if err != nil {
		return nil, false, err
	}
	err = syscall.Connect(s, d.sa)
	if err == nil {
		_, err = syscall.CreateIoCompletionPort(s, e.port, 0, 0)
	}
	if err != nil {
		syscall.Closesocket(s)
		return nil, false, err
	}
	c = &Connection{engine: e, handler: d.handler, dialing: d,
		udp: &udpState{raddr: &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}}}
	c.udp.buf = make([]byte, maxDatagramSize)
	c.handle.Store(uintptr(s))
	c.readOp = ioOp{kind: opRecvDatagram, conn: c}
	c.writeOp = ioOp{kind: opWrite, conn: c}
	e.conns[c] = struct{}{}
	c.mu.Lock()
	err = c.armDatagramReadLocked()
	c.mu.Unlock()
	if err != nil {
		delete(e.conns, c)
		syscall.Closesocket(s)
		return nil, false, err
	}
	return c, true, nil
}

// connectUnix connects a Unix socket. ConnectEx only takes TCP, so this is a
// plain connect, as the net package makes on Windows too; a local connect
// finishes, or is refused, without waiting on a network.
func (e *Engine) connectUnix(d *dialRequest) (c *Connection, connected bool, err error) {
	s, err := newSocket(d.family)
	if err != nil {
		return nil, false, err
	}
	err = syscall.Connect(s, d.sa)
	if err == nil {
		err = setNonblock(s)
	}
	if err == nil {
		_, err = syscall.CreateIoCompletionPort(s, e.port, 0, 0)
	}
	if err != nil {
		syscall.Closesocket(s)
		return nil, false, err
	}
	c = &Connection{engine: e, handler: d.handler, dialing: d}
	c.handle.Store(uintptr(s))
	c.readOp = ioOp{kind: opRead, conn: c}
	c.writeOp = ioOp{kind: opWrite, conn: c}
	e.conns[c] = struct{}{}
	return c, true, nil
}

// completeConnect settles a ConnectEx. A dial that was abandoned while the
// connect was in flight, by its timeout or by the engine closing, has already
// been reported, and only its memory is left to release.
func (e *Engine) completeConnect(c *Connection, err error) {
	c.outstanding.Add(-1)
	c.writeOp.kind = opWrite
	if c.dialing == nil {
		e.forget(c)
		return
	}
	if err == nil {
		// Without this the socket does not know it is connected, and
		// shutdown and getpeername fail on it.
		err = syscall.Setsockopt(c.socket(), syscall.SOL_SOCKET, syscall.SO_UPDATE_CONNECT_CONTEXT, nil, 0)
	}
	if err != nil {
		e.failDial(c, err)
		return
	}
	e.completeDial(c)
	c.rearmRead()
}
