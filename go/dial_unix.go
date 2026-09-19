//go:build linux || darwin

package fib

import (
	"net"
	"syscall"
)

// connectSocket opens a non-blocking socket, starts connecting it and
// registers it with the backend, where the connect's outcome arrives as the
// socket's first events. connected reports a connect the kernel finished on the
// spot, as it may over loopback. Callers run on the event loop.
func (e *Engine) connectSocket(d *dialRequest) (c *Connection, connected bool, err error) {
	if isUDPNetwork(d.network) {
		return e.connectDatagram(d)
	}
	fd, err := newSocket(d.family)
	if err != nil {
		return nil, false, err
	}
	for {
		err = syscall.Connect(fd, d.sa)
		if err != syscall.EINTR {
			break
		}
	}
	switch err {
	case nil:
		connected = true
	case syscall.EINPROGRESS, syscall.EALREADY:
	default:
		syscall.Close(fd)
		return nil, false, err
	}
	token := uint64(uint32(fd)) | e.nextGeneration.Add(1)<<32
	c = &Connection{engine: e, handler: d.handler, dialing: d}
	c.token = token
	c.fd.Store(int32(fd))
	// Registering an unconnected socket is what makes the connect
	// asynchronous: its first write edge says the connect has finished, one
	// way or the other. A connect that finished before the registration still
	// raises that edge, since adding a descriptor reports its current state.
	if err = e.registerConnection(fd, token); err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	e.trackConnection(fd, c)
	return c, connected, nil
}

// connectDatagram opens a UDP socket connected to the dialed peer. Connecting
// a UDP socket only records the peer, so it is done on the spot.
func (e *Engine) connectDatagram(d *dialRequest) (c *Connection, connected bool, err error) {
	fd, err := newDatagramSocket(d.family)
	if err != nil {
		return nil, false, err
	}
	for {
		err = syscall.Connect(fd, d.sa)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	token := uint64(uint32(fd)) | e.nextGeneration.Add(1)<<32
	c = &Connection{engine: e, handler: d.handler, dialing: d,
		udp: &udpState{raddr: &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}}}
	c.token = token
	c.fd.Store(int32(fd))
	if err = e.registerDatagram(fd, token); err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	e.trackConnection(fd, c)
	return c, true, nil
}

// finishDial settles a connect from the events its socket raised and reports
// the connection if those events also need a worker. reported is the error the
// backend delivered with the events, if it carries one; the socket's own error
// takes precedence. Callers run on the event loop.
func (e *Engine) finishDial(c *Connection, events uint32, reported error) *Connection {
	fd := c.FD()
	var err error
	errno, sockErr := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ERROR)
	switch {
	case sockErr != nil:
		err = sockErr
	case errno == int(syscall.EINPROGRESS), errno == int(syscall.EALREADY), errno == int(syscall.EINTR):
		// Still connecting: an event can arrive before the connect settles.
		return nil
	case errno != 0 && errno != int(syscall.EISCONN):
		err = syscall.Errno(errno)
	case reported != nil:
		err = reported
	}
	if err == nil {
		// A clean socket error does not prove the connect finished, since
		// events can arrive spuriously. Having a peer does.
		if _, peerErr := syscall.Getpeername(fd); peerErr != nil {
			if events&(evErr|evHup) == 0 {
				return nil
			}
			// The kernel says the socket is finished but names no error.
			// Waiting would never end: no further edge is coming.
			err = syscall.ECONNREFUSED
		}
	}
	if err != nil {
		e.failDial(c, err)
		return nil
	}
	e.completeDial(c)
	// Whatever else arrived with the connect, bytes the peer sent straight
	// away above all, is now the connection's to handle.
	return e.noteEvent(c, events)
}
