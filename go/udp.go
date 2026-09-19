//go:build linux || darwin || windows

package fib

import (
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"
)

// UDP rides on the same connections, handlers and workers as TCP. What is
// different is who reads the socket and what a connection is.
//
// A listening UDP socket has no connections of its own, so the engine makes
// one per peer address: the first datagram from an address opens a
// connection for it, OnOpen runs, and every datagram from that address is
// then that connection's input. The connection closes when it is closed, or
// when the peer has been silent for Config.UDPIdleTimeout. A dialed UDP
// connection is a socket connected to one peer and is simply that peer's.
//
// The event loop reads UDP sockets itself and queues each datagram on its
// connection, and a worker hands the queue to OnData one datagram per call.
// Reading on the loop is what lets many peers share one socket without any of
// them reading another's datagrams, and it keeps datagram boundaries intact:
// OnData receives exactly one datagram, and each Send sends exactly one.
//
// Sends go straight to the socket and are never queued. A datagram the socket
// has no room for is dropped and Send reports the error, which is what UDP
// does anyway when a router has no room for it, so there is no backpressure
// and no write watermark to pause reads on.

// udpState is what a UDP connection keeps on top of an ordinary one.
type udpState struct {
	udpPlatform
	// listener is the socket a peer's datagrams arrive on and its replies
	// leave from, or nil for a dialed connection, which has its own socket.
	listener *udpListener
	// sa and key are a peer's address, as the socket calls take it and as the
	// listener's peer table is keyed. raddr is the same for RemoteAddr.
	sa    syscall.Sockaddr
	key   netip.AddrPort
	raddr *net.UDPAddr
	// queue holds datagrams the loop has read and the handler has not seen
	// yet, from head on. Guarded by the connection's mu.
	queue [][]byte
	head  int
	// lastActive is when the peer last sent or was sent a datagram, in
	// nanoseconds, for the idle timeout.
	lastActive atomic.Int64
}

// udpListener is a bound UDP socket and the peers it has seen.
type udpListener struct {
	udpListenerPlatform
	// peers maps an address to its open connection. Event-loop ownership.
	peers map[netip.AddrPort]*Connection
}

// sockaddrKey turns a peer's socket address into its table key.
func sockaddrKey(sa syscall.Sockaddr) (netip.AddrPort, bool) {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)), true
	case *syscall.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port)), true
	}
	return netip.AddrPort{}, false
}

func sockaddrToUDPAddr(sa syscall.Sockaddr) *net.UDPAddr {
	addr, err := sockaddrToTCPAddr(sa)
	if err != nil {
		return nil
	}
	return &net.UDPAddr{IP: addr.IP, Port: addr.Port, Zone: addr.Zone}
}

// udpPeer returns the connection for the peer at sa, opening one if this is
// the first datagram from it. It returns nil once the engine is stopping.
// Callers run on the event loop.
func (e *Engine) udpPeer(l *udpListener, sa syscall.Sockaddr) *Connection {
	key, ok := sockaddrKey(sa)
	if !ok {
		return nil
	}
	if c := l.peers[key]; c != nil {
		return c
	}
	if e.stopping.Load() {
		return nil
	}
	c := &Connection{engine: e, handler: e.handler,
		udp: &udpState{listener: l, sa: sa, key: key, raddr: sockaddrToUDPAddr(sa)}}
	c.initUDPPeer()
	c.udp.lastActive.Store(time.Now().UnixNano())
	l.peers[key] = c
	c.handler.OnOpen(c)
	return c
}

// deliverDatagram queues a copy of one datagram on its connection and reports
// the connection if that made it runnable. Callers run on the event loop.
func (e *Engine) deliverDatagram(c *Connection, data []byte) *Connection {
	u := c.udp
	c.mu.Lock()
	if c.closing || c.closed || len(u.queue)-u.head >= maxQueuedDatagrams {
		c.mu.Unlock()
		return nil
	}
	if u.head == len(u.queue) {
		u.queue = u.queue[:0]
		u.head = 0
	}
	u.queue = append(u.queue, append([]byte(nil), data...))
	c.mu.Unlock()
	u.lastActive.Store(time.Now().UnixNano())
	return e.noteEvent(c, evIn)
}

// drainDatagrams hands every queued datagram to the handler, one per OnData.
func (c *Connection) drainDatagrams() {
	u := c.udp
	for {
		c.mu.Lock()
		if u.head == len(u.queue) || c.closing || c.closed {
			c.mu.Unlock()
			return
		}
		data := u.queue[u.head]
		u.queue[u.head] = nil
		u.head++
		c.mu.Unlock()
		c.handler.OnData(c, data)
	}
}

// sendDatagram sends data as one datagram, or drops it and reports why.
func (c *Connection) sendDatagram(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.closeAfterSend {
		return syscall.EPIPE
	}
	err := c.sysSendDatagram(data)
	if err == nil {
		c.udp.lastActive.Store(time.Now().UnixNano())
	}
	return err
}

// sendDatagramParts sends two parts as one datagram.
func (c *Connection) sendDatagramParts(first, second []byte) error {
	if len(second) == 0 {
		return c.sendDatagram(first)
	}
	if len(first) == 0 {
		return c.sendDatagram(second)
	}
	return c.sendDatagram(append(append(make([]byte, 0, len(first)+len(second)), first...), second...))
}

// detachPeer drops a closed peer from its listener's table. The socket is the
// listener's, so there is nothing to close. Callers run on the event loop.
func (e *Engine) detachPeer(c *Connection) {
	u := c.udp
	if u.listener.peers[u.key] == c {
		delete(u.listener.peers, u.key)
	}
}

// startUDPSweeper starts the ticker that has the loop look for idle peers.
func (e *Engine) startUDPSweeper() {
	if len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
	e.udpSweepDone = make(chan struct{})
	go func(done <-chan struct{}) {
		ticker := time.NewTicker(udpSweepInterval(e.udpIdleTimeout))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if !e.stopping.Load() && !e.request(command{kind: commandUDPSweep}) {
					return
				}
			}
		}
	}(e.udpSweepDone)
}

func (e *Engine) stopUDPSweeper() {
	if e.udpSweepDone != nil {
		close(e.udpSweepDone)
		e.udpSweepDone = nil
	}
}

// sweepUDP closes the peers that have been silent for the idle timeout.
// Callers run on the event loop.
func (e *Engine) sweepUDP() {
	limit := int64(e.udpIdleTimeout)
	if limit <= 0 {
		return
	}
	now := time.Now().UnixNano()
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			if now-c.udp.lastActive.Load() >= limit {
				e.closeConnection(c, ErrUDPIdleTimeout, true)
			}
		}
	}
}

// closeUDPPeers closes every peer without a callback, as Close does for TCP
// connections.
func (e *Engine) closeUDPPeers() {
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			e.closeConnection(c, nil, false)
		}
	}
}

// LocalUDPAddrs returns one address per UDP listener, in configured order.
// Ports left at zero report the port the kernel chose.
func (e *Engine) LocalUDPAddrs() ([]*net.UDPAddr, error) {
	addrs := make([]*net.UDPAddr, 0, len(e.udpListeners))
	for _, l := range e.udpListeners {
		sa, err := l.sockname()
		if err != nil {
			return nil, err
		}
		addr := sockaddrToUDPAddr(sa)
		if addr == nil {
			return nil, syscall.EAFNOSUPPORT
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// LocalUDPAddr returns the address of the engine's first UDP listener.
func (e *Engine) LocalUDPAddr() (*net.UDPAddr, error) {
	addrs, err := e.LocalUDPAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNoListener
	}
	return addrs[0], nil
}

// IsUDP reports whether the connection exchanges datagrams.
func (c *Connection) IsUDP() bool { return c.udp != nil }

// RemoteAddr returns the peer's address: a *net.UDPAddr for a UDP
// connection, a *net.UnixAddr for a Unix socket, whose name is empty when the
// peer never bound one, and a *net.TCPAddr otherwise. It returns nil once the
// socket is gone.
func (c *Connection) RemoteAddr() net.Addr {
	if c.udp != nil {
		return c.udp.raddr
	}
	sa, err := c.peerSockaddr()
	if err != nil {
		return nil
	}
	return sockaddrToAddr(sa)
}
