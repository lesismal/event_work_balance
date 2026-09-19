//go:build !linux && !darwin && !windows

package fib

import (
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"
)

// udpListener is a UDP socket and the peers it has seen. Each peer is a
// connection whose conn is a udpPeerConn, so it sends and closes like any
// other connection on this backend.
type udpListener struct {
	pc    *net.UDPConn
	mu    sync.Mutex
	peers map[netip.AddrPort]*Connection
}

func listenUDP(network string, addrs []string) ([]*udpListener, error) {
	listeners := make([]*udpListener, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			addr = ":0"
		}
		resolved, err := net.ResolveUDPAddr(network, addr)
		if err == nil {
			var pc *net.UDPConn
			if pc, err = net.ListenUDP(network, resolved); err == nil {
				listeners = append(listeners, &udpListener{pc: pc, peers: make(map[netip.AddrPort]*Connection)})
				continue
			}
		}
		for _, opened := range listeners {
			_ = opened.pc.Close()
		}
		return nil, err
	}
	return listeners, nil
}

// serveUDP reads a UDP socket until the engine stops, queueing each datagram
// on its peer's connection and opening connections for new peers.
func (e *Engine) serveUDP(l *udpListener) error {
	buf := make([]byte, maxDatagramSize)
	for {
		n, addr, err := l.pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			if e.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			// A failed receive loses only its own datagram.
			continue
		}
		if c := e.udpPeer(l, addr); c != nil {
			c.udpActive.Store(time.Now().UnixNano())
			c.enqueueData(buf[:n])
		}
	}
}

func (e *Engine) udpPeer(l *udpListener, addr netip.AddrPort) *Connection {
	l.mu.Lock()
	c := l.peers[addr]
	l.mu.Unlock()
	if c != nil {
		return c
	}
	c = &Connection{engine: e, handler: e.handler, conn: &udpPeerConn{l: l, addr: addr}, udp: true}
	c.fd.Store(-1)
	c.udpActive.Store(time.Now().UnixNano())
	e.mu.Lock()
	if e.stopping.Load() {
		e.mu.Unlock()
		return nil
	}
	e.connections[c] = struct{}{}
	e.mu.Unlock()
	l.mu.Lock()
	l.peers[addr] = c
	l.mu.Unlock()
	c.handler.OnOpen(c)
	return c
}

// udpPeerConn is one peer of a UDP listener, as the connection sees it: writes
// are datagrams to the peer, and closing forgets the peer.
type udpPeerConn struct {
	l    *udpListener
	addr netip.AddrPort
}

func (p *udpPeerConn) Write(b []byte) (int, error) { return p.l.pc.WriteToUDPAddrPort(b, p.addr) }

func (p *udpPeerConn) Read([]byte) (int, error) {
	return 0, errors.New("fib: udp peer is read by its listener")
}

func (p *udpPeerConn) Close() error {
	p.l.mu.Lock()
	if c := p.l.peers[p.addr]; c != nil && c.conn == net.Conn(p) {
		delete(p.l.peers, p.addr)
	}
	p.l.mu.Unlock()
	return nil
}

func (p *udpPeerConn) LocalAddr() net.Addr              { return p.l.pc.LocalAddr() }
func (p *udpPeerConn) RemoteAddr() net.Addr             { return net.UDPAddrFromAddrPort(p.addr) }
func (p *udpPeerConn) SetDeadline(time.Time) error      { return nil }
func (p *udpPeerConn) SetReadDeadline(time.Time) error  { return nil }
func (p *udpPeerConn) SetWriteDeadline(time.Time) error { return nil }

// startUDPSweeper closes peers that have been silent for the idle timeout,
// until the engine stops.
func (e *Engine) startUDPSweeper() {
	if len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(udpSweepInterval(e.udpIdleTimeout))
		defer ticker.Stop()
		for {
			select {
			case <-e.stopped:
				return
			case <-ticker.C:
			}
			limit := int64(e.udpIdleTimeout)
			now := time.Now().UnixNano()
			var idle []*Connection
			for _, l := range e.udpListeners {
				l.mu.Lock()
				for _, c := range l.peers {
					if now-c.udpActive.Load() >= limit {
						idle = append(idle, c)
					}
				}
				l.mu.Unlock()
			}
			for _, c := range idle {
				c.closeWithError(ErrUDPIdleTimeout)
			}
		}
	}()
}

// LocalUDPAddrs returns one address per UDP listener, in configured order.
func (e *Engine) LocalUDPAddrs() ([]*net.UDPAddr, error) {
	addrs := make([]*net.UDPAddr, 0, len(e.udpListeners))
	for _, l := range e.udpListeners {
		addrs = append(addrs, l.pc.LocalAddr().(*net.UDPAddr))
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
		return nil, errors.New("fib: engine has no listener")
	}
	return addrs[0], nil
}
