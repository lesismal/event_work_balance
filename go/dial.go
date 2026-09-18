//go:build linux || darwin || windows

package fib

import (
	"net"
	"os"
	"strings"
	"syscall"
	"time"
)

// dialRequest is one outbound connect, from Dial until the event loop reports
// its outcome.
type dialRequest struct {
	network string
	addr    string
	timeout time.Duration
	done    func(*Connection, error)
	// handler serves the connection once it is established.
	handler Handler
	// family and sa are the resolved address to connect to, and raddr the same
	// address as the net package reports it. err is set instead when resolving
	// failed, and the loop only has to report it.
	family int
	sa     syscall.Sockaddr
	raddr  *net.TCPAddr
	err    error
	// timer fires the dial timeout. Event-loop ownership.
	timer *time.Timer
}

// Dial connects to addr without blocking and hands the connection to this
// engine's event loop, which then serves it exactly as it serves an accepted
// one: the same handler, the same workers and the same backpressure.
//
// network and addr are what net.Dial takes: network is "tcp", "tcp4" or
// "tcp6" (empty means "tcp"), and addr is "host:port". A host that is not an IP
// literal is resolved on a goroutine of its own, so Dial never waits on DNS.
// A timeout of zero leaves the connect to the operating system's own timeout.
//
// When the connect succeeds, the handler's OnOpen runs first and then done
// with the connection. When it fails or times out, done runs with a nil
// connection and a *net.OpError, and the handler hears nothing. Either way done
// runs exactly once, on the event loop, so like OnOpen it must not block. It
// may be nil.
//
// Dial returns an error, and never calls done, only when it can tell at once
// that the dial cannot start: an unknown network, a malformed or unresolvable
// literal address, or an engine that has been stopped.
func (e *Engine) Dial(network, addr string, timeout time.Duration, done func(*Connection, error)) error {
	return e.DialWithHandler(network, addr, timeout, nil, done)
}

// DialWithHandler is Dial with a handler of the connection's own: OnOpen,
// OnData, OnPriorityData and OnClose for this connection go to handler rather
// than to the engine's. This is what lets one engine carry a server and the
// clients it talks to, each with its own protocol. A nil handler means the
// engine's.
func (e *Engine) DialWithHandler(network, addr string, timeout time.Duration, handler Handler, done func(*Connection, error)) error {
	if handler == nil {
		handler = e.handler
	}
	d := &dialRequest{network: network, addr: addr, timeout: timeout, done: done, handler: handler}
	if network == "" {
		d.network = "tcp"
	}
	if e.stopping.Load() {
		return d.opError(net.ErrClosed)
	}
	if !isLiteralAddr(addr) {
		if !isTCPNetwork(network) {
			return d.opError(net.UnknownNetworkError(network))
		}
		go func() {
			d.family, d.sa, d.raddr, d.err = resolveDialAddr(network, addr)
			e.requestDial(d)
		}()
		return nil
	}
	var err error
	if d.family, d.sa, d.raddr, err = resolveDialAddr(network, addr); err != nil {
		return d.opError(err)
	}
	e.requestDial(d)
	return nil
}

// isLiteralAddr reports whether addr names its host by IP, or not at all, so
// that resolving it needs no DNS.
func isLiteralAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		// A malformed address fails in the resolver without a lookup.
		return true
	}
	if zone := strings.IndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	return net.ParseIP(host) != nil
}

// requestDial hands a dial to the event loop. An engine that has already closed
// will never run it, so the dial fails here instead.
func (e *Engine) requestDial(d *dialRequest) {
	if !e.request(command{kind: commandDial, dial: d}) {
		d.fail(net.ErrClosed)
	}
}

// fail reports a dial that never produced a connection.
func (d *dialRequest) fail(err error) {
	if d.done != nil {
		d.done(nil, d.opError(err))
	}
}

// opError wraps a dial failure the way net.Dial does.
func (d *dialRequest) opError(err error) error {
	if errno, ok := err.(syscall.Errno); ok {
		err = os.NewSyscallError("connect", errno)
	}
	opErr := &net.OpError{Op: "dial", Net: d.network, Err: err}
	if d.raddr != nil {
		opErr.Addr = d.raddr
	}
	return opErr
}

// startDial runs on the event loop: it opens the socket, starts the connect
// and registers the descriptor, or reports why it could not.
func (e *Engine) startDial(d *dialRequest) {
	if d.err != nil {
		d.fail(d.err)
		return
	}
	if e.stopping.Load() {
		d.fail(net.ErrClosed)
		return
	}
	c, connected, err := e.connectSocket(d)
	if err != nil {
		d.fail(err)
		return
	}
	if connected {
		e.completeDial(c)
		return
	}
	if d.timeout > 0 {
		d.timer = time.AfterFunc(d.timeout, func() {
			e.request(command{kind: commandDialTimeout, connection: c, dial: d})
		})
	}
}

// expireDial fails a dial whose timeout fired while it was still connecting.
// The timer may fire just as the connect completes, so a dial that has already
// been settled, or a connection now dialing for a different request, is left
// alone.
func (e *Engine) expireDial(c *Connection, d *dialRequest) {
	if c.dialing == d {
		e.failDial(c, os.ErrDeadlineExceeded)
	}
}

// completeDial admits a connected socket to the engine. From here on it is an
// ordinary connection.
func (e *Engine) completeDial(c *Connection) {
	d := c.dialing
	c.dialing = nil
	if d.timer != nil {
		d.timer.Stop()
	}
	c.handler.OnOpen(c)
	if d.done != nil {
		d.done(c, nil)
	}
}

// failDial abandons a connect that failed, timed out or was overtaken by the
// engine closing, and tells the dialer why.
func (e *Engine) failDial(c *Connection, err error) {
	d := c.dialing
	c.dialing = nil
	if d.timer != nil {
		d.timer.Stop()
	}
	c.mu.Lock()
	c.closing = true
	c.closed = true
	c.mu.Unlock()
	e.detach(c)
	d.fail(err)
}
