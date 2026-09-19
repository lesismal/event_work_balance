//go:build !linux && !darwin && !windows

package fib

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Engine struct {
	listeners       []net.Listener
	handler         Handler
	taskPool        TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	stopping        atomic.Bool
	// stopped closes when Stop is called, which is what Run waits on when
	// there is no listener to serve.
	stopped        chan struct{}
	closeOnce      sync.Once
	mu             sync.Mutex
	connections    map[*Connection]struct{}
	readers        sync.WaitGroup
	readBufferPool sync.Pool
	// udpListeners are the engine's UDP sockets, when Config.Network names
	// UDP, and udpIdleTimeout closes their silent peers.
	udpListeners   []*udpListener
	udpIdleTimeout time.Duration
}

// Bind creates an engine that listens on config.Addr, or on every address in
// config.Addrs, and serves the connections it accepts with handler.
func Bind(config Config, handler Handler) (*Engine, error) {
	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
	}
	return newEngine(config, handler, addrs)
}

// NewEngine creates an engine with no listeners, for connections it dials.
func NewEngine(config Config, handler Handler) (*Engine, error) {
	return newEngine(config, handler, nil)
}

func newEngine(config Config, handler Handler, addrs []string) (*Engine, error) {
	if err := config.validateTaskPool(); err != nil {
		return nil, err
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = 256
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = 16 * 1024
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	network := config.Network
	if network == "" {
		network = "tcp"
	}
	var udpListeners []*udpListener
	if isUDPNetwork(network) {
		var err error
		if udpListeners, err = listenUDP(network, addrs); err != nil {
			return nil, err
		}
		addrs = nil
	}
	listeners := make([]net.Listener, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			addr = ":0"
		}
		listener, err := net.Listen(network, addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	pool, releasePool := acquireTaskPool(config)
	e := &Engine{listeners: listeners, handler: handler, taskPool: pool, releaseTaskPool: releasePool,
		connections: make(map[*Connection]struct{}), stopped: make(chan struct{}),
		udpListeners: udpListeners, udpIdleTimeout: udpIdleTimeout(config.UDPIdleTimeout)}
	e.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
	e.startUDPSweeper()
	return e, nil
}

func (e *Engine) submit(c *Connection) bool {
	e.taskWG.Add(1)
	if !e.taskPool.GoTask(c) {
		e.taskWG.Done()
		return false
	}
	return true
}

// Stats reports what backpressure this server has applied. This backend leaves
// socket I/O to the runtime's own poller, which applies the pushback itself, so
// there is no read interest for the server to pause and the counters stay at
// zero. The method exists so that code written against either backend compiles
// and runs on both.
type Stats struct {
	ReadsPausedByWatermark uint64
	ReadsPausedByBudget    uint64
	ReadsResumed           uint64
	PendingBytes           int64
}

func (e *Engine) Stats() Stats { return Stats{} }

// LocalAddr returns the address of the server's first listener.
func (e *Engine) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := e.LocalAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("fib: engine has no listener")
	}
	return addrs[0], nil
}

// ListenAddrs returns the address of every listener, in configured order,
// whatever its network.
func (e *Engine) ListenAddrs() ([]net.Addr, error) {
	addrs := make([]net.Addr, 0, len(e.listeners)+len(e.udpListeners))
	for _, listener := range e.listeners {
		addrs = append(addrs, listener.Addr())
	}
	for _, l := range e.udpListeners {
		addrs = append(addrs, l.pc.LocalAddr())
	}
	return addrs, nil
}

// LocalAddrs returns one address per TCP listener, in configured order.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(e.listeners))
	for _, listener := range e.listeners {
		addr, ok := listener.Addr().(*net.TCPAddr)
		if !ok {
			return nil, errors.New("listener is not TCP")
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// Run serves every listener until the server is stopped. It returns the first
// error any of them reported.
func (e *Engine) Run() error {
	if len(e.listeners) == 0 && len(e.udpListeners) == 0 {
		// A client engine's connections are served by their own readers, so
		// Run only has to last as long as the engine does.
		<-e.stopped
		return nil
	}
	if len(e.listeners) == 1 && len(e.udpListeners) == 0 {
		return e.serve(e.listeners[0])
	}
	errs := make(chan error, len(e.listeners)+len(e.udpListeners))
	var serving sync.WaitGroup
	for _, listener := range e.listeners {
		serving.Add(1)
		go func(listener net.Listener) {
			defer serving.Done()
			errs <- e.serve(listener)
		}(listener)
	}
	for _, l := range e.udpListeners {
		serving.Add(1)
		go func(l *udpListener) {
			defer serving.Done()
			errs <- e.serveUDP(l)
		}(l)
	}
	serving.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if e.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		e.adopt(conn, e.handler, nil)
	}
}

// adopt starts serving an established connection: OnOpen, then done if the
// connection was dialed, then its reader. It reports false, and closes conn,
// if the engine has stopped in the meantime.
func (e *Engine) adopt(conn net.Conn, handler Handler, done func(*Connection, error)) bool {
	return e.adoptWith(conn, handler, done, false)
}

// adoptWith is adopt for a connection that may be a dialed UDP one.
func (e *Engine) adoptWith(conn net.Conn, handler Handler, done func(*Connection, error), udp bool) bool {
	c := &Connection{engine: e, handler: handler, conn: conn, udp: udp}
	c.fd.Store(-1)
	e.mu.Lock()
	if e.stopping.Load() {
		e.mu.Unlock()
		_ = conn.Close()
		return false
	}
	e.connections[c] = struct{}{}
	e.readers.Add(1)
	e.mu.Unlock()
	c.handler.OnOpen(c)
	if done != nil {
		done(c, nil)
	}
	go e.readConnection(c)
	return true
}

// Dial connects to addr and serves the connection like an accepted one. This
// backend leaves socket I/O to the runtime's poller, so the connect runs on a
// goroutine of its own through net.DialTimeout, and done runs on that
// goroutine rather than on an event loop. Otherwise it behaves as the native
// backends' Dial does: OnOpen and then done on success, done alone with the
// error on failure, and an error from Dial itself, with done never called,
// only when the dial cannot start at all.
func (e *Engine) Dial(network, addr string, timeout time.Duration, done func(*Connection, error)) error {
	return e.DialWithHandler(network, addr, timeout, nil, done)
}

// DialWithHandler is Dial with a handler of the connection's own. A nil
// handler means the engine's.
func (e *Engine) DialWithHandler(network, addr string, timeout time.Duration, handler Handler, done func(*Connection, error)) error {
	if handler == nil {
		handler = e.handler
	}
	switch network {
	case "":
		network = "tcp"
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6", "unix":
	default:
		return &net.OpError{Op: "dial", Net: network, Err: net.UnknownNetworkError(network)}
	}
	if e.stopping.Load() {
		return &net.OpError{Op: "dial", Net: network, Err: net.ErrClosed}
	}
	go func() {
		conn, err := net.DialTimeout(network, addr, timeout)
		if err == nil && !e.adoptWith(conn, handler, done, isUDPNetwork(network)) {
			err = &net.OpError{Op: "dial", Net: network, Addr: conn.RemoteAddr(), Err: net.ErrClosed}
		}
		if err != nil && done != nil {
			done(nil, err)
		}
	}()
	return nil
}
func (e *Engine) readConnection(c *Connection) {
	defer e.readers.Done()
	buffer := e.readBufferPool.Get().(*readBuffer)
	buf := buffer.data
	defer e.readBufferPool.Put(buffer)
	if c.udp {
		// A datagram larger than the buffer would be truncated.
		buf = make([]byte, maxDatagramSize)
	}
	for {
		n, err := c.conn.Read(buf)
		if n > 0 && !c.enqueueData(buf[:n]) {
			return
		}
		if err != nil {
			c.closeWithError(err)
			return
		}
		if n == 0 && !c.udp {
			c.closeWithError(io.EOF)
			return
		}
	}
}
func (e *Engine) finishConnection(c *Connection, err error) {
	c.mu.Lock()
	if c.closeDelivered {
		c.mu.Unlock()
		return
	}
	c.closeDelivered, c.closing, c.scheduled = true, true, false
	c.mu.Unlock()
	_ = c.conn.Close()
	e.mu.Lock()
	delete(e.connections, c)
	e.mu.Unlock()
	c.handler.OnClose(c, err)
}
func (e *Engine) Stop() {
	if e.stopping.CompareAndSwap(false, true) {
		close(e.stopped)
		for _, listener := range e.listeners {
			_ = listener.Close()
		}
		for _, l := range e.udpListeners {
			_ = l.pc.Close()
		}
	}
}
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		e.Stop()
		e.mu.Lock()
		connections := make([]*Connection, 0, len(e.connections))
		for c := range e.connections {
			connections = append(connections, c)
		}
		e.mu.Unlock()
		for _, c := range connections {
			c.Close()
		}
		e.readers.Wait()
		e.taskWG.Wait()
		e.releaseTaskPool()
	})
	return nil
}

var _ io.Closer = (*Engine)(nil)
