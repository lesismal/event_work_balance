//go:build !linux

package fib

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lesismal/fib/go/taskpool"
)

type Engine struct {
	listeners       []net.Listener
	handler         Handler
	taskPool        *taskpool.TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	stopping        atomic.Bool
	closeOnce       sync.Once
	mu              sync.Mutex
	connections     map[*Connection]struct{}
	readers         sync.WaitGroup
	readBufferPool  sync.Pool
}

func Bind(config Config, handler Handler) (*Engine, error) {
	if config.WorkerCount <= 0 {
		return nil, errors.New("worker count must be greater than zero")
	}
	if !config.TaskPoolMode.Valid() {
		return nil, fmt.Errorf("invalid task pool mode %d", config.TaskPoolMode)
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
	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
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
	e := &Engine{listeners: listeners, handler: handler, taskPool: pool, releaseTaskPool: releasePool, connections: make(map[*Connection]struct{})}
	e.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
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
	return addrs[0], nil
}

// LocalAddrs returns one address per listener, in configured order.
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
	if len(e.listeners) == 1 {
		return e.serve(e.listeners[0])
	}
	errs := make(chan error, len(e.listeners))
	var serving sync.WaitGroup
	for _, listener := range e.listeners {
		serving.Add(1)
		go func(listener net.Listener) {
			defer serving.Done()
			errs <- e.serve(listener)
		}(listener)
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
		c := &Connection{engine: e, conn: conn}
		c.fd.Store(-1)
		e.mu.Lock()
		e.connections[c] = struct{}{}
		e.mu.Unlock()
		e.handler.OnOpen(c)
		e.readers.Add(1)
		go e.readConnection(c)
	}
}
func (e *Engine) readConnection(c *Connection) {
	defer e.readers.Done()
	buffer := e.readBufferPool.Get().(*readBuffer)
	buf := buffer.data
	defer e.readBufferPool.Put(buffer)
	for {
		n, err := c.conn.Read(buf)
		if n > 0 && !c.enqueueData(buf[:n]) {
			return
		}
		if err != nil {
			c.closeWithError(err)
			return
		}
		if n == 0 {
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
	e.handler.OnClose(c, err)
}
func (e *Engine) Stop() {
	if e.stopping.CompareAndSwap(false, true) {
		for _, listener := range e.listeners {
			_ = listener.Close()
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
