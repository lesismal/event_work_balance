//go:build !linux

package epoll

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

type Config struct {
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". An empty Network means "tcp", and an
	// empty Addr means ":0".
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares its connection table, task pool and buffer pool
	// across all of them.
	Addrs                           []string
	Backlog, WorkerCount, MaxEvents int
	ReadBufferSize                  int
	WriteBufferHighWatermark        int
	UseWritev                       bool
	TaskPoolMode                    taskpool.Mode
	SharedTaskPool                  bool
}

func DefaultConfig() Config {
	workerCount, maxEvents := defaultPoolSizing()
	return Config{Network: "tcp", Addr: ":9000", Backlog: 128, WorkerCount: workerCount, MaxEvents: maxEvents, ReadBufferSize: 16 * 1024, WriteBufferHighWatermark: 4 * 1024, UseWritev: true, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
}

type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}
type HandlerFuncs struct {
	Open         func(*Connection)
	Data         func(*Connection, []byte)
	PriorityData func(*Connection, []byte)
	Close        func(*Connection, error)
}

func (h HandlerFuncs) OnOpen(c *Connection) {
	if h.Open != nil {
		h.Open(c)
	}
}
func (h HandlerFuncs) OnData(c *Connection, b []byte) {
	if h.Data != nil {
		h.Data(c, b)
	}
}
func (h HandlerFuncs) OnPriorityData(c *Connection, b []byte) {
	if h.PriorityData != nil {
		h.PriorityData(c, b)
	}
}
func (h HandlerFuncs) OnClose(c *Connection, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}

type portableEvent struct {
	data     []byte
	closeErr error
	closing  bool
}
type readBuffer struct{ data []byte }
type connectionAttachment struct{ value any }
type Connection struct {
	server                             *Server
	conn                               net.Conn
	fd                                 atomic.Int64
	mu                                 sync.Mutex
	events                             []portableEvent
	scheduled, closing, closeDelivered bool
	writeMu                            sync.Mutex
	attachment                         atomic.Pointer[connectionAttachment]
}

func (c *Connection) FD() int { return int(c.fd.Load()) }
func (c *Connection) Close()  { c.closeWithError(nil) }
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
	if submit && !c.server.submit(c) {
		c.server.finishConnection(c, err)
	}
}
func (c *Connection) Send(data []byte) error {
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
	return nil
}

// SendOwned is equivalent to Send on the synchronous portable backend.
func (c *Connection) SendOwned(data []byte) error { return c.Send(data) }

func (c *Connection) SendParts(first, second []byte) error {
	data := make([]byte, len(first)+len(second))
	n := copy(data, first)
	copy(data[n:], second)
	return c.Send(data)
}
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
	if submit && !c.server.submit(c) {
		c.closeWithError(errors.New("task pool stopped"))
		return false
	}
	return true
}
func (c *Connection) process() {
	defer func() {
		if r := recover(); r != nil {
			c.server.finishConnection(c, fmt.Errorf("handler panic: %v", r))
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
			c.server.finishConnection(c, event.closeErr)
			return
		}
		c.server.handler.OnData(c, event.data)
	}
}

func (c *Connection) RunTask() {
	defer c.server.taskWG.Done()
	c.process()
}

type Server struct {
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

func Bind(config Config, handler Handler) (*Server, error) {
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
	s := &Server{listeners: listeners, handler: handler, taskPool: pool, releaseTaskPool: releasePool, connections: make(map[*Connection]struct{})}
	s.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
	return s, nil
}

func (s *Server) submit(c *Connection) bool {
	s.taskWG.Add(1)
	if !s.taskPool.GoTask(c) {
		s.taskWG.Done()
		return false
	}
	return true
}

// LocalAddr returns the address of the server's first listener.
func (s *Server) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := s.LocalAddrs()
	if err != nil {
		return nil, err
	}
	return addrs[0], nil
}

// LocalAddrs returns one address per listener, in configured order.
func (s *Server) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(s.listeners))
	for _, listener := range s.listeners {
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
func (s *Server) Run() error {
	if len(s.listeners) == 1 {
		return s.serve(s.listeners[0])
	}
	errs := make(chan error, len(s.listeners))
	var serving sync.WaitGroup
	for _, listener := range s.listeners {
		serving.Add(1)
		go func(listener net.Listener) {
			defer serving.Done()
			errs <- s.serve(listener)
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

func (s *Server) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		c := &Connection{server: s, conn: conn}
		c.fd.Store(-1)
		s.mu.Lock()
		s.connections[c] = struct{}{}
		s.mu.Unlock()
		s.handler.OnOpen(c)
		s.readers.Add(1)
		go s.readConnection(c)
	}
}
func (s *Server) readConnection(c *Connection) {
	defer s.readers.Done()
	buffer := s.readBufferPool.Get().(*readBuffer)
	buf := buffer.data
	defer s.readBufferPool.Put(buffer)
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
func (s *Server) finishConnection(c *Connection, err error) {
	c.mu.Lock()
	if c.closeDelivered {
		c.mu.Unlock()
		return
	}
	c.closeDelivered, c.closing, c.scheduled = true, true, false
	c.mu.Unlock()
	_ = c.conn.Close()
	s.mu.Lock()
	delete(s.connections, c)
	s.mu.Unlock()
	s.handler.OnClose(c, err)
}
func (s *Server) Stop() {
	if s.stopping.CompareAndSwap(false, true) {
		for _, listener := range s.listeners {
			_ = listener.Close()
		}
	}
}
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.Stop()
		s.mu.Lock()
		connections := make([]*Connection, 0, len(s.connections))
		for c := range s.connections {
			connections = append(connections, c)
		}
		s.mu.Unlock()
		for _, c := range connections {
			c.Close()
		}
		s.readers.Wait()
		s.taskWG.Wait()
		s.releaseTaskPool()
	})
	return nil
}

var _ io.Closer = (*Server)(nil)
