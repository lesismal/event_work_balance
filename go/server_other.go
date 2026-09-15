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
	BindAddress                     string
	Port                            uint16
	Backlog, WorkerCount, MaxEvents int
	ReadBufferSize                  int
	UseWritev                       bool
}

func DefaultConfig() Config {
	workerCount, maxEvents := defaultPoolSizing()
	return Config{BindAddress: "0.0.0.0", Port: 9000, Backlog: 128, WorkerCount: workerCount, MaxEvents: maxEvents, ReadBufferSize: 16 * 1024, UseWritev: true}
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
type Connection struct {
	server                             *Server
	conn                               net.Conn
	fd                                 atomic.Int64
	mu                                 sync.Mutex
	events                             []portableEvent
	scheduled, closing, closeDelivered bool
	writeMu                            sync.Mutex
}

func (c *Connection) FD() int { return int(c.fd.Load()) }
func (c *Connection) Close()  { c.closeWithError(nil) }
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
	if submit && !c.server.taskPool.Go(c.process) {
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
	if submit && !c.server.taskPool.Go(c.process) {
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

type Server struct {
	listener       net.Listener
	handler        Handler
	taskPool       *taskpool.TaskPool
	stopping       atomic.Bool
	closeOnce      sync.Once
	mu             sync.Mutex
	connections    map[*Connection]struct{}
	readers        sync.WaitGroup
	readBufferPool sync.Pool
}

func Bind(config Config, handler Handler) (*Server, error) {
	if config.WorkerCount <= 0 {
		return nil, errors.New("worker count must be greater than zero")
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = 256
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = 16 * 1024
	}
	if config.BindAddress == "" {
		config.BindAddress = "0.0.0.0"
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(config.BindAddress, fmt.Sprint(config.Port)))
	if err != nil {
		return nil, err
	}
	s := &Server{listener: listener, handler: handler, taskPool: taskpool.New(config.WorkerCount, config.MaxEvents), connections: make(map[*Connection]struct{})}
	s.readBufferPool.New = func() any { return make([]byte, config.ReadBufferSize) }
	return s, nil
}
func (s *Server) LocalAddr() (*net.TCPAddr, error) {
	addr, ok := s.listener.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errors.New("listener is not TCP")
	}
	return addr, nil
}
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
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
	buf := s.readBufferPool.Get().([]byte)
	defer s.readBufferPool.Put(buf)
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
		_ = s.listener.Close()
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
		s.taskPool.Stop()
	})
	return nil
}

var _ io.Closer = (*Server)(nil)
