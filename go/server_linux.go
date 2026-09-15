//go:build linux

// Package epoll implements a single edge-triggered epoll event loop whose
// connections are dynamically scheduled onto a pool of logical workers.
package epoll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

const (
	listenerToken  = uint64(0)
	wakeToken      = uint64(1)
	firstConnToken = uint64(2)
	maxWritevItems = 64
	epollET        = uint32(1 << 31)
	baseEvents     = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
		syscall.EPOLLHUP|syscall.EPOLLRDHUP) | epollET
	allEvents = baseEvents | syscall.EPOLLOUT
)

// Config controls listener and worker-pool sizing.
type Config struct {
	BindAddress    string
	Port           uint16
	Backlog        int
	WorkerCount    int
	MaxEvents      int
	ReadBufferSize int
	UseWritev      bool
}

func DefaultConfig() Config {
	workerCount, maxEvents := defaultPoolSizing()
	return Config{BindAddress: "0.0.0.0", Port: 9000, Backlog: 128, WorkerCount: workerCount, MaxEvents: maxEvents, ReadBufferSize: 16 * 1024, UseWritev: true}
}

// Handler callbacks run on a logical worker, except OnOpen and OnClose which
// run on the event-loop goroutine. Data is only valid for the duration of OnData.
type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}

// HandlerFuncs allows callers to implement only the callbacks they need.
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

type commandType uint8

const (
	commandRefresh commandType = iota
	commandClose
)

type command struct {
	kind       commandType
	connection *Connection
	err        error
}
type sendItem struct {
	data   []byte
	offset int
}

// Connection is safe to use from callback and application goroutines.
type Connection struct {
	fd             atomic.Int32
	token          uint64
	server         *Server
	mu             sync.Mutex
	events         []uint32
	sends          []*sendItem
	scheduled      bool
	closing        bool
	closed         bool
	writeInterest  bool
	flushing       bool
	closeAfterSend bool
}

func (c *Connection) FD() int { return int(c.fd.Load()) }

func (c *Connection) Close() {
	c.closeWithError(nil)
}

// CloseAfterSend closes the connection after all data already accepted by Send
// has been handed to the kernel.
func (c *Connection) CloseAfterSend() {
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return
	}
	c.closeAfterSend = true
	closeNow := len(c.sends) == 0 && !c.flushing
	c.mu.Unlock()
	if closeNow {
		c.closeWithError(nil)
	}
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	request := !c.closing && !c.closed
	c.closing = true
	c.mu.Unlock()
	if request {
		c.server.request(command{kind: commandClose, connection: c, err: err})
	}
}

// Send copies data before returning. It first attempts a direct nonblocking write.
func (c *Connection) Send(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	queueWasEmpty := len(c.sends) == 0
	sent := 0
	if queueWasEmpty && !c.flushing {
		for {
			n, err := syscall.SendmsgN(c.FD(), data, nil, nil, syscall.MSG_NOSIGNAL)
			if err == syscall.EINTR {
				continue
			}
			if err != nil && err != syscall.EAGAIN && err != syscall.EWOULDBLOCK {
				c.mu.Unlock()
				c.closeWithError(err)
				return err
			}
			if err == nil {
				sent = n
			}
			break
		}
		if sent == len(data) {
			c.mu.Unlock()
			return nil
		}
	}
	copyOfData := append([]byte(nil), data[sent:]...)
	c.sends = append(c.sends, &sendItem{data: copyOfData})
	refresh := queueWasEmpty && !c.flushing
	c.mu.Unlock()
	if refresh {
		c.server.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// Server owns the listener, epoll descriptor, command queue, and worker pool.
type Server struct {
	epollFD, listenFD, wakeFD int
	maxEvents                 int
	useWritev                 bool
	handler                   Handler
	stopping                  atomic.Bool
	nextToken                 atomic.Uint64
	commandMu                 sync.Mutex
	commands                  []command
	connections               map[uint64]*Connection // event-loop ownership
	taskPool                  *taskpool.TaskPool
	readBufferPool            sync.Pool
	closeOnce                 sync.Once
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
	if config.Backlog <= 0 {
		config.Backlog = 128
	}
	if config.BindAddress == "" {
		config.BindAddress = "0.0.0.0"
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	listenFD, err := createListener(config)
	if err != nil {
		syscall.Close(epfd)
		return nil, err
	}
	wakeFD, err := eventfd()
	if err != nil {
		syscall.Close(listenFD)
		syscall.Close(epfd)
		return nil, err
	}
	s := &Server{epollFD: epfd, listenFD: listenFD, wakeFD: wakeFD, maxEvents: config.MaxEvents,
		useWritev: config.UseWritev, handler: handler, connections: make(map[uint64]*Connection)}
	s.nextToken.Store(firstConnToken)
	s.taskPool = taskpool.New(config.WorkerCount, config.MaxEvents)
	s.readBufferPool.New = func() any { return make([]byte, config.ReadBufferSize) }
	if err = s.addFD(listenFD, listenerToken, uint32(syscall.EPOLLIN)|epollET); err == nil {
		err = s.addFD(wakeFD, wakeToken, uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		s.taskPool.Stop()
		syscall.Close(wakeFD)
		syscall.Close(listenFD)
		syscall.Close(epfd)
		return nil, err
	}
	return s, nil
}

func (s *Server) LocalAddr() (*net.TCPAddr, error) {
	sa, err := syscall.Getsockname(s.listenFD)
	if err != nil {
		return nil, err
	}
	v4, ok := sa.(*syscall.SockaddrInet4)
	if !ok {
		return nil, errors.New("listener is not IPv4")
	}
	return &net.TCPAddr{IP: net.IP(v4.Addr[:]), Port: v4.Port}, nil
}

func (s *Server) Run() error {
	events := make([]syscall.EpollEvent, s.maxEvents)
	for !s.stopping.Load() {
		n, err := syscall.EpollWait(s.epollFD, events, -1)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			token := uint64(uint32(events[i].Fd)) | uint64(uint32(events[i].Pad))<<32
			switch token {
			case listenerToken:
				s.acceptConnections()
			case wakeToken:
				s.drainCommands()
			default:
				s.enqueueEvent(token, events[i].Events)
			}
		}
	}
	s.drainCommands()
	return nil
}

func (s *Server) Stop() { s.stopping.Store(true); s.notify() }

// Close releases all resources. Run must have returned before Close is called.
func (s *Server) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.Stop()
		s.taskPool.Stop()
		s.drainCommands()
		for _, c := range s.connections {
			s.closeConnection(c, nil, false)
		}
		for _, fd := range []int{s.listenFD, s.wakeFD, s.epollFD} {
			if err := syscall.Close(fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func (s *Server) request(cmd command) {
	s.commandMu.Lock()
	s.commands = append(s.commands, cmd)
	s.commandMu.Unlock()
	s.notify()
}
func (s *Server) notify() {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = syscall.Write(s.wakeFD, b[:])
}
func (s *Server) addFD(fd int, token uint64, events uint32) error {
	e := syscall.EpollEvent{Events: events, Fd: int32(token), Pad: int32(token >> 32)}
	return syscall.EpollCtl(s.epollFD, syscall.EPOLL_CTL_ADD, fd, &e)
}
func (s *Server) modifyFD(c *Connection, events uint32) error {
	e := syscall.EpollEvent{Events: events, Fd: int32(c.token), Pad: int32(c.token >> 32)}
	return syscall.EpollCtl(s.epollFD, syscall.EPOLL_CTL_MOD, c.FD(), &e)
}

func (s *Server) acceptConnections() {
	for {
		fd, _, err := syscall.Accept4(s.listenFD, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		token := s.nextToken.Add(1) - 1
		c := &Connection{token: token, server: s}
		c.fd.Store(int32(fd))
		if err := s.addFD(fd, token, baseEvents); err != nil {
			syscall.Close(fd)
			continue
		}
		s.connections[token] = c
		s.handler.OnOpen(c)
	}
}

func (s *Server) enqueueEvent(token uint64, events uint32) {
	c := s.connections[token]
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return
	}
	if events&syscall.EPOLLIN != 0 {
		for _, prior := range c.events {
			if prior&syscall.EPOLLIN != 0 {
				events &^= syscall.EPOLLIN
				break
			}
		}
	}
	events &= allEvents
	if events == 0 {
		c.mu.Unlock()
		return
	}
	c.events = append(c.events, events)
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if submit && !s.submit(c) {
		c.mu.Lock()
		c.scheduled = false
		c.mu.Unlock()
		c.Close()
	}
}

func (s *Server) submit(c *Connection) bool {
	return s.taskPool.Go(c.process)
}

func (s *Server) drainCommands() {
	var b [8]byte
	for {
		if _, err := syscall.Read(s.wakeFD, b[:]); err != nil {
			break
		}
	}
	s.commandMu.Lock()
	commands := s.commands
	s.commands = nil
	s.commandMu.Unlock()
	for _, cmd := range commands {
		if cmd.kind == commandClose {
			s.closeConnection(cmd.connection, cmd.err, true)
		} else {
			s.refreshConnection(cmd.connection)
		}
	}
}
func (s *Server) refreshConnection(c *Connection) {
	c.mu.Lock()
	hasOutput := len(c.sends) != 0
	usable := !c.closing && !c.closed
	changed := c.writeInterest != hasOutput
	c.writeInterest = hasOutput
	c.mu.Unlock()
	if usable && changed {
		events := uint32(baseEvents)
		if hasOutput {
			events |= syscall.EPOLLOUT
		}
		if err := s.modifyFD(c, events); err != nil {
			c.closeWithError(err)
		}
	}
}
func (s *Server) closeConnection(c *Connection, closeErr error, callback bool) {
	c.mu.Lock()
	doClose := !c.closed
	c.closed = true
	c.closing = true
	c.mu.Unlock()
	if !doClose {
		return
	}
	fd := int(c.fd.Swap(-1))
	if fd >= 0 {
		_ = syscall.EpollCtl(s.epollFD, syscall.EPOLL_CTL_DEL, fd, nil)
		_ = syscall.Close(fd)
	}
	delete(s.connections, c.token)
	if callback {
		s.handler.OnClose(c, closeErr)
	}
}

func (c *Connection) process() {
	defer func() {
		if recovered := recover(); recovered != nil {
			// TaskPool isolates task panics. Roll back connection ownership before
			// propagating the panic to the pool, otherwise scheduled would remain
			// true and this connection could never be submitted again.
			c.mu.Lock()
			c.scheduled = false
			c.mu.Unlock()
			c.closeWithError(fmt.Errorf("handler panic: %v", recovered))
			panic(recovered)
		}
	}()
	for {
		c.mu.Lock()
		if len(c.events) == 0 {
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		events := c.events[0]
		c.events = c.events[1:]
		closed := c.closed || c.closing
		c.mu.Unlock()
		alive := !closed
		var closeErr error
		if alive && events&syscall.EPOLLPRI != 0 {
			closeErr = c.drainPriorityInput()
			alive = closeErr == nil
		}
		if alive && events&syscall.EPOLLIN != 0 {
			closeErr = c.drainInput()
			alive = closeErr == nil
		}
		if alive && events&syscall.EPOLLOUT != 0 {
			closeErr = c.flushOutput()
			alive = closeErr == nil
		}
		if alive && events&syscall.EPOLLERR != 0 {
			closeErr = c.socketError()
			alive = false
		} else if alive && events&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0 {
			closeErr = io.EOF
			alive = false
		}
		if !alive {
			c.closeWithError(closeErr)
		}
	}
}
func (c *Connection) drainInput() error {
	buf := c.server.readBufferPool.Get().([]byte)
	defer c.server.readBufferPool.Put(buf)
	for {
		n, err := syscall.Read(c.FD(), buf)
		if n > 0 {
			c.server.handler.OnData(c, buf[:n])
			continue
		}
		if n == 0 && err == nil {
			return io.EOF
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			return nil
		}
		return err
	}
}
func (c *Connection) drainPriorityInput() error {
	buf := []byte{0}
	for {
		n, _, err := syscall.Recvfrom(c.FD(), buf, syscall.MSG_OOB)
		if n > 0 {
			c.server.handler.OnPriorityData(c, buf[:n])
			continue
		}
		if n == 0 && err == nil {
			return io.EOF
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINVAL {
			return nil
		}
		return err
	}
}
func (c *Connection) flushOutput() error {
	c.mu.Lock()
	if c.closing || c.closed || c.flushing {
		usable := !c.closing && !c.closed
		c.mu.Unlock()
		if usable {
			return nil
		}
		return syscall.EPIPE
	}
	c.flushing = true
	c.mu.Unlock()
	for {
		c.mu.Lock()
		if len(c.sends) == 0 {
			c.flushing = false
			closeAfterSend := c.closeAfterSend
			c.mu.Unlock()
			if closeAfterSend {
				c.closeWithError(nil)
				return nil
			}
			c.server.request(command{kind: commandRefresh, connection: c})
			return nil
		}
		var n int
		var err error
		if c.server.useWritev {
			count := len(c.sends)
			if count > maxWritevItems {
				count = maxWritevItems
			}
			buffers := make([][]byte, count)
			for i := 0; i < count; i++ {
				buffers[i] = c.sends[i].data[c.sends[i].offset:]
			}
			n, err = writev(c.FD(), buffers)
		} else {
			item := c.sends[0]
			n, err = syscall.Write(c.FD(), item.data[item.offset:])
		}
		if n > 0 {
			left := n
			for len(c.sends) > 0 {
				item := c.sends[0]
				remaining := len(item.data) - item.offset
				if left < remaining {
					item.offset += left
					break
				}
				left -= remaining
				c.sends[0] = nil
				c.sends = c.sends[1:]
			}
			c.mu.Unlock()
			continue
		}
		if err == syscall.EINTR {
			c.mu.Unlock()
			continue
		}
		c.flushing = false
		c.mu.Unlock()
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			c.server.request(command{kind: commandRefresh, connection: c})
			return nil
		}
		return err
	}
}

func (c *Connection) socketError() error {
	errno, err := syscall.GetsockoptInt(c.FD(), syscall.SOL_SOCKET, syscall.SO_ERROR)
	if err != nil {
		return err
	}
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return syscall.ECONNRESET
}

func createListener(config Config) (int, error) {
	ip := net.ParseIP(config.BindAddress).To4()
	if ip == nil {
		return -1, fmt.Errorf("invalid IPv4 bind address %q", config.BindAddress)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	sa := &syscall.SockaddrInet4{Port: int(config.Port)}
	copy(sa.Addr[:], ip)
	if err = syscall.Bind(fd, sa); err == nil {
		err = syscall.Listen(fd, config.Backlog)
	}
	if err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}
func eventfd() (int, error) {
	r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(syscall.O_NONBLOCK|syscall.O_CLOEXEC), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r0), nil
}
func writev(fd int, buffers [][]byte) (int, error) {
	iov := make([]syscall.Iovec, 0, len(buffers))
	for _, b := range buffers {
		if len(b) > 0 {
			iov = append(iov, syscall.Iovec{Base: &b[0], Len: uint64(len(b))})
		}
	}
	if len(iov) == 0 {
		return 0, nil
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}

var _ io.Closer = (*Server)(nil)
