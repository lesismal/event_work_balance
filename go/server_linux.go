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
	// maxWaitBatch caps the epoll_wait output buffer. MaxEvents keeps sizing
	// the task queue; beyond this many events per wait the loop just calls
	// epoll_wait again, so a larger buffer only costs memory per server.
	maxWaitBatch = 1024
	// maxRetainedSendBuffer bounds the per-connection outbound buffer kept for
	// reuse once its queue drains.
	maxRetainedSendBuffer = 64 << 10
	epollET               = uint32(1 << 31)
	baseEvents            = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
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
	// WriteBufferHighWatermark pauses socket reads while at least this many
	// bytes are waiting to be written. This bounds userspace buffering while
	// TCP backpressure catches up. Set below zero to disable write backpressure.
	WriteBufferHighWatermark int
	UseWritev                bool
	TaskPoolMode             taskpool.Mode
	SharedTaskPool           bool
}

func DefaultConfig() Config {
	workerCount, maxEvents := defaultPoolSizing()
	return Config{BindAddress: "0.0.0.0", Port: 9000, Backlog: 128, WorkerCount: workerCount, MaxEvents: maxEvents, ReadBufferSize: 16 * 1024, WriteBufferHighWatermark: 4 * 1024, UseWritev: true, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
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
type commandBatch struct{ items []command }
type readBuffer struct{ data []byte }
type sendBuffer struct{ data []byte }
type sendItem struct {
	data   []byte
	offset int
}

type connectionAttachment struct{ value any }

// Connection is safe to use from callback and application goroutines.
type Connection struct {
	fd             atomic.Int32
	token          uint64
	server         *Server
	mu             sync.Mutex
	pendingEvents  uint32
	sends          []sendItem
	sendHead       int
	scratch        *sendBuffer
	scratchQueued  bool
	scheduled      bool
	closing        bool
	closed         bool
	readPaused     bool
	flushing       bool
	corked         bool
	closeAfterSend bool
	pendingBytes   atomic.Int64
	attachment     atomic.Pointer[connectionAttachment]
}

func (c *Connection) FD() int { return int(c.fd.Load()) }

// Attachment returns application state associated with the connection.
func (c *Connection) Attachment() any {
	if value := c.attachment.Load(); value != nil {
		return value.value
	}
	return nil
}

// SetAttachment associates application state with the connection. Passing nil
// clears it. Protocol handlers use this to avoid a global connection-state map.
func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
		return
	}
	c.attachment.Store(&connectionAttachment{value: value})
}

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
	closeNow := c.sendHead == len(c.sends) && !c.flushing
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

// canWriteDirectlyLocked reports whether send may hand data straight to the
// socket instead of queueing it. Callers hold c.mu.
func (c *Connection) canWriteDirectlyLocked() bool {
	return c.sendHead == len(c.sends) && !c.flushing && !c.corked
}

// rewindQueueLocked restarts an emptied item queue, keeping the outbound
// buffer because a send is about to refill it. Callers hold c.mu.
func (c *Connection) rewindQueueLocked() {
	if cap(c.sends) > maxWritevItems*2 {
		c.sends = nil
	} else {
		c.sends = c.sends[:0]
	}
	c.sendHead = 0
	c.scratchQueued = false
}

// resetQueueLocked restarts an emptied queue and returns the outbound buffer.
// Callers hold c.mu.
func (c *Connection) resetQueueLocked() {
	c.rewindQueueLocked()
	c.releaseScratchLocked()
}

// acquireScratchLocked returns the connection's outbound buffer, borrowing one
// from the server pool if it does not hold one. Callers hold c.mu.
func (c *Connection) acquireScratchLocked() *sendBuffer {
	if c.scratch == nil {
		c.scratch = c.server.sendBufferPool.Get().(*sendBuffer)
	}
	return c.scratch
}

// releaseScratchLocked hands the outbound buffer back once no queued item can
// reference it. The buffers live in a server-wide pool rather than on each
// connection: a round needs one only until its replies reach the socket, so the
// server keeps roughly one per busy worker instead of one per connection.
// Callers hold c.mu.
func (c *Connection) releaseScratchLocked() {
	if c.scratch == nil {
		return
	}
	if cap(c.scratch.data) <= maxRetainedSendBuffer {
		c.scratch.data = c.scratch.data[:0]
		c.server.sendBufferPool.Put(c.scratch)
	}
	c.scratch = nil
}

// queueLocked copies the parts into the send queue. Consecutive chunks merge
// into one reusable buffer, so a round that answers several messages leaves a
// single item for writev and allocates nothing once the buffer has grown.
// Callers hold c.mu.
func (c *Connection) queueLocked(first, second []byte) {
	if c.scratchQueued && c.sends[len(c.sends)-1].offset == 0 {
		buffer := c.scratch
		buffer.data = append(append(buffer.data, first...), second...)
		// Appending may have moved the buffer. The trailing item spans all of
		// it and nothing has been written from it yet, so re-pointing the item
		// is enough and no in-flight write can be disturbed.
		c.sends[len(c.sends)-1].data = buffer.data
		return
	}
	if c.sendHead != len(c.sends) {
		// Items are still queued and one of them may reference the shared
		// buffer, so this chunk gets its own.
		queued := make([]byte, 0, len(first)+len(second))
		c.sends = append(c.sends, sendItem{data: append(append(queued, first...), second...)})
		c.scratchQueued = false
		return
	}
	// The queue is empty, so nothing references the shared buffer any more.
	c.rewindQueueLocked()
	buffer := c.acquireScratchLocked()
	buffer.data = append(append(buffer.data[:0], first...), second...)
	c.sends = append(c.sends, sendItem{data: buffer.data})
	c.scratchQueued = true
}

// queueOwnedLocked queues data the caller handed over, which cannot be merged
// into the shared buffer. Callers hold c.mu.
func (c *Connection) queueOwnedLocked(data []byte) {
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{data: data})
	c.scratchQueued = false
}

// pauseStateChangedLocked reports whether queued output has crossed a watermark
// so that the epoll read interest no longer matches it. It mirrors the decision
// refreshConnection makes, so that a refresh is only requested when the event
// loop actually has an epoll_ctl to perform. Callers hold c.mu.
func (c *Connection) pauseStateChangedLocked() bool {
	if c.server.writeHighWatermark <= 0 {
		return false
	}
	pendingBytes := c.pendingBytes.Load()
	pauseReads := pendingBytes >= int64(c.server.writeHighWatermark)
	if c.readPaused && pendingBytes > int64(c.server.writeLowWatermark) {
		pauseReads = true
	}
	return pauseReads != c.readPaused
}

// Send copies data before returning. It first attempts a direct nonblocking write.
func (c *Connection) Send(data []byte) error {
	return c.send(data, true)
}

// SendOwned sends data without copying it. Ownership transfers to the
// connection immediately; the caller must not access data after the call.
func (c *Connection) SendOwned(data []byte) error {
	return c.send(data, false)
}

func (c *Connection) send(data []byte, copyData bool) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	sent := 0
	if c.canWriteDirectlyLocked() {
		for {
			n, err := syscall.Write(c.FD(), data)
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
	queued := data[sent:]
	if copyData {
		c.queueLocked(queued, nil)
	} else {
		c.queueOwnedLocked(queued)
	}
	c.pendingBytes.Add(int64(len(queued)))
	// EPOLLOUT stays armed, so queueing alone needs no epoll change; only a
	// watermark crossing does. While corked the flush at the end of the read
	// round settles the read interest instead.
	refresh := !c.corked && c.pauseStateChangedLocked()
	c.mu.Unlock()
	if refresh {
		c.server.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// SendParts writes a two-part message without first joining the parts. If the
// socket is backpressured, only the unsent suffix is copied before returning.
func (c *Connection) SendParts(first, second []byte) error {
	total := len(first) + len(second)
	if total == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	sent := 0
	if c.canWriteDirectlyLocked() {
		for {
			n, err := writev2(c.FD(), first, second)
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
		if sent == total {
			c.mu.Unlock()
			return nil
		}
	}
	if sent < len(first) {
		c.queueLocked(first[sent:], second)
	} else {
		c.queueLocked(second[sent-len(first):], nil)
	}
	c.pendingBytes.Add(int64(total - sent))
	refresh := !c.corked && c.pauseStateChangedLocked()
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
	writeHighWatermark        int
	writeLowWatermark         int
	handler                   Handler
	stopping                  atomic.Bool
	nextToken                 atomic.Uint64
	commandMu                 sync.Mutex
	commands                  *commandBatch
	commandPool               sync.Pool
	wakePending               atomic.Bool
	connections               map[uint64]*Connection // event-loop ownership
	taskPool                  *taskpool.TaskPool
	releaseTaskPool           func()
	taskWG                    sync.WaitGroup
	readBufferPool            sync.Pool
	sendBufferPool            sync.Pool
	closeOnce                 sync.Once
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
	if config.WriteBufferHighWatermark == 0 {
		config.WriteBufferHighWatermark = 4 * 1024
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
		useWritev: config.UseWritev, writeHighWatermark: config.WriteBufferHighWatermark,
		writeLowWatermark: config.WriteBufferHighWatermark / 2, handler: handler,
		connections: make(map[uint64]*Connection)}
	s.nextToken.Store(firstConnToken)
	s.taskPool, s.releaseTaskPool = acquireTaskPool(config)
	s.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
	s.sendBufferPool.New = func() any { return &sendBuffer{} }
	if err = s.addFD(listenFD, listenerToken, uint32(syscall.EPOLLIN)|epollET); err == nil {
		err = s.addFD(wakeFD, wakeToken, uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		s.releaseTaskPool()
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
	batch := s.maxEvents
	if batch > maxWaitBatch {
		batch = maxWaitBatch
	}
	events := make([]syscall.EpollEvent, batch)
	var ready []*Connection
	var tasks []taskpool.Task
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
				if c := s.noteEvent(token, events[i].Events); c != nil {
					ready = append(ready, c)
				}
			}
		}
		if len(ready) > 0 {
			tasks = s.submitReady(ready, tasks[:0])
			clear(ready)
			ready = ready[:0]
		}
	}
	s.drainCommands()
	return nil
}

// submitReady hands one epoll round's newly runnable connections to the task
// pool in a single batch instead of one lock-and-wake cycle per connection.
func (s *Server) submitReady(ready []*Connection, tasks []taskpool.Task) []taskpool.Task {
	for _, c := range ready {
		tasks = append(tasks, c)
	}
	s.taskWG.Add(len(tasks))
	accepted := s.taskPool.GoTasks(tasks)
	for _, c := range ready[accepted:] {
		s.taskWG.Done()
		c.mu.Lock()
		c.scheduled = false
		c.mu.Unlock()
		c.Close()
	}
	clear(tasks)
	return tasks
}

func (s *Server) Stop() { s.stopping.Store(true); s.notify() }

// Close releases all resources. Run must have returned before Close is called.
func (s *Server) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.Stop()
		s.taskWG.Wait()
		s.releaseTaskPool()
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
	if s.commands == nil {
		if pooled := s.commandPool.Get(); pooled != nil {
			s.commands = pooled.(*commandBatch)
		} else {
			s.commands = &commandBatch{}
		}
	}
	s.commands.items = append(s.commands.items, cmd)
	s.commandMu.Unlock()
	s.notify()
}
func (s *Server) notify() {
	if !s.wakePending.CompareAndSwap(false, true) {
		return
	}
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
		fd, err := accept4(s.listenFD)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		token := s.nextToken.Add(1) - 1
		c := &Connection{token: token, server: s}
		c.fd.Store(int32(fd))
		// EPOLLOUT is registered up front and never modified again. The
		// descriptor is edge-triggered, so an always-armed write interest only
		// fires when the socket goes from full back to writable, which spares
		// the loop an epoll_ctl pair per backpressured message.
		if err := s.addFD(fd, token, allEvents); err != nil {
			syscall.Close(fd)
			continue
		}
		s.connections[token] = c
		s.handler.OnOpen(c)
	}
}

func accept4(listenFD int) (int, error) {
	r0, _, errno := syscall.RawSyscall6(
		syscall.SYS_ACCEPT4,
		uintptr(listenFD),
		0,
		0,
		uintptr(syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC),
		0,
		0,
	)
	if errno != 0 {
		return -1, errno
	}
	return int(r0), nil
}

// noteEvent folds readiness into the connection and reports whether it needs
// to be scheduled. Actual submission happens once per epoll round in Run.
func (s *Server) noteEvent(token uint64, events uint32) *Connection {
	c := s.connections[token]
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return nil
	}
	events &= allEvents
	if events == 0 {
		c.mu.Unlock()
		return nil
	}
	// epoll readiness is level information from the connection's point of
	// view. Coalescing duplicate notifications avoids a slice scan and keeps a
	// hot connection from allocating an unbounded event queue while its worker
	// is draining the socket.
	c.pendingEvents |= events
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if !submit {
		return nil
	}
	return c
}

// RunTask implements taskpool.Task without allocating a method value for each
// readiness notification.
func (c *Connection) RunTask() {
	defer c.server.taskWG.Done()
	c.process()
}

func (s *Server) drainCommands() {
	// A single read drains the eventfd: reading returns the whole counter and
	// resets it to zero, so looping until EAGAIN only adds a wasted syscall.
	var b [8]byte
	for {
		if _, err := syscall.Read(s.wakeFD, b[:]); err != syscall.EINTR {
			break
		}
	}
	s.wakePending.Store(false)
	s.commandMu.Lock()
	batch := s.commands
	s.commands = nil
	s.commandMu.Unlock()
	if batch == nil {
		return
	}
	for _, cmd := range batch.items {
		if cmd.kind == commandClose {
			s.closeConnection(cmd.connection, cmd.err, true)
		} else {
			s.refreshConnection(cmd.connection)
		}
	}
	for i := range batch.items {
		batch.items[i] = command{}
	}
	if cap(batch.items) <= s.maxEvents {
		batch.items = batch.items[:0]
		s.commandPool.Put(batch)
	}
}
func (s *Server) refreshConnection(c *Connection) {
	c.mu.Lock()
	usable := !c.closing && !c.closed
	pendingBytes := c.pendingBytes.Load()
	pauseReads := s.writeHighWatermark > 0 && pendingBytes >= int64(s.writeHighWatermark)
	if c.readPaused && pendingBytes > int64(s.writeLowWatermark) {
		pauseReads = true
	}
	changed := c.readPaused != pauseReads
	c.readPaused = pauseReads
	c.mu.Unlock()
	if usable && changed {
		// Write interest is permanent, so registration only tracks whether
		// reads are paused while the peer catches up.
		events := uint32(allEvents)
		if pauseReads {
			events &^= syscall.EPOLLIN
		}
		if err := s.modifyFD(c, events); err != nil {
			c.closeWithError(err)
		}
	}
}
func (s *Server) closeConnection(c *Connection, closeErr error, callback bool) {
	c.mu.Lock()
	doClose := !c.closed
	if !doClose {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closing = true
	c.sends = nil
	c.sendHead = 0
	c.scratchQueued = false
	c.releaseScratchLocked()
	c.pendingBytes.Store(0)
	c.mu.Unlock()
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
	// deferred carries readiness that this round observed but did not act on
	// because output was still queued. It is folded back into pendingEvents
	// before the next round so the notification is never lost: epoll is
	// edge-triggered, so a dropped EPOLLIN would only reappear once the peer
	// sent more data, leaving readable bytes stranded on a connection that
	// looks idle.
	var deferred uint32
	for {
		c.mu.Lock()
		c.pendingEvents |= deferred
		if c.pendingEvents == deferred {
			// Only the deferred readiness remains. Stop here instead of spinning:
			// the next EPOLLOUT resubmits the connection, flushOutput runs first,
			// and drainInput follows once the queue is empty.
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		events := c.pendingEvents
		c.pendingEvents = 0
		closed := c.closed || c.closing
		c.mu.Unlock()
		deferred = 0
		alive := !closed
		var closeErr error
		// Flush before reading so that a round which both frees socket send
		// space and delivers new input never reads while output is still
		// queued behind it. This keeps userspace buffering bounded by what the
		// peer is willing to accept instead of what it is willing to send.
		if alive && events&syscall.EPOLLOUT != 0 && c.hasQueuedOutput() {
			closeErr = c.flushOutput()
			alive = closeErr == nil
		}
		if alive && events&syscall.EPOLLPRI != 0 {
			closeErr = c.drainPriorityInput()
			alive = closeErr == nil
		}
		if alive && events&syscall.EPOLLIN != 0 {
			if c.hasQueuedOutput() {
				// Queued output means EPOLLOUT interest is registered or a flush
				// requested it, so a later round is guaranteed. Carry the read,
				// and any half-close that arrived with it, into that round so
				// the peer's final bytes are still delivered after the flush.
				deferred = syscall.EPOLLIN | events&syscall.EPOLLRDHUP
			} else {
				closeErr = c.drainInput()
				alive = closeErr == nil
			}
		}
		if alive && events&syscall.EPOLLERR != 0 {
			closeErr = c.socketError()
			alive = false
		} else if alive && events&(syscall.EPOLLHUP|syscall.EPOLLRDHUP)&^deferred != 0 {
			closeErr = io.EOF
			alive = false
		}
		if !alive {
			c.closeWithError(closeErr)
		}
	}
}

// hasQueuedOutput reports whether bytes accepted by Send are still waiting for
// the socket. Reads are deferred while it is true.
func (c *Connection) hasQueuedOutput() bool {
	c.mu.Lock()
	queued := c.sendHead != len(c.sends)
	c.mu.Unlock()
	return queued
}

// drainInput reads until the socket is empty, handing each chunk to the
// handler. Replies produced along the way are corked: rather than one write
// syscall per message they accumulate in one contiguous buffer and reach the
// socket in a single write when the round ends.
func (c *Connection) drainInput() error {
	c.mu.Lock()
	c.corked = true
	c.mu.Unlock()
	err := c.readLoop()
	if flushErr := c.uncork(); err == nil {
		err = flushErr
	}
	return err
}

// uncork flushes whatever the handler queued while the round was corked. The
// cork is dropped first so a send from another goroutine racing the flush
// writes for itself instead of waiting for a round that has already ended.
// Uncorking an already-uncorked connection does nothing, so the caller that
// ends the round does not re-flush a queue a mid-round flush already left
// behind.
func (c *Connection) uncork() error {
	c.mu.Lock()
	wasCorked := c.corked
	c.corked = false
	queued := c.sendHead != len(c.sends)
	c.mu.Unlock()
	if !wasCorked || !queued {
		return nil
	}
	return c.flushOutput()
}

// overWriteWatermark reports whether queued output has reached the budget that
// bounds how much the connection buffers in userspace.
func (c *Connection) overWriteWatermark() bool {
	return c.server.writeHighWatermark > 0 &&
		c.pendingBytes.Load() >= int64(c.server.writeHighWatermark)
}

func (c *Connection) readLoop() error {
	buffer := c.server.readBufferPool.Get().(*readBuffer)
	buf := buffer.data
	defer c.server.readBufferPool.Put(buffer)
	for {
		n, err := syscall.Read(c.FD(), buf)
		if n > 0 {
			c.server.handler.OnData(c, buf[:n])
			if c.overWriteWatermark() {
				// The replies queued so far already fill the write budget.
				// Hand them to the socket before reading on rather than
				// stopping outright: stopping would leave readable bytes
				// behind an edge that does not fire again until the peer sends
				// more, and it is the flush, not the queue depth, that says
				// whether the peer is actually keeping up.
				if flushErr := c.uncork(); flushErr != nil {
					return flushErr
				}
				if c.overWriteWatermark() {
					// The peer is behind. Stop reading; the flush has already
					// asked the loop to pause reads until the queue drains,
					// and re-arming EPOLLIN then redelivers what is left.
					return nil
				}
				c.mu.Lock()
				c.corked = true
				c.mu.Unlock()
			}
			if n < len(buf) {
				// A short read means the socket buffer is empty, so the next
				// read would only return EAGAIN. Data arriving after this
				// point raises a fresh edge, which resubmits the connection.
				return nil
			}
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
		if c.sendHead == len(c.sends) {
			c.resetQueueLocked()
			c.flushing = false
			closeAfterSend := c.closeAfterSend
			refresh := c.pauseStateChangedLocked()
			c.mu.Unlock()
			if closeAfterSend {
				c.closeWithError(nil)
				return nil
			}
			if refresh {
				c.server.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		var n int
		var err error
		attempted := 0
		pending := len(c.sends) - c.sendHead
		if c.server.useWritev && pending > 1 {
			count := pending
			if count > maxWritevItems {
				count = maxWritevItems
			}
			var batch [maxWritevItems][]byte
			buffers := batch[:count]
			for i := 0; i < count; i++ {
				item := &c.sends[c.sendHead+i]
				buffers[i] = item.data[item.offset:]
				attempted += len(buffers[i])
			}
			n, err = writev(c.FD(), buffers)
		} else {
			item := c.sends[c.sendHead]
			attempted = len(item.data) - item.offset
			n, err = syscall.Write(c.FD(), item.data[item.offset:])
		}
		if n > 0 {
			if pending := c.pendingBytes.Add(-int64(n)); pending < 0 {
				c.pendingBytes.Store(0)
			}
			left := n
			for c.sendHead < len(c.sends) {
				item := &c.sends[c.sendHead]
				remaining := len(item.data) - item.offset
				if left < remaining {
					item.offset += left
					break
				}
				left -= remaining
				c.sends[c.sendHead] = sendItem{}
				c.sendHead++
			}
			if n < attempted {
				// A short write means the socket send buffer is full, so
				// retrying now would only earn an EAGAIN. Wait for EPOLLOUT.
				c.flushing = false
				refresh := c.pauseStateChangedLocked()
				c.mu.Unlock()
				if refresh {
					c.server.request(command{kind: commandRefresh, connection: c})
				}
				return nil
			}
			c.mu.Unlock()
			continue
		}
		if err == syscall.EINTR {
			c.mu.Unlock()
			continue
		}
		c.flushing = false
		refresh := c.pauseStateChangedLocked()
		c.mu.Unlock()
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			if refresh {
				c.server.request(command{kind: commandRefresh, connection: c})
			}
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
	var iov [maxWritevItems]syscall.Iovec
	count := 0
	for _, b := range buffers {
		if len(b) > 0 {
			if count == len(iov) {
				break
			}
			iov[count] = syscall.Iovec{Base: &b[0], Len: uint64(len(b))}
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(count))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}

func writev2(fd int, first, second []byte) (int, error) {
	if len(first) == 0 {
		return syscall.Write(fd, second)
	}
	if len(second) == 0 {
		return syscall.Write(fd, first)
	}
	var iov [2]syscall.Iovec
	count := 0
	if len(first) != 0 {
		iov[count] = syscall.Iovec{Base: &first[0], Len: uint64(len(first))}
		count++
	}
	if len(second) != 0 {
		iov[count] = syscall.Iovec{Base: &second[0], Len: uint64(len(second))}
		count++
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(count))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}

var _ io.Closer = (*Server)(nil)
