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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

const (
	// A token's low half is always a descriptor and its high half says what
	// that descriptor is: one of the server's listeners, the wake-up eventfd,
	// or a connection. For a connection the high half doubles as a generation,
	// so an event left over from a descriptor's previous owner resolves to
	// nothing. Tagging rather than reserving whole token values is what lets a
	// server carry any number of listeners.
	listenerKind = uint64(0)
	wakeKind     = uint64(1)
	// firstGeneration seeds the generation half of a connection token. Starting
	// above the reserved kinds keeps every connection token distinct from a
	// listener or wake token whatever descriptor it lands on.
	firstGeneration = uint64(2)
	maxWritevItems  = 64
	// maxWaitBatch caps the epoll_wait output buffer. MaxEvents keeps sizing
	// the task queue; beyond this many events per wait the loop just calls
	// epoll_wait again, so a larger buffer only costs memory per server.
	maxWaitBatch = 1024
	// connPageShift sizes one page of the descriptor table. 4096 entries is
	// 32KB per page, small enough that a server holding few connections in a
	// wide descriptor range wastes little, large enough that the page
	// directory stays tiny.
	connPageShift = 12
	connPageSize  = 1 << connPageShift
	connPageMask  = connPageSize - 1
	// defaultWriteHighWatermark is the per-connection outbound budget. It is
	// one read round's worth of replies, which is the size that balances the
	// two costs either side of it. Lower, and ordinary traffic crosses it on
	// every reply, and each crossing costs an eventfd write and an epoll_ctl:
	// at 4KB that churn was 16% of a 100k-connection profile. Higher, and the
	// buffer holding those bytes grows past what the pool will retain, so it is
	// dropped and reallocated every round: at 64KB that treadmill allocated
	// 13.7GB across a 15-second rate test against the same run's 254MB live.
	defaultWriteHighWatermark = 16 << 10
	// defaultMaxPendingBytes is the server-wide outbound budget. It is the
	// bound that actually holds at high connection counts, where the
	// per-connection watermark alone would admit gigabytes in aggregate.
	defaultMaxPendingBytes = 64 << 20
	epollET                = uint32(1 << 31)
	baseEvents             = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
		syscall.EPOLLHUP|syscall.EPOLLRDHUP) | epollET
	allEvents = baseEvents | syscall.EPOLLOUT
)

// Config controls listener and worker-pool sizing.
type Config struct {
	BindAddress string
	Port        uint16
	// Ports, when it is not empty, is the complete set of ports to listen on
	// and Port is ignored. One server spanning several ports shares a single
	// event loop, descriptor table, task pool and buffer pool across all of
	// them, where a server per port gives each its own copy of all four.
	Ports          []uint16
	Backlog        int
	WorkerCount    int
	MaxEvents      int
	ReadBufferSize int
	// WriteBufferHighWatermark pauses socket reads while at least this many
	// bytes are waiting to be written. This bounds userspace buffering while
	// TCP backpressure catches up. Set below zero to disable write backpressure.
	//
	// Crossing it is not free: the worker hands a command to the event loop,
	// which costs an eventfd write and an epoll_ctl. A watermark near the
	// message size makes every reply a crossing, so keep it well above one
	// round's worth of output.
	WriteBufferHighWatermark int
	// MaxPendingBytes caps the bytes this server may hold across all of its
	// connections waiting for their sockets. WriteBufferHighWatermark bounds a
	// single connection, which at 100k connections still admits a per-server
	// total of watermark*100k; this is the bound on the sum. Reads pause on
	// every connection while the budget is exhausted. Zero means unlimited.
	MaxPendingBytes int64
	UseWritev       bool
	TaskPoolMode    taskpool.Mode
	SharedTaskPool  bool
	// InlineHandlers runs a ready connection's round on the event loop instead
	// of handing it to a worker.
	//
	// The handoff is not free, and at high message rates it is the dominant
	// cost: it makes a goroutine runnable, and that goroutine has to be given a
	// P before it can issue the read. An execution trace of a 100k-connection
	// echo run measured 872 seconds of runnable-but-not-running time in a
	// 2-second window, almost all of it on workers woken from the loop.
	// Skipping the handoff measured 446k echoes/s against 395k for the same
	// build with workers, and 104k accepted connections/s against 95k.
	//
	// The cost is that a handler now blocks its whole server: the loop cannot
	// collect events, accept, or serve any other connection while it runs. Set
	// this only when every handler is short and never blocks. Handlers that do
	// I/O, take contended locks, or run unbounded work want the worker pool,
	// which exists precisely so that one slow connection cannot stall the rest.
	InlineHandlers bool
}

func DefaultConfig() Config {
	workerCount, maxEvents := defaultPoolSizing()
	return Config{BindAddress: "0.0.0.0", Port: 9000, Backlog: defaultBacklog(), WorkerCount: workerCount,
		MaxEvents: maxEvents, ReadBufferSize: 16 * 1024,
		WriteBufferHighWatermark: defaultWriteHighWatermark, MaxPendingBytes: defaultMaxPendingBytes,
		UseWritev: true, TaskPoolMode: taskpool.ModeElastic, SharedTaskPool: true}
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

// sendItem is one queued chunk. buf is the pooled buffer backing data, or nil
// when the caller handed over an array the connection does not own. Returning
// buf to the pool as soon as the item drains is what keeps a backpressured
// connection from allocating a fresh array per reply.
type sendItem struct {
	data   []byte
	offset int
	buf    *sendBuffer
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
	scheduled      bool
	closing        bool
	closed         bool
	readPaused     bool
	budgetPaused   bool
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

// rewindQueueLocked restarts an emptied item queue. Every item has already
// surrendered its buffer as it drained, so there is nothing left to return.
// Callers hold c.mu.
func (c *Connection) rewindQueueLocked() {
	if cap(c.sends) > maxWritevItems*2 {
		c.sends = nil
	} else {
		c.sends = c.sends[:0]
	}
	c.sendHead = 0
}

// resetQueueLocked restarts an emptied queue. Callers hold c.mu.
func (c *Connection) resetQueueLocked() {
	c.rewindQueueLocked()
}

// releaseItemLocked returns a drained item's buffer to the server pool. An
// outsized buffer is dropped instead so that one large message does not leave
// every pooled buffer permanently inflated. Callers hold c.mu.
func (c *Connection) releaseItemLocked(item *sendItem) {
	if item.buf != nil {
		c.server.releaseSendBuffer(item.buf)
	}
	*item = sendItem{}
}

// acquireSendBuffer returns a pooled outbound buffer.
//
// One size for every buffer is deliberate. Sizing buffers to the backlog was
// measured twice and helped neither: a 4KB class left 736MB of buffers about a
// third full, and stepping down to 1KB classes made it worse still at 1181MB,
// because several pools each retain their own idle buffers. Resident peak did
// not move for any of them, so the bound that matters is MaxPendingBytes, not
// the shape of the buffers underneath it.
func (s *Server) acquireSendBuffer() *sendBuffer {
	return s.sendBufferPool.Get().(*sendBuffer)
}

// releaseSendBuffer hands a buffer back. One larger than the retention limit is
// dropped, so that a single big message cannot leave every pooled buffer
// permanently inflated.
func (s *Server) releaseSendBuffer(b *sendBuffer) {
	if cap(b.data) <= s.retainedSendBuffer {
		b.data = b.data[:0]
		s.sendBufferPool.Put(b)
	}
}

// queueLocked copies the parts into the send queue. Consecutive chunks merge
// into the trailing item's buffer, so a round that answers several messages
// leaves a single item for the socket and allocates nothing once that buffer
// has grown. Callers hold c.mu.
func (c *Connection) queueLocked(first, second []byte) {
	if n := len(c.sends); n > 0 {
		tail := &c.sends[n-1]
		// Merging is only safe while the socket has taken nothing from the
		// item: appending may move the array, and re-pointing an item a write
		// has already consumed part of would disturb that write. It also has
		// to fit: growing past the pooled capacity would both reallocate and
		// produce a buffer too large to hand back, so a round's replies would
		// allocate their way up the size classes and throw the result away.
		// Starting a new item instead keeps every buffer poolable, and writev
		// still hands the whole round to the socket in one call.
		if tail.buf != nil && tail.offset == 0 &&
			len(tail.buf.data)+len(first)+len(second) <= cap(tail.buf.data) {
			tail.buf.data = append(append(tail.buf.data, first...), second...)
			tail.data = tail.buf.data
			return
		}
	}
	if c.sendHead == len(c.sends) {
		// Nothing is queued any more, so the item slice can start over.
		c.rewindQueueLocked()
	}
	buf := c.server.acquireSendBuffer()
	buf.data = append(append(buf.data[:0], first...), second...)
	c.sends = append(c.sends, sendItem{data: buf.data, buf: buf})
}

// queueOwnedLocked queues data the caller handed over. The connection does not
// own the array, so the item carries no pooled buffer and later chunks cannot
// merge into it. Callers hold c.mu.
func (c *Connection) queueOwnedLocked(data []byte) {
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{data: data})
}

// pauseStateChangedLocked reports whether queued output has crossed a watermark
// so that the epoll read interest no longer matches it. It mirrors the decision
// refreshConnection makes, so that a refresh is only requested when the event
// loop actually has an epoll_ctl to perform. Callers hold c.mu.
func (c *Connection) pauseStateChangedLocked() bool {
	pause, _ := c.pauseDecision(c.readPaused)
	return pause != c.readPaused
}

// pauseDecision is the single definition of whether a connection's reads should
// be paused, used by the worker to decide whether a refresh is worth asking for
// and by the event loop to carry it out, so the two cannot disagree. The second
// result reports that the server-wide budget, rather than this connection's own
// backlog, is what forces the pause: such a connection may have nothing left to
// flush and so cannot re-evaluate on its own, and the event loop has to wake it
// once the budget recovers.
func (c *Connection) pauseDecision(readPaused bool) (pause, byBudget bool) {
	s := c.server
	if s.maxPendingBytes > 0 && s.pendingTotal.Load() >= s.maxPendingBytes {
		return true, true
	}
	if s.writeHighWatermark <= 0 {
		return false, false
	}
	pendingBytes := c.pendingBytes.Load()
	if pendingBytes >= int64(s.writeHighWatermark) {
		return true, false
	}
	// Hysteresis: once paused, stay paused until the backlog falls well below
	// the watermark. Resuming at the watermark itself would make every reply
	// re-cross it, and each crossing costs an eventfd write and an epoll_ctl.
	return readPaused && pendingBytes > int64(s.writeLowWatermark), false
}

// addPending grows both the connection's outbound backlog and the server-wide
// total, which are kept in step so that the budget is always the sum of its
// connections.
func (c *Connection) addPending(n int64) {
	if n <= 0 {
		return
	}
	c.pendingBytes.Add(n)
	c.server.pendingTotal.Add(n)
}

// subPending shrinks both counters. If the connection counter would go below
// zero the decrement is trimmed to what was actually there, so a clamp on one
// counter cannot let the other drift.
func (c *Connection) subPending(n int64) {
	if n <= 0 {
		return
	}
	if pending := c.pendingBytes.Add(-n); pending < 0 {
		c.pendingBytes.Store(0)
		n += pending
	}
	if n > 0 {
		c.server.pendingTotal.Add(-n)
	}
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
	c.addPending(int64(len(queued)))
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
	c.addPending(int64(total - sent))
	refresh := !c.corked && c.pauseStateChangedLocked()
	c.mu.Unlock()
	if refresh {
		c.server.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// Server owns the listener, epoll descriptor, command queue, and worker pool.
type Server struct {
	epollFD, wakeFD    int
	listenFDs          []int
	maxEvents          int
	useWritev          bool
	inlineHandlers     bool
	writeHighWatermark int
	writeLowWatermark  int
	maxPendingBytes    int64
	budgetResumeBytes  int64
	retainedSendBuffer int
	handler            Handler
	stopping           atomic.Bool
	nextGeneration     atomic.Uint64
	pendingTotal       atomic.Int64
	commandMu          sync.Mutex
	commands           *commandBatch
	commandPool        sync.Pool
	wakePending        atomic.Bool
	// connections is a paged table indexed by file descriptor: a descriptor is
	// a small dense integer the kernel already allocates, so the lookup on
	// every epoll event is two bounds checks rather than a hash.
	//
	// It is paged rather than flat because descriptors are handed out per
	// process while this table is per server. A process running one server per
	// listening port sees every server's descriptors drawn from one
	// interleaved range, so a flat table would grow to the highest descriptor
	// in the process no matter how few connections this server holds, and
	// doubling its way there copied 102MB across a 100k-connection dial.
	// Pages are allocated once, on demand, and never copied.
	// Event-loop ownership.
	connections [][]*Connection
	// budgetPaused holds connections whose reads the server-wide budget stopped,
	// waiting to be resumed once it recovers. Event-loop ownership.
	budgetPaused []*Connection
	// budgetResume is the spare list resumeBudgetPaused swaps in while it walks
	// the current one. Event-loop ownership.
	budgetResume    []*Connection
	taskPool        *taskpool.TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	readBufferPool  sync.Pool
	sendBufferPool  sync.Pool
	closeOnce       sync.Once
}

func listenerToken(fd int) uint64 { return uint64(uint32(fd)) | listenerKind<<32 }
func wakeToken(fd int) uint64     { return uint64(uint32(fd)) | wakeKind<<32 }

// connectionFor returns the connection a token refers to, or nil if the token
// is stale. The low half is the descriptor and the high half a generation, so a
// reused descriptor never resolves to the connection that previously held it.
func (s *Server) connectionFor(token uint64) *Connection {
	fd := int(uint32(token))
	page := fd >> connPageShift
	if page < 0 || page >= len(s.connections) {
		return nil
	}
	entries := s.connections[page]
	if entries == nil {
		return nil
	}
	c := entries[fd&connPageMask]
	if c == nil || c.token != token {
		return nil
	}
	return c
}

// trackConnection records a newly accepted connection, growing the descriptor
// table to cover it. Callers run on the event loop.
func (s *Server) trackConnection(fd int, c *Connection) {
	page := fd >> connPageShift
	if page >= len(s.connections) {
		// Only the page directory is ever copied, and it holds one pointer per
		// 4096 descriptors, so growing it stays cheap however high descriptors
		// climb.
		directory := make([][]*Connection, max(page+1, 2*len(s.connections)))
		copy(directory, s.connections)
		s.connections = directory
	}
	if s.connections[page] == nil {
		s.connections[page] = make([]*Connection, connPageSize)
	}
	s.connections[page][fd&connPageMask] = c
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
		config.Backlog = defaultBacklog()
	}
	if config.BindAddress == "" {
		config.BindAddress = "0.0.0.0"
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}

	ports := config.Ports
	if len(ports) == 0 {
		ports = []uint16{config.Port}
	}

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	listenFDs := make([]int, 0, len(ports))
	closeListeners := func() {
		for _, fd := range listenFDs {
			syscall.Close(fd)
		}
	}
	for _, port := range ports {
		listenFD, listenErr := createListener(config, port)
		if listenErr != nil {
			closeListeners()
			syscall.Close(epfd)
			return nil, listenErr
		}
		listenFDs = append(listenFDs, listenFD)
	}
	wakeFD, err := eventfd()
	if err != nil {
		closeListeners()
		syscall.Close(epfd)
		return nil, err
	}
	s := &Server{epollFD: epfd, listenFDs: listenFDs, wakeFD: wakeFD, maxEvents: config.MaxEvents,
		useWritev: config.UseWritev, inlineHandlers: config.InlineHandlers,
		writeHighWatermark: config.WriteBufferHighWatermark,
		// Resume at a quarter of the budget rather than at the budget itself,
		// so recovery admits a useful amount of work instead of re-pausing on
		// the first reply.
		writeLowWatermark: config.WriteBufferHighWatermark / 4,
		maxPendingBytes:   config.MaxPendingBytes,
		budgetResumeBytes: config.MaxPendingBytes / 4,
		// Retention is sized to one read round's replies, not to the write
		// watermark. The watermark bounds the bytes a connection may have
		// pending; it does not bound the capacity of the buffer holding them,
		// and a buffer that grew to the watermark during a burst would
		// otherwise be pooled at that size and handed to the next connection.
		// At high connection counts that capacity, not the pending bytes, is
		// what the process actually pays for: retaining at the watermark held
		// 908MB of buffers against 256MB of pending data.
		//
		// The allowance is twice the round size because replies carry framing
		// on top of the bytes that arrived. Retaining at exactly the round size
		// would drop the buffer every round and allocate a new one next round.
		retainedSendBuffer: 2 * config.ReadBufferSize,
		handler:            handler}
	s.nextGeneration.Store(firstGeneration)
	s.taskPool, s.releaseTaskPool = acquireTaskPool(config)
	s.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
	// Pooled outbound buffers start at the size a full round's replies actually
	// reach, which is the bytes that arrived plus the framing put back on top
	// of them, not the read buffer size alone. Starting any smaller costs a
	// reallocation per round on every connection, and starting from empty costs
	// one per doubling: empty cost 49.5GB of allocation across a 15-second rate
	// test, and an exact-fit 16KB still cost 12.4GB. Matching the retention
	// limit means a buffer that has grown is still handed back to the pool
	// rather than dropped.
	s.sendBufferPool.New = func() any {
		return &sendBuffer{data: make([]byte, 0, s.retainedSendBuffer)}
	}
	for _, fd := range listenFDs {
		if err = s.addFD(fd, listenerToken(fd), uint32(syscall.EPOLLIN)|epollET); err != nil {
			break
		}
	}
	if err == nil {
		err = s.addFD(wakeFD, wakeToken(wakeFD), uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		s.releaseTaskPool()
		syscall.Close(wakeFD)
		closeListeners()
		syscall.Close(epfd)
		return nil, err
	}
	return s, nil
}

// LocalAddr returns the address of the server's first listener.
func (s *Server) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := s.LocalAddrs()
	if err != nil {
		return nil, err
	}
	return addrs[0], nil
}

// LocalAddrs returns one address per listener, in configured order. Ports left
// at zero report the port the kernel chose.
func (s *Server) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(s.listenFDs))
	for _, fd := range s.listenFDs {
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			return nil, err
		}
		v4, ok := sa.(*syscall.SockaddrInet4)
		if !ok {
			return nil, errors.New("listener is not IPv4")
		}
		addrs = append(addrs, &net.TCPAddr{IP: net.IP(v4.Addr[:]), Port: v4.Port})
	}
	return addrs, nil
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
			switch token >> 32 {
			case listenerKind:
				s.acceptConnections(int(uint32(token)))
			case wakeKind:
				s.drainCommands()
			default:
				if c := s.noteEvent(token, events[i].Events); c != nil {
					ready = append(ready, c)
				}
			}
		}
		if len(ready) > 0 {
			if s.inlineHandlers {
				for _, c := range ready {
					c.process()
				}
			} else {
				tasks = s.submitReady(ready, tasks[:0])
			}
			clear(ready)
			ready = ready[:0]
		}
		s.resumeBudgetPaused()
	}
	s.drainCommands()
	return nil
}

// resumeBudgetPaused re-arms reads on connections the server-wide budget held
// back, once enough of that budget has drained. They are re-examined rather
// than resumed outright: a connection that has since built a backlog of its own
// stays paused on its own account, and simply goes back on the list.
func (s *Server) resumeBudgetPaused() {
	if len(s.budgetPaused) == 0 || s.pendingTotal.Load() > s.budgetResumeBytes {
		return
	}
	// Swap in the spare list before refreshing. A connection that is still held
	// back goes straight back onto s.budgetPaused, which therefore must not
	// share an array with the one being iterated.
	waiting := s.budgetPaused
	s.budgetPaused = s.budgetResume[:0]
	for _, c := range waiting {
		// Clear the flag first: it is what stops a connection already on the
		// list from being added twice, so leaving it set would drop a
		// connection that turns out to still need the budget.
		c.mu.Lock()
		c.budgetPaused = false
		c.mu.Unlock()
		s.refreshConnection(c)
	}
	clear(waiting)
	s.budgetResume = waiting
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
		for _, entries := range s.connections {
			for _, c := range entries {
				if c != nil {
					s.closeConnection(c, nil, false)
				}
			}
		}
		s.budgetPaused = nil
		for _, fd := range append(append([]int(nil), s.listenFDs...), s.wakeFD, s.epollFD) {
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

func (s *Server) acceptConnections(listenFD int) {
	for {
		fd, err := accept4(listenFD)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		// The token pairs the descriptor with a generation: the descriptor
		// indexes the table, and the generation makes an event left over from a
		// previous owner of the same descriptor resolve to nothing.
		token := uint64(uint32(fd)) | s.nextGeneration.Add(1)<<32
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
		s.trackConnection(fd, c)
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
	c := s.connectionFor(token)
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
	if events == uint32(syscall.EPOLLOUT) && c.sendHead == len(c.sends) {
		// Write interest is armed for the connection's whole life, so the
		// socket reports itself writable the moment it is registered and again
		// every time it drains. With nothing queued there is nothing for a
		// worker to do, and the edge that does matter, the one after a write
		// stops short, always arrives later. Skipping the wake-up keeps an
		// accept burst from scheduling every new connection a second time.
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
	pauseReads, byBudget := c.pauseDecision(c.readPaused)
	changed := c.readPaused != pauseReads
	c.readPaused = pauseReads
	track := usable && pauseReads && byBudget && !c.budgetPaused
	if track {
		c.budgetPaused = true
	}
	if !pauseReads {
		c.budgetPaused = false
	}
	c.mu.Unlock()
	if track {
		// A connection held back only by the server-wide budget may have
		// nothing of its own left to flush, so no later event of its own would
		// re-evaluate it. The loop resumes it when the budget recovers.
		s.budgetPaused = append(s.budgetPaused, c)
	}
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
	for i := c.sendHead; i < len(c.sends); i++ {
		c.releaseItemLocked(&c.sends[i])
	}
	c.sends = nil
	c.sendHead = 0
	c.budgetPaused = false
	// Drop this connection's share of the server-wide budget in the same step
	// that abandons its queue, so a closed connection cannot hold the budget
	// against the ones still running.
	if pending := c.pendingBytes.Swap(0); pending > 0 {
		s.pendingTotal.Add(-pending)
	}
	c.mu.Unlock()
	fd := int(c.fd.Swap(-1))
	if fd >= 0 {
		_ = syscall.EpollCtl(s.epollFD, syscall.EPOLL_CTL_DEL, fd, nil)
		_ = syscall.Close(fd)
	}
	if page := fd >> connPageShift; fd >= 0 && page < len(s.connections) {
		if entries := s.connections[page]; entries != nil && entries[fd&connPageMask] == c {
			entries[fd&connPageMask] = nil
		}
	}
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
// bounds how much this connection, or the server as a whole, buffers in
// userspace.
func (c *Connection) overWriteWatermark() bool {
	pause, _ := c.pauseDecision(false)
	return pause
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
			c.subPending(int64(n))
			left := n
			for c.sendHead < len(c.sends) {
				item := &c.sends[c.sendHead]
				remaining := len(item.data) - item.offset
				if left < remaining {
					item.offset += left
					break
				}
				left -= remaining
				c.releaseItemLocked(item)
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

var (
	backlogOnce  sync.Once
	backlogValue int
)

// defaultBacklog reports the accept queue depth the kernel is willing to
// honour, which is what net.Listen asks for and therefore what every framework
// built on it gets. The historical SOMAXCONN of 128 is far below a connection
// burst: an overflowing accept queue makes the kernel drop the client's ACK
// rather than refuse it, so the client learns nothing until its SYN-ACK
// retransmission timer fires a second later.
func defaultBacklog() int {
	backlogOnce.Do(func() {
		backlogValue = syscall.SOMAXCONN
		data, err := os.ReadFile("/proc/sys/net/core/somaxconn")
		if err != nil {
			return
		}
		limit, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || limit <= 0 {
			return
		}
		// Above this the value no longer fits the kernel's backlog field.
		if limit > 1<<16-1 {
			limit = 1<<16 - 1
		}
		backlogValue = limit
	})
	return backlogValue
}

func createListener(config Config, port uint16) (int, error) {
	ip := net.ParseIP(config.BindAddress).To4()
	if ip == nil {
		return -1, fmt.Errorf("invalid IPv4 bind address %q", config.BindAddress)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	sa := &syscall.SockaddrInet4{Port: int(port)}
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
