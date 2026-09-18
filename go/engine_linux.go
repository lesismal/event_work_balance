//go:build linux

package fib

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/lesismal/fib/go/taskpool"
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

// acquireSendBuffer returns a pooled outbound buffer.
//
// One size for every buffer is deliberate. Sizing buffers to the backlog was
// measured twice and helped neither: a 4KB class left 736MB of buffers about a
// third full, and stepping down to 1KB classes made it worse still at 1181MB,
// because several pools each retain their own idle buffers. Resident peak did
// not move for any of them, so the bound that matters is MaxPendingBytes, not
// the shape of the buffers underneath it.
func (e *Engine) acquireSendBuffer() *sendBuffer {
	return e.sendBufferPool.Get().(*sendBuffer)
}

// releaseSendBuffer hands a buffer back. One larger than the retention limit is
// dropped, so that a single big message cannot leave every pooled buffer
// permanently inflated.
func (e *Engine) releaseSendBuffer(b *sendBuffer) {
	if cap(b.data) <= e.retainedSendBuffer {
		b.data = b.data[:0]
		e.sendBufferPool.Put(b)
	}
}

// Engine owns the listener, epoll descriptor, command queue, and worker pool.
type Engine struct {
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
	// Backpressure counters, reported by Stats. They move only when a
	// connection's read interest actually changes, which is rare by design.
	readsPausedByWatermark atomic.Uint64
	readsPausedByBudget    atomic.Uint64
	readsResumed           atomic.Uint64
	commandMu              sync.Mutex
	commands               *commandBatch
	commandPool            sync.Pool
	wakePending            atomic.Bool
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
func (e *Engine) connectionFor(token uint64) *Connection {
	fd := int(uint32(token))
	page := fd >> connPageShift
	if page < 0 || page >= len(e.connections) {
		return nil
	}
	entries := e.connections[page]
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
func (e *Engine) trackConnection(fd int, c *Connection) {
	page := fd >> connPageShift
	if page >= len(e.connections) {
		// Only the page directory is ever copied, and it holds one pointer per
		// 4096 descriptors, so growing it stays cheap however high descriptors
		// climb.
		directory := make([][]*Connection, max(page+1, 2*len(e.connections)))
		copy(directory, e.connections)
		e.connections = directory
	}
	if e.connections[page] == nil {
		e.connections[page] = make([]*Connection, connPageSize)
	}
	e.connections[page][fd&connPageMask] = c
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
	if config.WriteBufferHighWatermark == 0 {
		config.WriteBufferHighWatermark = 4 * 1024
	}
	if config.Backlog <= 0 {
		config.Backlog = defaultBacklog()
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}

	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
	}

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	listenFDs := make([]int, 0, len(addrs))
	closeListeners := func() {
		for _, fd := range listenFDs {
			syscall.Close(fd)
		}
	}
	for _, addr := range addrs {
		listenFD, listenErr := createListener(config, addr)
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
	e := &Engine{epollFD: epfd, listenFDs: listenFDs, wakeFD: wakeFD, maxEvents: config.MaxEvents,
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
	e.nextGeneration.Store(firstGeneration)
	e.taskPool, e.releaseTaskPool = acquireTaskPool(config)
	e.readBufferPool.New = func() any { return &readBuffer{data: make([]byte, config.ReadBufferSize)} }
	// Pooled outbound buffers start at the size a full round's replies actually
	// reach, which is the bytes that arrived plus the framing put back on top
	// of them, not the read buffer size alone. Starting any smaller costs a
	// reallocation per round on every connection, and starting from empty costs
	// one per doubling: empty cost 49.5GB of allocation across a 15-second rate
	// test, and an exact-fit 16KB still cost 12.4GB. Matching the retention
	// limit means a buffer that has grown is still handed back to the pool
	// rather than dropped.
	e.sendBufferPool.New = func() any {
		return &sendBuffer{data: make([]byte, 0, e.retainedSendBuffer)}
	}
	for _, fd := range listenFDs {
		if err = e.addFD(fd, listenerToken(fd), uint32(syscall.EPOLLIN)|epollET); err != nil {
			break
		}
	}
	if err == nil {
		err = e.addFD(wakeFD, wakeToken(wakeFD), uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		e.releaseTaskPool()
		syscall.Close(wakeFD)
		closeListeners()
		syscall.Close(epfd)
		return nil, err
	}
	return e, nil
}

// LocalAddr returns the address of the server's first listener.
func (e *Engine) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := e.LocalAddrs()
	if err != nil {
		return nil, err
	}
	return addrs[0], nil
}

// LocalAddrs returns one address per listener, in configured order. Ports left
// at zero report the port the kernel chose.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(e.listenFDs))
	for _, fd := range e.listenFDs {
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			return nil, err
		}
		switch bound := sa.(type) {
		case *syscall.SockaddrInet4:
			addrs = append(addrs, &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port})
		case *syscall.SockaddrInet6:
			addr := &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port}
			if bound.ZoneId != 0 {
				if zone, zoneErr := net.InterfaceByIndex(int(bound.ZoneId)); zoneErr == nil {
					addr.Zone = zone.Name
				}
			}
			addrs = append(addrs, addr)
		default:
			return nil, fmt.Errorf("listener is not TCP: %T", sa)
		}
	}
	return addrs, nil
}

// Stats reports what backpressure this server has applied. It answers the
// question the counters exist for: whether reads were ever paused, and which
// of the two bounds did it, since the two have very different causes.
type Stats struct {
	// ReadsPausedByWatermark counts the times a connection's reads were paused
	// because that connection's own queued output reached
	// WriteBufferHighWatermark. Its peer is not keeping up with its replies.
	ReadsPausedByWatermark uint64
	// ReadsPausedByBudget counts the times a connection's reads were paused
	// because MaxPendingBytes was exhausted across the whole server. Such a
	// connection may be holding almost nothing itself: it is paying for what
	// the others have queued, so a connection well under its own watermark can
	// still be stopped this way.
	ReadsPausedByBudget uint64
	// ReadsResumed counts the pauses that have since been lifted.
	ReadsResumed uint64
	// PendingBytes is the outbound total queued across this server's
	// connections right now, which is what MaxPendingBytes bounds.
	PendingBytes int64
}

func (e *Engine) Stats() Stats {
	return Stats{
		ReadsPausedByWatermark: e.readsPausedByWatermark.Load(),
		ReadsPausedByBudget:    e.readsPausedByBudget.Load(),
		ReadsResumed:           e.readsResumed.Load(),
		PendingBytes:           e.pendingTotal.Load(),
	}
}

func (e *Engine) Run() error {
	batch := e.maxEvents
	if batch > maxWaitBatch {
		batch = maxWaitBatch
	}
	events := make([]syscall.EpollEvent, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	for !e.stopping.Load() {
		n, err := syscall.EpollWait(e.epollFD, events, -1)
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
				e.acceptConnections(int(uint32(token)))
			case wakeKind:
				e.drainCommands()
			default:
				if c := e.noteEvent(token, events[i].Events); c != nil {
					ready = append(ready, c)
				}
			}
		}
		if len(ready) > 0 {
			if e.inlineHandlers {
				for _, c := range ready {
					c.process()
				}
			} else {
				tasks = e.submitReady(ready, tasks[:0])
			}
			clear(ready)
			ready = ready[:0]
		}
		e.resumeBudgetPaused()
	}
	e.drainCommands()
	return nil
}

// resumeBudgetPaused re-arms reads on connections the server-wide budget held
// back, once enough of that budget has drained. They are re-examined rather
// than resumed outright: a connection that has since built a backlog of its own
// stays paused on its own account, and simply goes back on the list.
func (e *Engine) resumeBudgetPaused() {
	if len(e.budgetPaused) == 0 || e.pendingTotal.Load() > e.budgetResumeBytes {
		return
	}
	// Swap in the spare list before refreshing. A connection that is still held
	// back goes straight back onto s.budgetPaused, which therefore must not
	// share an array with the one being iterated.
	waiting := e.budgetPaused
	e.budgetPaused = e.budgetResume[:0]
	for _, c := range waiting {
		// Clear the flag first: it is what stops a connection already on the
		// list from being added twice, so leaving it set would drop a
		// connection that turns out to still need the budget.
		c.mu.Lock()
		c.budgetPaused = false
		c.mu.Unlock()
		e.refreshConnection(c)
	}
	clear(waiting)
	e.budgetResume = waiting
}

// submitReady hands one epoll round's newly runnable connections to the task
// pool in a single batch instead of one lock-and-wake cycle per connection.
func (e *Engine) submitReady(ready []*Connection, tasks []taskpool.Task) []taskpool.Task {
	for _, c := range ready {
		tasks = append(tasks, c)
	}
	e.taskWG.Add(len(tasks))
	accepted := e.taskPool.GoTasks(tasks)
	for _, c := range ready[accepted:] {
		e.taskWG.Done()
		c.mu.Lock()
		c.scheduled = false
		c.mu.Unlock()
		c.Close()
	}
	clear(tasks)
	return tasks
}

func (e *Engine) Stop() { e.stopping.Store(true); e.notify() }

// Close releases all resources. Run must have returned before Close is called.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.Stop()
		e.taskWG.Wait()
		e.releaseTaskPool()
		e.drainCommands()
		for _, entries := range e.connections {
			for _, c := range entries {
				if c != nil {
					e.closeConnection(c, nil, false)
				}
			}
		}
		e.budgetPaused = nil
		for _, fd := range append(append([]int(nil), e.listenFDs...), e.wakeFD, e.epollFD) {
			if err := syscall.Close(fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func (e *Engine) request(cmd command) {
	e.commandMu.Lock()
	if e.commands == nil {
		if pooled := e.commandPool.Get(); pooled != nil {
			e.commands = pooled.(*commandBatch)
		} else {
			e.commands = &commandBatch{}
		}
	}
	e.commands.items = append(e.commands.items, cmd)
	e.commandMu.Unlock()
	e.notify()
}
func (e *Engine) notify() {
	if !e.wakePending.CompareAndSwap(false, true) {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = syscall.Write(e.wakeFD, b[:])
}
func (e *Engine) addFD(fd int, token uint64, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(token), Pad: int32(token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_ADD, fd, &ev)
}
func (e *Engine) modifyFD(c *Connection, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(c.token), Pad: int32(c.token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_MOD, c.FD(), &ev)
}

func (e *Engine) acceptConnections(listenFD int) {
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
		token := uint64(uint32(fd)) | e.nextGeneration.Add(1)<<32
		c := &Connection{token: token, engine: e}
		c.fd.Store(int32(fd))
		// EPOLLOUT is registered up front and never modified again. The
		// descriptor is edge-triggered, so an always-armed write interest only
		// fires when the socket goes from full back to writable, which spares
		// the loop an epoll_ctl pair per backpressured message.
		if err := e.addFD(fd, token, allEvents); err != nil {
			syscall.Close(fd)
			continue
		}
		e.trackConnection(fd, c)
		e.handler.OnOpen(c)
	}
}

// noteEvent folds readiness into the connection and reports whether it needs
// to be scheduled. Actual submission happens once per epoll round in Run.
func (e *Engine) noteEvent(token uint64, events uint32) *Connection {
	c := e.connectionFor(token)
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

func (e *Engine) drainCommands() {
	// A single read drains the eventfd: reading returns the whole counter and
	// resets it to zero, so looping until EAGAIN only adds a wasted syscall.
	var b [8]byte
	for {
		if _, err := syscall.Read(e.wakeFD, b[:]); err != syscall.EINTR {
			break
		}
	}
	e.wakePending.Store(false)
	e.commandMu.Lock()
	batch := e.commands
	e.commands = nil
	e.commandMu.Unlock()
	if batch == nil {
		return
	}
	for _, cmd := range batch.items {
		if cmd.kind == commandClose {
			e.closeConnection(cmd.connection, cmd.err, true)
		} else {
			e.refreshConnection(cmd.connection)
		}
	}
	for i := range batch.items {
		batch.items[i] = command{}
	}
	if cap(batch.items) <= e.maxEvents {
		batch.items = batch.items[:0]
		e.commandPool.Put(batch)
	}
}
func (e *Engine) refreshConnection(c *Connection) {
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
		e.budgetPaused = append(e.budgetPaused, c)
	}
	if changed {
		switch {
		case !pauseReads:
			e.readsResumed.Add(1)
		case byBudget:
			e.readsPausedByBudget.Add(1)
		default:
			e.readsPausedByWatermark.Add(1)
		}
	}
	if usable && changed {
		// Write interest is permanent, so registration only tracks whether
		// reads are paused while the peer catches up.
		events := uint32(allEvents)
		if pauseReads {
			events &^= syscall.EPOLLIN
		}
		if err := e.modifyFD(c, events); err != nil {
			c.closeWithError(err)
		}
	}
}
func (e *Engine) closeConnection(c *Connection, closeErr error, callback bool) {
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
		e.pendingTotal.Add(-pending)
	}
	c.mu.Unlock()
	fd := int(c.fd.Swap(-1))
	if fd >= 0 {
		_ = syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_DEL, fd, nil)
		_ = syscall.Close(fd)
	}
	if page := fd >> connPageShift; fd >= 0 && page < len(e.connections) {
		if entries := e.connections[page]; entries != nil && entries[fd&connPageMask] == c {
			entries[fd&connPageMask] = nil
		}
	}
	if callback {
		e.handler.OnClose(c, closeErr)
	}
}

var _ io.Closer = (*Engine)(nil)
