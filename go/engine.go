//go:build linux || darwin || windows

package fib

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lesismal/fib/go/taskpool"
)

const (
	maxWritevItems = 64
	// maxWaitBatch caps the buffer one wait for events fills. MaxEvents keeps
	// sizing the task queue; beyond this many events per wait the loop just
	// waits again, so a larger buffer only costs memory per server.
	maxWaitBatch = 1024
	// defaultWriteHighWatermark is the per-connection outbound budget. Two
	// costs sit either side of it. Lower, and ordinary traffic crosses it on
	// every reply, and each crossing costs an eventfd write and an epoll_ctl:
	// at 4KB that churn was 16% of a 100k-connection profile. Higher, and a
	// backlog grows past the buffer size the pool retains: at 64KB against the
	// default 16KB read buffer, a 15-second rate test allocated 13.7GB against
	// the same run's 254MB live. The default sits at that upper end, favouring
	// fewer pauses for bursty peers over allocation under sustained backlog;
	// memory-sensitive deployments with many connections should lower it.
	defaultWriteHighWatermark = 64 << 10
	// defaultMaxPendingBytes is the server-wide outbound budget. It is the
	// bound that actually holds at high connection counts, where the
	// per-connection watermark alone would admit its own limit times the
	// connection count.
	defaultMaxPendingBytes = 1 << 30
)

// Readiness a connection accumulates between rounds. The values are epoll's,
// so the Linux backend passes its events through untranslated; kqueue and IOCP
// translate what they observe into the same bits.
const (
	evIn    uint32 = 0x1
	evPri   uint32 = 0x2
	evOut   uint32 = 0x4
	evErr   uint32 = 0x8
	evHup   uint32 = 0x10
	evRdHup uint32 = 0x2000
	evAll          = evIn | evPri | evOut | evErr | evHup | evRdHup
)

type commandType uint8

const (
	commandRefresh commandType = iota
	commandClose
	commandDial
	commandDialTimeout
)

type command struct {
	kind       commandType
	connection *Connection
	dial       *dialRequest
	err        error
}
type commandBatch struct{ items []command }

// Engine owns the listeners, the event loop, its command queue, and the worker
// pool. The loop itself is platform code: epoll on Linux, kqueue on macOS and
// an I/O completion port on Windows, each embedded here as enginePlatform.
type Engine struct {
	enginePlatform
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
	pendingTotal       atomic.Int64
	// Backpressure counters, reported by Stats. They move only when a
	// connection's read interest actually changes, which is rare by design.
	readsPausedByWatermark atomic.Uint64
	readsPausedByBudget    atomic.Uint64
	readsResumed           atomic.Uint64
	commandMu              sync.Mutex
	commands               *commandBatch
	// commandsClosed turns requests away once Close has drained the queue for
	// the last time, since nothing would ever run what arrived after it.
	// Guarded by commandMu.
	commandsClosed bool
	commandPool    sync.Pool
	wakePending    atomic.Bool
	// budgetPaused holds connections whose reads the server-wide budget stopped,
	// waiting to be resumed once it recovers. Event-loop ownership.
	budgetPaused []*Connection
	// budgetResume is the spare list resumeBudgetPaused swaps in while it walks
	// the current one. Event-loop ownership.
	budgetResume []*Connection
	// redeliver holds stalled connections whose read the loop is handing back
	// directly, to be scheduled at the end of the round. Event-loop ownership.
	redeliver       []*Connection
	taskPool        TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	readBufferPool  sync.Pool
	sendBufferPool  sync.Pool
	closeOnce       sync.Once
}

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

// Bind creates an engine that listens on config.Addr, or on every address in
// config.Addrs, and serves the connections it accepts with handler.
func Bind(config Config, handler Handler) (*Engine, error) {
	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
	}
	return newEngine(config, handler, addrs)
}

// NewEngine creates an engine with no listeners. Its connections are the ones
// it dials, which makes it a client engine; config.Addr and config.Addrs are
// ignored. handler serves connections dialed with Dial, and may be nil when
// every dial names its own handler through DialWithHandler.
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
	if config.WriteBufferHighWatermark == 0 {
		config.WriteBufferHighWatermark = 4 * 1024
	}
	if config.Backlog <= 0 {
		config.Backlog = defaultBacklog()
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}

	e := &Engine{maxEvents: config.MaxEvents,
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
	e.taskPool, e.releaseTaskPool = acquireTaskPool(config)
	if err := e.open(config, addrs); err != nil {
		e.releaseTaskPool()
		return nil, err
	}
	return e, nil
}

// errNoListener is what LocalAddr reports for an engine made by NewEngine.
var errNoListener = errors.New("fib: engine has no listener")

// LocalAddr returns the address of the server's first listener.
func (e *Engine) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := e.LocalAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNoListener
	}
	return addrs[0], nil
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

// runReady ends one round of the loop: it hands the connections the round made
// runnable to the workers, or runs them in place when handlers are inline, and
// then gives connections parked on the server-wide budget a chance to resume.
func (e *Engine) runReady(ready []*Connection, tasks []taskpool.Task) ([]*Connection, []taskpool.Task) {
	// Resuming comes first so that a connection it hands a read back to is
	// scheduled in this round rather than whenever the loop next wakes.
	e.resumeBudgetPaused()
	ready = append(ready, e.redeliver...)
	clear(e.redeliver)
	e.redeliver = e.redeliver[:0]
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
	return ready, tasks
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

// submitReady hands one round's newly runnable connections to the task pool in
// a single batch instead of one lock-and-wake cycle per connection.
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

// request queues a command for the event loop. It reports false, and queues
// nothing, once Close has drained the queue for the last time.
func (e *Engine) request(cmd command) bool {
	e.commandMu.Lock()
	if e.commandsClosed {
		e.commandMu.Unlock()
		return false
	}
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
	return true
}

// closeCommands drains the command queue one last time and turns away every
// request after it. Close calls it once no worker can queue anything more.
func (e *Engine) closeCommands() {
	e.commandMu.Lock()
	e.commandsClosed = true
	e.commandMu.Unlock()
	e.drainCommands()
}

// notify wakes the event loop, coalescing requests that arrive before it has
// woken into a single wake-up.
func (e *Engine) notify() {
	if !e.wakePending.CompareAndSwap(false, true) {
		return
	}
	e.wake()
}

// noteEvent folds readiness into the connection and reports whether it needs
// to be scheduled. Actual submission happens once per round in Run.
func (e *Engine) noteEvent(c *Connection, events uint32) *Connection {
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return nil
	}
	events &= evAll
	if events == 0 {
		c.mu.Unlock()
		return nil
	}
	if events == evOut && c.sendHead == len(c.sends) && c.pendingEvents == 0 {
		// Write interest is armed for the connection's whole life, so the
		// socket reports itself writable the moment it is registered and again
		// every time it drains. With nothing queued there is nothing for a
		// worker to do, and the edge that does matter, the one after a write
		// stops short, always arrives later. Skipping the wake-up keeps an
		// accept burst from scheduling every new connection a second time.
		c.mu.Unlock()
		return nil
	}
	// Readiness is level information from the connection's point of view.
	// Coalescing duplicate notifications avoids a slice scan and keeps a hot
	// connection from allocating an unbounded event queue while its worker is
	// draining the socket.
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
	e.ackWake()
	e.wakePending.Store(false)
	e.commandMu.Lock()
	batch := e.commands
	e.commands = nil
	e.commandMu.Unlock()
	if batch == nil {
		return
	}
	for _, cmd := range batch.items {
		switch cmd.kind {
		case commandClose:
			e.closeConnection(cmd.connection, cmd.err, true)
		case commandDial:
			e.startDial(cmd.dial)
		case commandDialTimeout:
			e.expireDial(cmd.connection, cmd.dial)
		default:
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
	// A stalled read is settled once reads are running again. Resuming them
	// re-arms the backend, and re-arming reports bytes already waiting, so
	// only a connection that was never paused needs its read handed back.
	redeliver := false
	if c.readStalled && !pauseReads {
		c.readStalled = false
		redeliver = usable && !changed
	}
	c.mu.Unlock()
	if redeliver {
		if ready := e.noteEvent(c, evIn); ready != nil {
			e.redeliver = append(e.redeliver, ready)
		}
	}
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
		// Write interest is permanent, so the backend only has to track
		// whether reads are paused while the peer catches up.
		if err := e.setReadPaused(c, pauseReads); err != nil {
			c.closeWithError(err)
		}
	}
}

func (e *Engine) closeConnection(c *Connection, closeErr error, callback bool) {
	if c.dialing != nil {
		// The dialer has not been handed the connection yet, so it is the one
		// to hear about the close, and the handler never hears of it at all.
		if closeErr == nil {
			closeErr = net.ErrClosed
		}
		e.failDial(c, closeErr)
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closing = true
	// Buffers the kernel is still sending from cannot go back to the pool: the
	// next connection to take one would overwrite bytes still on their way out.
	// They are left to the garbage collector instead.
	if !c.writeBusyLocked() {
		for i := c.sendHead; i < len(c.sends); i++ {
			c.releaseItemLocked(&c.sends[i])
		}
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
	e.detach(c)
	if callback {
		c.handler.OnClose(c, closeErr)
	}
}

var _ io.Closer = (*Engine)(nil)
