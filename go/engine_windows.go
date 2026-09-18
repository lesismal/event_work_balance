//go:build windows

package fib

import (
	"net"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/lesismal/fib/go/taskpool"
)

// The Windows backend drives sockets through an I/O completion port, but keeps
// the readiness model the rest of the engine is written against, so that the
// workers, the queueing and the backpressure are the same code on every
// platform.
//
// Readability comes from a zero-byte overlapped WSARecv. It completes when data
// or a close arrives, without taking any bytes and without pinning a buffer on
// an idle connection, and the worker then drains the socket with non-blocking
// receives exactly as it would after an epoll edge. Re-posting it after the
// drain is what edge-triggering means here.
//
// Writability is the other way round. A non-blocking send that cannot finish
// hands the queued bytes to an overlapped WSASend, and its completion plays the
// part of the write edge: it says the bytes left, and the worker carries on
// with whatever was queued behind them.
const (
	// acceptsPerListener is how many AcceptEx calls each listener keeps
	// outstanding. Connections beyond them wait in the listen backlog, so this
	// only bounds how many one completion round can admit.
	acceptsPerListener = 32
	// closeDrainTimeout bounds how long Close waits for the kernel to report
	// the operations it cancelled.
	closeDrainTimeout = 5 * time.Second
)

const (
	opAccept uint8 = iota
	opConnect
	opRead
	opWrite
)

// ioOp is an OVERLAPPED plus what the loop needs to route its completion. The
// OVERLAPPED comes first, so the pointer the port hands back is the ioOp's own
// address.
type ioOp struct {
	ov   syscall.Overlapped
	kind uint8
	conn *Connection
}

// acceptOp is one outstanding AcceptEx: the socket it will hand over, and the
// buffer AcceptEx writes both addresses into.
type acceptOp struct {
	ioOp
	listener *winListener
	socket   syscall.Handle
	received uint32
	addrs    [2 * acceptAddrLen]byte
}

const acceptAddrLen = int(unsafe.Sizeof(syscall.RawSockaddrAny{})) + 16

type winListener struct {
	fd     syscall.Handle
	family int
	closed bool
	// accepts holds the listener's AcceptEx operations. The kernel keeps only
	// their addresses, which the garbage collector does not see, so without
	// this reference an operation could be freed and its memory reused while
	// its AcceptEx is still pending.
	accepts []*acceptOp
}

type enginePlatform struct {
	port      syscall.Handle
	listeners []*winListener
	// conns holds every connection the engine owns, open or not. The kernel
	// writes into a connection's OVERLAPPED structures until each operation it
	// holds completes, so a closed connection stays here, and therefore stays
	// reachable, until its last completion has arrived. Event-loop ownership.
	conns map[*Connection]struct{}
	// accepts counts AcceptEx calls not yet completed, for the same reason.
	// Event-loop ownership.
	accepts int
}

type connPlatform struct {
	handle  atomic.Uintptr
	readOp  ioOp
	writeOp ioOp
	// outstanding counts operations posted and not yet completed. It rises
	// only under mu while the connection is open and falls only on the event
	// loop, so once the connection is closed the loop alone moves it.
	outstanding atomic.Int32
	// readArmed and writeInFlight say which of the two operations is posted.
	// Guarded by mu.
	readArmed     bool
	writeInFlight bool
	recvFlags     uint32
	// connectSent receives ConnectEx's byte count, which stays zero since a
	// dial sends nothing with its connect.
	connectSent uint32
	// inFlight keeps the arrays an overlapped send is reading from reachable
	// until it completes, even if the connection drops its queue meanwhile.
	inFlight [maxWritevItems][]byte
	// failure is the error a failed zero-byte read reported, which socketError
	// hands on when the worker asks why the connection broke.
	failure error
}

func (c *Connection) socket() syscall.Handle { return syscall.Handle(c.handle.Load()) }

// FD returns the connection's socket handle, or -1 once it is closed.
func (c *Connection) FD() int {
	h := c.socket()
	if h == syscall.InvalidHandle {
		return -1
	}
	return int(h)
}

func (e *Engine) open(config Config, addrs []string) error {
	port, err := syscall.CreateIoCompletionPort(syscall.InvalidHandle, 0, 0, 1)
	if err != nil {
		return err
	}
	e.port = port
	e.conns = make(map[*Connection]struct{})
	for _, addr := range addrs {
		l, err := createListener(config, addr)
		if err == nil {
			if _, err = syscall.CreateIoCompletionPort(l.fd, port, 0, 0); err != nil {
				syscall.Closesocket(l.fd)
			}
		}
		if err != nil {
			for _, opened := range e.listeners {
				syscall.Closesocket(opened.fd)
			}
			syscall.CloseHandle(port)
			return err
		}
		e.listeners = append(e.listeners, l)
	}
	for _, l := range e.listeners {
		for i := 0; i < acceptsPerListener; i++ {
			op := &acceptOp{listener: l}
			op.kind = opAccept
			l.accepts = append(l.accepts, op)
			if err := e.postAccept(op); err != nil {
				e.abandon()
				return err
			}
		}
	}
	return nil
}

// abandon tears down a Bind that failed after its AcceptEx calls went out.
func (e *Engine) abandon() {
	e.stopping.Store(true)
	for _, l := range e.listeners {
		l.closed = true
		syscall.Closesocket(l.fd)
	}
	e.drainPort()
	syscall.CloseHandle(e.port)
}

func createListener(config Config, addr string) (*winListener, error) {
	family, bound, err := resolveListenAddr(config.Network, addr)
	if err != nil {
		return nil, err
	}
	fd, err := newSocket(family)
	if err != nil {
		return nil, err
	}
	// No SO_REUSEADDR: on Windows it lets another socket take over a port that
	// is still in use, rather than just reusing one in TIME_WAIT.
	if family == syscall.AF_INET6 {
		// "tcp" accepts both families on one socket; "tcp6" is IPv6 only. This
		// is the distinction net.Listen draws between the two networks.
		v6only := 0
		if config.Network == "tcp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err = syscall.Bind(fd, bound); err == nil {
		err = syscall.Listen(fd, config.Backlog)
	}
	if err != nil {
		syscall.Closesocket(fd)
		return nil, err
	}
	return &winListener{fd: fd, family: family}, nil
}

// postAccept starts one AcceptEx on a fresh socket.
func (e *Engine) postAccept(op *acceptOp) error {
	s, err := newSocket(op.listener.family)
	if err != nil {
		return err
	}
	op.socket = s
	op.ov = syscall.Overlapped{}
	err = syscall.AcceptEx(op.listener.fd, s, &op.addrs[0], 0, uint32(acceptAddrLen), uint32(acceptAddrLen),
		&op.received, &op.ov)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		syscall.Closesocket(s)
		op.socket = syscall.InvalidHandle
		return err
	}
	e.accepts++
	return nil
}

// LocalAddrs returns one address per listener, in configured order. Ports left
// at zero report the port the kernel chose.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(e.listeners))
	for _, l := range e.listeners {
		sa, err := syscall.Getsockname(l.fd)
		if err != nil {
			return nil, err
		}
		addr, err := sockaddrToTCPAddr(sa)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func (e *Engine) Run() error {
	batch := e.maxEvents
	if batch > maxWaitBatch {
		batch = maxWaitBatch
	}
	entries := make([]overlappedEntry, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	for !e.stopping.Load() {
		n, err := getQueuedCompletionStatusEx(e.port, entries, syscall.INFINITE)
		if err != nil {
			if err == syscall.Errno(syscall.WAIT_TIMEOUT) {
				continue
			}
			return err
		}
		woken := false
		for i := 0; i < n; i++ {
			if entries[i].overlapped == nil {
				// Only the wake-up is posted without an OVERLAPPED.
				woken = true
				continue
			}
			if c := e.complete(&entries[i]); c != nil {
				ready = append(ready, c)
			}
			entries[i] = overlappedEntry{}
		}
		if woken {
			e.drainCommands()
		}
		ready, tasks = e.runReady(ready, tasks)
	}
	e.drainCommands()
	return nil
}

// complete routes one completion and reports a connection it made runnable.
func (e *Engine) complete(entry *overlappedEntry) *Connection {
	op := (*ioOp)(unsafe.Pointer(entry.overlapped))
	var err error
	if entry.status != 0 {
		err = ntStatusError(entry.status)
	}
	switch op.kind {
	case opAccept:
		e.completeAccept((*acceptOp)(unsafe.Pointer(op)), err)
		return nil
	case opConnect:
		e.completeConnect(op.conn, err)
		return nil
	case opRead:
		return e.completeRead(op.conn, err)
	default:
		return e.completeWrite(op.conn, int(entry.qty), err)
	}
}

func (e *Engine) completeAccept(op *acceptOp, err error) {
	e.accepts--
	s := op.socket
	op.socket = syscall.InvalidHandle
	if e.stopping.Load() || op.listener.closed {
		// A connection that arrives while the engine shuts down is refused
		// rather than opened into a loop that no longer runs.
		syscall.Closesocket(s)
		return
	}
	if err == nil {
		err = e.adopt(op.listener, s)
	}
	if err != nil {
		syscall.Closesocket(s)
	}
	// Keep the listener's accepts topped up. A client that resets between
	// connecting and being accepted fails only its own AcceptEx, so one retry
	// is worth making before the slot is given up.
	for attempt := 0; attempt < 2; attempt++ {
		if e.postAccept(op) == nil {
			return
		}
	}
}

// adopt turns an accepted socket into a connection and arms its first read.
func (e *Engine) adopt(l *winListener, s syscall.Handle) error {
	// Without this the socket does not know its addresses, and shutdown and
	// getpeername fail on it.
	lfd := l.fd
	err := syscall.Setsockopt(s, syscall.SOL_SOCKET, syscall.SO_UPDATE_ACCEPT_CONTEXT,
		(*byte)(unsafe.Pointer(&lfd)), int32(unsafe.Sizeof(lfd)))
	if err == nil {
		err = setNonblock(s)
	}
	if err == nil {
		_, err = syscall.CreateIoCompletionPort(s, e.port, 0, 0)
	}
	if err != nil {
		return err
	}
	c := &Connection{engine: e}
	c.handle.Store(uintptr(s))
	c.readOp = ioOp{kind: opRead, conn: c}
	c.writeOp = ioOp{kind: opWrite, conn: c}
	e.conns[c] = struct{}{}
	e.handler.OnOpen(c)
	c.rearmRead()
	return nil
}

// completeRead handles the zero-byte read: the socket has data, or an EOF or
// error waiting to be read, so a worker's drain has something to find.
func (e *Engine) completeRead(c *Connection, err error) *Connection {
	c.outstanding.Add(-1)
	c.mu.Lock()
	c.readArmed = false
	closed := c.closed
	if err != nil && !closed {
		c.failure = err
	}
	c.mu.Unlock()
	if closed {
		e.forget(c)
		return nil
	}
	events := evIn
	if err != nil {
		events = evErr
	}
	return e.noteEvent(c, events)
}

// completeWrite takes the bytes an overlapped send delivered off the queue and
// plays the write edge for whatever was queued behind them.
func (e *Engine) completeWrite(c *Connection, n int, err error) *Connection {
	c.outstanding.Add(-1)
	c.mu.Lock()
	c.writeInFlight = false
	clear(c.inFlight[:])
	if c.closed {
		c.mu.Unlock()
		e.forget(c)
		return nil
	}
	if err != nil {
		c.mu.Unlock()
		c.closeWithError(err)
		return nil
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
	}
	closeAfterSend := false
	if c.sendHead == len(c.sends) {
		c.resetQueueLocked()
		closeAfterSend = c.closeAfterSend
	}
	refresh := c.pauseStateChangedLocked()
	c.mu.Unlock()
	if closeAfterSend {
		c.closeWithError(nil)
		return nil
	}
	if refresh {
		e.refreshConnection(c)
	}
	return e.noteEvent(c, evOut)
}

// forget drops a closed connection once the kernel holds nothing of it.
func (e *Engine) forget(c *Connection) {
	if c.outstanding.Load() == 0 {
		delete(e.conns, c)
	}
}

func (e *Engine) wake() {
	_ = syscall.PostQueuedCompletionStatus(e.port, 0, 0, nil)
}

// ackWake does nothing: every wake-up is its own completion, already
// dequeued by the time the loop drains commands.
func (e *Engine) ackWake() {}

// setReadPaused re-arms the zero-byte read when reads resume. Pausing needs no
// call: a read already armed may still complete, but the round it starts finds
// the output that caused the pause still queued and defers, and it does not
// re-arm while reads are paused.
func (e *Engine) setReadPaused(c *Connection, paused bool) error {
	if paused {
		return nil
	}
	c.mu.Lock()
	err := c.armReadLocked()
	c.mu.Unlock()
	return err
}

// detach closes a connection's socket, which cancels whatever it has posted.
// The cancelled operations still complete through the port, and the connection
// stays in conns until the last of them has.
func (e *Engine) detach(c *Connection) {
	if h := syscall.Handle(c.handle.Swap(uintptr(syscall.InvalidHandle))); h != syscall.InvalidHandle {
		_ = syscall.Closesocket(h)
	}
	e.forget(c)
}

// Close releases all resources. Run must have returned before Close is called.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.Stop()
		e.taskWG.Wait()
		e.releaseTaskPool()
		e.closeCommands()
		for c := range e.conns {
			e.closeConnection(c, nil, false)
		}
		e.budgetPaused = nil
		for _, l := range e.listeners {
			l.closed = true
			if err := syscall.Closesocket(l.fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		e.drainPort()
		if err := syscall.CloseHandle(e.port); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

// drainPort collects the completions of the operations closing the sockets
// cancelled. Until each has arrived the kernel may still write into its
// OVERLAPPED, so the memory holding it cannot be let go before then.
func (e *Engine) drainPort() {
	entries := make([]overlappedEntry, 64)
	deadline := time.Now().Add(closeDrainTimeout)
	for (len(e.conns) > 0 || e.accepts > 0) && time.Now().Before(deadline) {
		n, err := getQueuedCompletionStatusEx(e.port, entries, 100)
		if err != nil {
			continue
		}
		for i := 0; i < n; i++ {
			if entries[i].overlapped != nil {
				e.complete(&entries[i])
			}
			entries[i] = overlappedEntry{}
		}
	}
}

// rearmRead posts the zero-byte read that reports the socket's next data. It
// runs after each drain; a connection whose reads are paused is re-armed when
// they resume instead.
func (c *Connection) rearmRead() {
	c.mu.Lock()
	err := c.armReadLocked()
	c.mu.Unlock()
	if err != nil {
		c.closeWithError(err)
	}
}

func (c *Connection) armReadLocked() error {
	if c.readArmed || c.readPaused || c.closing || c.closed {
		return nil
	}
	c.readOp.ov = syscall.Overlapped{}
	c.recvFlags = 0
	c.readArmed = true
	c.outstanding.Add(1)
	var buf syscall.WSABuf
	var n uint32
	err := syscall.WSARecv(c.socket(), &buf, 1, &n, &c.recvFlags, &c.readOp.ov, nil)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		c.readArmed = false
		c.outstanding.Add(-1)
		return err
	}
	return nil
}

// writeBusyLocked reports that an overlapped send owns the head of the queue,
// so nothing else may write or flush until it completes.
func (c *Connection) writeBusyLocked() bool { return c.writeInFlight }

// awaitWritableLocked hands whatever is still queued to an overlapped send.
// Its completion is the write edge: it takes the bytes off the queue and
// schedules the connection for anything queued behind them.
func (c *Connection) awaitWritableLocked() error {
	if c.writeInFlight || c.closing || c.closed || c.sendHead == len(c.sends) {
		return nil
	}
	var bufs [maxWritevItems]syscall.WSABuf
	count := 0
	for i := c.sendHead; i < len(c.sends) && count < len(bufs); i++ {
		item := &c.sends[i]
		data := item.data[item.offset:]
		if len(data) == 0 {
			continue
		}
		if len(data) > maxWSABufLen {
			data = data[:maxWSABufLen]
		}
		bufs[count] = syscall.WSABuf{Len: uint32(len(data)), Buf: &data[0]}
		c.inFlight[count] = data
		count++
		if len(data) < len(item.data)-item.offset {
			// A truncated item has to be the last one, or the bytes after it
			// would be sent ahead of its remainder.
			break
		}
	}
	if count == 0 {
		return nil
	}
	c.writeOp.ov = syscall.Overlapped{}
	c.writeInFlight = true
	c.outstanding.Add(1)
	var sent uint32
	err := syscall.WSASend(c.socket(), &bufs[0], uint32(count), &sent, 0, &c.writeOp.ov, nil)
	if err != nil && err != syscall.ERROR_IO_PENDING {
		c.writeInFlight = false
		c.outstanding.Add(-1)
		clear(c.inFlight[:])
		return err
	}
	return nil
}

func (c *Connection) sysRead(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	var bufs [1]syscall.WSABuf
	wsaBufs(bufs[:], buf)
	var n, flags uint32
	if err := syscall.WSARecv(c.socket(), &bufs[0], 1, &n, &flags, nil, nil); err != nil {
		return 0, err
	}
	return int(n), nil
}

func (c *Connection) sysRecvOOB(buf []byte) (int, error) {
	var bufs [1]syscall.WSABuf
	wsaBufs(bufs[:], buf)
	var n uint32
	flags := uint32(msgOOB)
	if err := syscall.WSARecv(c.socket(), &bufs[0], 1, &n, &flags, nil, nil); err != nil {
		if err == wsaEINVAL {
			err = syscall.EINVAL
		}
		return 0, err
	}
	return int(n), nil
}

func (c *Connection) sysWrite(buf []byte) (int, error) {
	var bufs [1]syscall.WSABuf
	return c.wsaSend(bufs[:wsaBufs(bufs[:], buf)])
}

func (c *Connection) sysWrite2(first, second []byte) (int, error) {
	var bufs [2]syscall.WSABuf
	return c.wsaSend(bufs[:wsaBufs(bufs[:], first, second)])
}

func (c *Connection) sysWritev(buffers [][]byte) (int, error) {
	var bufs [maxWritevItems]syscall.WSABuf
	return c.wsaSend(bufs[:wsaBufs(bufs[:], buffers...)])
}

func (c *Connection) wsaSend(bufs []syscall.WSABuf) (int, error) {
	if len(bufs) == 0 {
		return 0, nil
	}
	var n uint32
	if err := syscall.WSASend(c.socket(), &bufs[0], uint32(len(bufs)), &n, 0, nil, nil); err != nil {
		return 0, err
	}
	return int(n), nil
}

func (c *Connection) socketError() error {
	c.mu.Lock()
	failure := c.failure
	c.mu.Unlock()
	if failure != nil {
		return failure
	}
	errno, err := syscall.GetsockoptInt(c.socket(), syscall.SOL_SOCKET, soError)
	if err != nil {
		return err
	}
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return syscall.ECONNRESET
}
