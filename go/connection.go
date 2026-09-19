//go:build linux || darwin || windows

package fib

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
)

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
	connPlatform
	engine *Engine
	// handler receives this connection's callbacks: the engine's handler for
	// an accepted connection, or the one DialWithHandler was given.
	handler       Handler
	mu            sync.Mutex
	pendingEvents uint32
	sends         []sendItem
	sendHead      int
	scheduled     bool
	closing       bool
	closed        bool
	readPaused    bool
	budgetPaused  bool
	// readStalled records that a read round stopped with bytes possibly
	// still in the socket, because the write backlog filled up first. Those
	// bytes raise no further edge on their own, so the event loop owes the
	// connection either a paused-then-resumed read or a direct redelivery.
	readStalled    bool
	flushing       bool
	corked         bool
	closeAfterSend bool
	pendingBytes   atomic.Int64
	attachment     atomic.Pointer[connectionAttachment]
	// dialing is set while an outbound connect is still in progress, and
	// cleared when it completes or fails. Event-loop ownership.
	dialing *dialRequest
	// layer, when set, carries every send, as TLS does to encrypt it.
	layer Layer
	// udp is set for a UDP connection, which exchanges datagrams.
	udp *udpState
}

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
	if l := c.layer; l != nil {
		l.CloseAfterSend()
		return
	}
	c.closeAfterSendRaw()
}

// closeAfterSendRaw is CloseAfterSend below any layer.
func (c *Connection) closeAfterSendRaw() {
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
		c.engine.request(command{kind: commandClose, connection: c, err: err})
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
		c.engine.releaseSendBuffer(item.buf)
	}
	*item = sendItem{}
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
	buf := c.engine.acquireSendBuffer()
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
	if c.udp != nil {
		// Datagrams are never queued for sending, so there is nothing to wait
		// for, and a flood is bounded by the receive queue instead.
		return false, false
	}
	e := c.engine
	if e.maxPendingBytes > 0 && e.pendingTotal.Load() >= e.maxPendingBytes {
		return true, true
	}
	if e.writeHighWatermark <= 0 {
		return false, false
	}
	pendingBytes := c.pendingBytes.Load()
	if pendingBytes >= int64(e.writeHighWatermark) {
		return true, false
	}
	// Hysteresis: once paused, stay paused until the backlog falls well below
	// the watermark. Resuming at the watermark itself would make every reply
	// re-cross it, and each crossing costs an eventfd write and an epoll_ctl.
	return readPaused && pendingBytes > int64(e.writeLowWatermark), false
}

// addPending grows both the connection's outbound backlog and the server-wide
// total, which are kept in step so that the budget is always the sum of its
// connections.
func (c *Connection) addPending(n int64) {
	if n <= 0 {
		return
	}
	c.pendingBytes.Add(n)
	c.engine.pendingTotal.Add(n)
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
		c.engine.pendingTotal.Add(-n)
	}
}

// Send copies data before returning. It first attempts a direct nonblocking write.
func (c *Connection) Send(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.send(data, true)
}

// SendOwned sends data without copying it. Ownership transfers to the
// connection immediately; the caller must not access data after the call.
func (c *Connection) SendOwned(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.send(data, false)
}

// sendRaw writes bytes to the socket below any layer.
func (c *Connection) sendRaw(data []byte) error {
	return c.send(data, true)
}

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closed := c.closing || c.closed || c.closeAfterSend
	c.mu.Unlock()
	return closed
}

func (c *Connection) send(data []byte, copyData bool) error {
	if c.udp != nil {
		return c.sendDatagram(data)
	}
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	sent := 0
	direct := c.canWriteDirectlyLocked()
	if direct {
		for {
			n, err := c.sysWrite(data)
			if err == syscall.EINTR {
				continue
			}
			if err != nil && !isWouldBlock(err) {
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
	// Write interest stays armed, so queueing alone needs no registration
	// change; only a watermark crossing does. While corked the flush at the end
	// of the read round settles the read interest instead.
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	refresh := !c.corked && c.pauseStateChangedLocked()
	c.mu.Unlock()
	if armErr != nil {
		c.closeWithError(armErr)
		return armErr
	}
	if refresh {
		c.engine.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// SendParts writes a two-part message without first joining the parts. If the
// socket is backpressured, only the unsent suffix is copied before returning.
func (c *Connection) SendParts(first, second []byte) error {
	if l := c.layer; l != nil {
		return l.Send(first, second)
	}
	if c.udp != nil {
		return c.sendDatagramParts(first, second)
	}
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
	direct := c.canWriteDirectlyLocked()
	if direct {
		for {
			n, err := c.sysWrite2(first, second)
			if err == syscall.EINTR {
				continue
			}
			if err != nil && !isWouldBlock(err) {
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
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	refresh := !c.corked && c.pauseStateChangedLocked()
	c.mu.Unlock()
	if armErr != nil {
		c.closeWithError(armErr)
		return armErr
	}
	if refresh {
		c.engine.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// RunTask implements taskpool.Task without allocating a method value for each
// readiness notification.
func (c *Connection) RunTask() {
	defer c.engine.taskWG.Done()
	c.process()
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
	// before the next round so the notification is never lost: readiness is
	// edge-triggered, so a dropped read edge would only reappear once the peer
	// sent more data, leaving readable bytes stranded on a connection that
	// looks idle.
	var deferred uint32
	for {
		c.mu.Lock()
		c.pendingEvents |= deferred
		if c.pendingEvents == deferred {
			// Only the deferred readiness remains. Stop here instead of spinning:
			// the next write edge resubmits the connection, flushOutput runs
			// first, and drainInput follows once the queue is empty.
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
		if c.udp != nil {
			// The loop has already read the socket; the round only hands the
			// queued datagrams to the handler.
			if alive && events&evIn != 0 {
				c.drainDatagrams()
			}
			continue
		}
		var closeErr error
		// Flush before reading so that a round which both frees socket send
		// space and delivers new input never reads while output is still
		// queued behind it. This keeps userspace buffering bounded by what the
		// peer is willing to accept instead of what it is willing to send.
		if alive && events&evOut != 0 && c.hasQueuedOutput() {
			closeErr = c.flushOutput()
			alive = closeErr == nil
		}
		if alive && events&evPri != 0 {
			closeErr = c.drainPriorityInput()
			alive = closeErr == nil
		}
		if alive && events&evIn != 0 {
			if c.hasQueuedOutput() {
				// Queued output means write interest is armed or a write is in
				// flight, so a later round is guaranteed. Carry the read,
				// and any half-close that arrived with it, into that round so
				// the peer's final bytes are still delivered after the flush.
				deferred = evIn | events&evRdHup
			} else {
				closeErr = c.drainInput()
				alive = closeErr == nil
				if alive {
					c.rearmRead()
				}
			}
		}
		if alive && events&evErr != 0 {
			closeErr = c.socketError()
			alive = false
		} else if alive && events&(evHup|evRdHup)&^deferred != 0 {
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
	c.readStalled = false
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
	buffer := c.engine.readBufferPool.Get().(*readBuffer)
	buf := buffer.data
	defer c.engine.readBufferPool.Put(buffer)
	for {
		n, err := c.sysRead(buf)
		if n > 0 {
			c.handler.OnData(c, buf[:n])
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
					// The peer is behind. Stop reading and let the event loop
					// decide what happens to the bytes left behind: it pauses
					// reads, and re-arming them later redelivers what is left,
					// or, if the backlog has drained by the time it looks,
					// hands the read straight back. Without the stall mark a
					// backlog that drains before the loop gets there leaves
					// nothing to re-arm, and the bytes sit unread until the
					// peer happens to send more.
					c.mu.Lock()
					c.readStalled = true
					c.mu.Unlock()
					c.engine.request(command{kind: commandRefresh, connection: c})
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
		if isWouldBlock(err) {
			return nil
		}
		return err
	}
}

func (c *Connection) drainPriorityInput() error {
	buf := []byte{0}
	for {
		n, err := c.sysRecvOOB(buf)
		if n > 0 {
			c.handler.OnPriorityData(c, buf[:n])
			continue
		}
		if n == 0 && err == nil {
			return io.EOF
		}
		if err == syscall.EINTR {
			continue
		}
		// EINVAL means no urgent data is waiting, and EOPNOTSUPP a socket
		// that has none at all, such as a Unix one.
		if isWouldBlock(err) || err == syscall.EINVAL || err == syscall.EOPNOTSUPP {
			return nil
		}
		return err
	}
}

func (c *Connection) flushOutput() error {
	c.mu.Lock()
	if c.closing || c.closed || c.flushing || c.writeBusyLocked() {
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
				c.engine.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		var n int
		var err error
		attempted := 0
		pending := len(c.sends) - c.sendHead
		if c.engine.useWritev && pending > 1 {
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
			n, err = c.sysWritev(buffers)
		} else {
			item := c.sends[c.sendHead]
			attempted = len(item.data) - item.offset
			n, err = c.sysWrite(item.data[item.offset:])
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
				// retrying now would only earn an EAGAIN. Wait for the socket
				// to become writable again.
				c.flushing = false
				armErr := c.awaitWritableLocked()
				refresh := c.pauseStateChangedLocked()
				c.mu.Unlock()
				if armErr != nil {
					return armErr
				}
				if refresh {
					c.engine.request(command{kind: commandRefresh, connection: c})
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
		if isWouldBlock(err) {
			armErr := c.awaitWritableLocked()
			refresh := c.pauseStateChangedLocked()
			c.mu.Unlock()
			if armErr != nil {
				return armErr
			}
			if refresh {
				c.engine.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		c.mu.Unlock()
		return err
	}
}
