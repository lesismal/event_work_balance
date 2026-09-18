//go:build darwin

package fib

import (
	"syscall"

	"github.com/lesismal/fib/go/taskpool"
)

// wakeIdent names the EVFILT_USER event other goroutines trigger to wake the
// loop. User events live in their own namespace, so it cannot collide with a
// descriptor.
const wakeIdent = 0

// EVFILT_EXCEPT with NOTE_OOB reports urgent data, which the read filter does
// not. The syscall package does not export either; the values are from
// <sys/event.h>.
const (
	evfiltExcept = -15
	noteOOB      = 0x2
)

// backend is a kqueue instance. Every descriptor is registered with EV_CLEAR,
// which gives kqueue the same edge-triggered behaviour the epoll backend gets
// from EPOLLET.
type backend struct {
	kq int
	// acceptable collects the listeners that became readable during one wait.
	// Event-loop ownership.
	acceptable []int
}

func (e *Engine) openBackend() error {
	syscall.ForkLock.RLock()
	kq, err := syscall.Kqueue()
	if err == nil {
		syscall.CloseOnExec(kq)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	changes := make([]syscall.Kevent_t, 0, len(e.listenFDs)+1)
	for _, fd := range e.listenFDs {
		changes = append(changes, syscall.Kevent_t{Ident: uint64(fd), Filter: syscall.EVFILT_READ,
			Flags: syscall.EV_ADD | syscall.EV_CLEAR})
	}
	changes = append(changes, syscall.Kevent_t{Ident: wakeIdent, Filter: syscall.EVFILT_USER,
		Flags: syscall.EV_ADD | syscall.EV_CLEAR})
	if _, err = syscall.Kevent(kq, changes, nil, nil); err != nil {
		syscall.Close(kq)
		return err
	}
	e.kq = kq
	return nil
}

func (e *Engine) closeBackend() error { return syscall.Close(e.kq) }

func (e *Engine) Run() error {
	batch := e.maxEvents
	if batch > maxWaitBatch {
		batch = maxWaitBatch
	}
	events := make([]syscall.Kevent_t, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	for !e.stopping.Load() {
		n, err := syscall.Kevent(e.kq, nil, events, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		woken := false
		for i := 0; i < n; i++ {
			ev := &events[i]
			if ev.Filter == syscall.EVFILT_USER {
				woken = true
				continue
			}
			fd := int(ev.Ident)
			c := e.connectionAt(fd)
			if c == nil {
				if ev.Filter == syscall.EVFILT_READ && e.isListener(fd) {
					e.acceptable = append(e.acceptable, fd)
				}
				continue
			}
			if c = e.noteEvent(c, kqueueEvents(ev)); c != nil {
				ready = append(ready, c)
			}
		}
		// A kevent names only a descriptor, with no generation to tell one
		// owner of it from the next, so nothing may close or accept a
		// descriptor while events that name it are still being read. Closes
		// and accepts therefore wait until the whole batch has been folded
		// into connections. Closing a descriptor removes its pending events
		// from the kqueue, so the next wait cannot see a stale one either.
		if woken {
			e.drainCommands()
		}
		for _, fd := range e.acceptable {
			e.acceptConnections(fd)
		}
		e.acceptable = e.acceptable[:0]
		ready, tasks = e.runReady(ready, tasks)
	}
	e.drainCommands()
	return nil
}

// kqueueEvents translates one kevent into the readiness bits a connection
// accumulates.
func kqueueEvents(ev *syscall.Kevent_t) uint32 {
	var events uint32
	switch ev.Filter {
	case syscall.EVFILT_READ:
		events = evIn
		if ev.Flags&syscall.EV_EOF != 0 {
			// The peer has finished sending. Whatever it sent first is still
			// readable, which is what the read-side half-close means under
			// epoll too.
			events |= evRdHup
		}
	case syscall.EVFILT_WRITE:
		events = evOut
	case evfiltExcept:
		events = evPri
	}
	if ev.Flags&syscall.EV_EOF != 0 && ev.Fflags != 0 {
		// On EOF, fflags carries the socket error, if there was one.
		events |= evErr
	}
	return events
}

func (e *Engine) wake() {
	changes := [1]syscall.Kevent_t{{Ident: wakeIdent, Filter: syscall.EVFILT_USER, Fflags: syscall.NOTE_TRIGGER}}
	for {
		if _, err := syscall.Kevent(e.kq, changes[:], nil, nil); err != syscall.EINTR {
			return
		}
	}
}

// ackWake does nothing: the user event is registered with EV_CLEAR, so
// delivering it has already reset it.
func (e *Engine) ackWake() {}

func (e *Engine) registerConnection(fd int, _ uint64) error {
	changes := [3]syscall.Kevent_t{
		{Ident: uint64(fd), Filter: syscall.EVFILT_READ, Flags: syscall.EV_ADD | syscall.EV_CLEAR},
		{Ident: uint64(fd), Filter: syscall.EVFILT_WRITE, Flags: syscall.EV_ADD | syscall.EV_CLEAR},
		{Ident: uint64(fd), Filter: evfiltExcept, Flags: syscall.EV_ADD | syscall.EV_CLEAR, Fflags: noteOOB},
	}
	_, err := syscall.Kevent(e.kq, changes[:], nil, nil)
	return err
}

// unregister does nothing: closing a descriptor removes its kevents.
func (e *Engine) unregister(int) {}

// setReadPaused removes the read filter to pause reads and adds it back to
// resume them. Adding a filter evaluates it straight away, so bytes that were
// already waiting in the socket raise an event without the peer sending more,
// which is the redelivery the paused read relies on.
func (e *Engine) setReadPaused(c *Connection, paused bool) error {
	change := [1]syscall.Kevent_t{{Ident: uint64(c.FD()), Filter: syscall.EVFILT_READ,
		Flags: syscall.EV_ADD | syscall.EV_CLEAR}}
	if paused {
		change[0].Flags = syscall.EV_DELETE
	}
	_, err := syscall.Kevent(e.kq, change[:], nil, nil)
	if paused && err == syscall.ENOENT {
		err = nil
	}
	return err
}
