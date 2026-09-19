//go:build linux

package fib

import (
	"encoding/binary"
	"syscall"

	"github.com/lesismal/fib/go/taskpool"
)

const (
	epollET    = uint32(1 << 31)
	baseEvents = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
		syscall.EPOLLHUP|syscall.EPOLLRDHUP) | epollET
	allEvents = baseEvents | syscall.EPOLLOUT
)

// The readiness bits are epoll's own, so events pass through untranslated. A
// mismatch fails to compile: the array length is zero only when they agree.
var _ [0]struct{} = [evIn ^ syscall.EPOLLIN | evPri ^ syscall.EPOLLPRI | evOut ^ syscall.EPOLLOUT |
	evErr ^ syscall.EPOLLERR | evHup ^ syscall.EPOLLHUP | evRdHup ^ syscall.EPOLLRDHUP]struct{}{}

// backend is an epoll instance plus the eventfd that other goroutines use to
// wake it.
type backend struct {
	epollFD, wakeFD int
}

func (e *Engine) openBackend() error {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return err
	}
	wakeFD, err := eventfd()
	if err != nil {
		syscall.Close(epfd)
		return err
	}
	e.epollFD, e.wakeFD = epfd, wakeFD
	for _, fd := range e.listenFDs {
		if err = e.addFD(fd, listenerToken(fd), uint32(syscall.EPOLLIN)|epollET); err != nil {
			break
		}
	}
	// UDP sockets are level-triggered, so the loop can stop reading one after
	// its share of a round and still hear about the rest.
	for _, l := range e.udpListeners {
		if err == nil {
			err = e.addFD(l.fd, udpToken(l.fd), uint32(syscall.EPOLLIN))
		}
	}
	if err == nil {
		err = e.addFD(wakeFD, wakeToken(wakeFD), uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		syscall.Close(wakeFD)
		syscall.Close(epfd)
		return err
	}
	return nil
}

func (e *Engine) closeBackend() error {
	err := syscall.Close(e.wakeFD)
	if closeErr := syscall.Close(e.epollFD); err == nil {
		err = closeErr
	}
	return err
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
			case udpKind:
				if l := e.udpListenerAt(int(uint32(token))); l != nil {
					ready = e.readUDPListener(l, ready)
				}
			default:
				if c := e.connectionFor(token); c != nil {
					if c.udp != nil {
						ready = e.readUDPConnection(c, ready)
						continue
					}
					if c.dialing != nil {
						c = e.finishDial(c, events[i].Events, nil)
					} else {
						c = e.noteEvent(c, events[i].Events)
					}
					if c != nil {
						ready = append(ready, c)
					}
				}
			}
		}
		ready, tasks = e.runReady(ready, tasks)
	}
	e.drainCommands()
	return nil
}

func (e *Engine) wake() {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = syscall.Write(e.wakeFD, b[:])
}

// ackWake drains the eventfd. A single read is enough: reading returns the
// whole counter and resets it to zero, so looping until EAGAIN only adds a
// wasted syscall.
func (e *Engine) ackWake() {
	var b [8]byte
	for {
		if _, err := syscall.Read(e.wakeFD, b[:]); err != syscall.EINTR {
			break
		}
	}
}

func (e *Engine) addFD(fd int, token uint64, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(token), Pad: int32(token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_ADD, fd, &ev)
}

func (e *Engine) registerConnection(fd int, token uint64) error {
	return e.addFD(fd, token, allEvents)
}

// registerDatagram registers a dialed UDP socket. Only reads matter, since
// sends never wait, and it is level-triggered like a UDP listener.
func (e *Engine) registerDatagram(fd int, token uint64) error {
	return e.addFD(fd, token, uint32(syscall.EPOLLIN|syscall.EPOLLERR))
}

func (e *Engine) unregister(fd int) {
	_ = syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_DEL, fd, nil)
}

func (e *Engine) setReadPaused(c *Connection, paused bool) error {
	events := allEvents
	if paused {
		events &^= syscall.EPOLLIN
	}
	ev := syscall.EpollEvent{Events: events, Fd: int32(c.token), Pad: int32(c.token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_MOD, c.FD(), &ev)
}
