//go:build linux || darwin

package fib

import (
	"net"
	"sync/atomic"
	"syscall"
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
	// connPageShift sizes one page of the descriptor table. 4096 entries is
	// 32KB per page, small enough that a server holding few connections in a
	// wide descriptor range wastes little, large enough that the page
	// directory stays tiny.
	connPageShift = 12
	connPageSize  = 1 << connPageShift
	connPageMask  = connPageSize - 1
)

// enginePlatform is the state a readiness backend keeps on top of the shared
// engine: the listening descriptors and the table that maps a descriptor back
// to its connection. backend is the epoll or kqueue instance itself.
type enginePlatform struct {
	backend
	listenFDs      []int
	nextGeneration atomic.Uint64
	// connections is a paged table indexed by file descriptor: a descriptor is
	// a small dense integer the kernel already allocates, so the lookup on
	// every event is two bounds checks rather than a hash.
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
}

type connPlatform struct {
	fd    atomic.Int32
	token uint64
}

func (c *Connection) FD() int { return int(c.fd.Load()) }

func (e *Engine) open(config Config, addrs []string) error {
	e.nextGeneration.Store(firstGeneration)
	for _, addr := range addrs {
		fd, err := createListener(config, addr)
		if err != nil {
			e.closeListeners()
			return err
		}
		e.listenFDs = append(e.listenFDs, fd)
	}
	if err := e.openBackend(); err != nil {
		e.closeListeners()
		return err
	}
	return nil
}

func (e *Engine) closeListeners() {
	for _, fd := range e.listenFDs {
		syscall.Close(fd)
	}
	e.listenFDs = nil
}

func listenerToken(fd int) uint64 { return uint64(uint32(fd)) | listenerKind<<32 }
func wakeToken(fd int) uint64     { return uint64(uint32(fd)) | wakeKind<<32 }

// connectionAt returns the connection currently holding a descriptor.
func (e *Engine) connectionAt(fd int) *Connection {
	page := fd >> connPageShift
	if page < 0 || page >= len(e.connections) {
		return nil
	}
	entries := e.connections[page]
	if entries == nil {
		return nil
	}
	return entries[fd&connPageMask]
}

// connectionFor returns the connection a token refers to, or nil if the token
// is stale. The low half is the descriptor and the high half a generation, so a
// reused descriptor never resolves to the connection that previously held it.
func (e *Engine) connectionFor(token uint64) *Connection {
	c := e.connectionAt(int(uint32(token)))
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

func (e *Engine) isListener(fd int) bool {
	for _, listenFD := range e.listenFDs {
		if listenFD == fd {
			return true
		}
	}
	return false
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
		addr, err := sockaddrToTCPAddr(sa)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func (e *Engine) acceptConnections(listenFD int) {
	for {
		fd, err := acceptSocket(listenFD)
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
		c := &Connection{engine: e}
		c.token = token
		c.fd.Store(int32(fd))
		// Write interest is registered up front and never modified again. The
		// descriptor is edge-triggered, so an always-armed write interest only
		// fires when the socket goes from full back to writable, which spares
		// the loop a registration change per backpressured message.
		if err := e.registerConnection(fd, token); err != nil {
			syscall.Close(fd)
			continue
		}
		e.trackConnection(fd, c)
		e.handler.OnOpen(c)
	}
}

// detach releases a closed connection's descriptor and its table slot.
func (e *Engine) detach(c *Connection) {
	fd := int(c.fd.Swap(-1))
	if fd < 0 {
		return
	}
	e.unregister(fd)
	_ = syscall.Close(fd)
	if page := fd >> connPageShift; page < len(e.connections) {
		if entries := e.connections[page]; entries != nil && entries[fd&connPageMask] == c {
			entries[fd&connPageMask] = nil
		}
	}
}

// Close releases all resources. Run must have returned before Close is called.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.Stop()
		e.taskWG.Wait()
		e.releaseTaskPool()
		e.closeCommands()
		for _, entries := range e.connections {
			for _, c := range entries {
				if c != nil {
					e.closeConnection(c, nil, false)
				}
			}
		}
		e.budgetPaused = nil
		for _, fd := range e.listenFDs {
			if err := syscall.Close(fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		if err := e.closeBackend(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func createListener(config Config, addr string) (int, error) {
	family, bound, err := resolveListenAddr(config.Network, addr)
	if err != nil {
		return -1, err
	}
	fd, err := newSocket(family)
	if err != nil {
		return -1, err
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
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
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

// The readiness backends never have a write outstanding in the kernel: a
// blocked write simply waits for the next write edge, which is always armed.
func (c *Connection) writeBusyLocked() bool      { return false }
func (c *Connection) awaitWritableLocked() error { return nil }
func (c *Connection) rearmRead()                 {}

func isWouldBlock(err error) bool { return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK }

func (c *Connection) sysRead(buf []byte) (int, error)  { return syscall.Read(c.FD(), buf) }
func (c *Connection) sysWrite(buf []byte) (int, error) { return syscall.Write(c.FD(), buf) }

func (c *Connection) sysRecvOOB(buf []byte) (int, error) {
	n, _, err := syscall.Recvfrom(c.FD(), buf, syscall.MSG_OOB)
	return n, err
}

func (c *Connection) sysWrite2(first, second []byte) (int, error) {
	if len(first) == 0 {
		return syscall.Write(c.FD(), second)
	}
	if len(second) == 0 {
		return syscall.Write(c.FD(), first)
	}
	var iov [2]syscall.Iovec
	iov[0].Base = &first[0]
	iov[0].SetLen(len(first))
	iov[1].Base = &second[0]
	iov[1].SetLen(len(second))
	return writevRaw(c.FD(), iov[:])
}

func (c *Connection) sysWritev(buffers [][]byte) (int, error) {
	var iov [maxWritevItems]syscall.Iovec
	count := 0
	for _, b := range buffers {
		if len(b) > 0 {
			if count == len(iov) {
				break
			}
			iov[count].Base = &b[0]
			iov[count].SetLen(len(b))
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	return writevRaw(c.FD(), iov[:count])
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
