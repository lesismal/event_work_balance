//go:build darwin

package fib

import (
	"sync"
	"syscall"
	_ "unsafe" // for go:linkname
)

// Darwin has neither accept4 nor SOCK_NONBLOCK, so a new descriptor gets its
// flags after the call that creates it. ForkLock keeps a concurrent fork from
// inheriting it in between.
func acceptSocket(listenFD int) (int, error) {
	syscall.ForkLock.RLock()
	fd, _, err := syscall.Accept(listenFD)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, err
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	// Writing to a socket the peer has reset raises SIGPIPE on Darwin unless
	// the socket opts out; the error return is all this package needs.
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_NOSIGPIPE, 1)
	return fd, nil
}

func newSocket(family int) (int, error) {
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM, 0)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, err
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

var (
	backlogOnce  sync.Once
	backlogValue int
)

// defaultBacklog reports the accept queue depth the kernel is willing to
// honour, which is what net.Listen asks for. See the Linux version for why the
// historical SOMAXCONN of 128 is too shallow for a connection burst.
func defaultBacklog() int {
	backlogOnce.Do(func() {
		backlogValue = syscall.SOMAXCONN
		if limit, err := syscall.SysctlUint32("kern.ipc.somaxconn"); err == nil && limit > 0 {
			backlogValue = int(limit)
		}
	})
	return backlogValue
}

// writev is the libc wrapper the syscall package already binds. Darwin wants
// system calls to go through libc rather than trapping directly.
//
//go:linkname writev syscall.writev
func writev(fd int, iovecs []syscall.Iovec) (cnt uintptr, err error)

func writevRaw(fd int, iov []syscall.Iovec) (int, error) {
	n, err := writev(fd, iov)
	return int(n), err
}
