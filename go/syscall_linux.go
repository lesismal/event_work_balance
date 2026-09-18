//go:build linux

package fib

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

func acceptSocket(listenFD int) (int, error) {
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

func eventfd() (int, error) {
	r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(syscall.O_NONBLOCK|syscall.O_CLOEXEC), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r0), nil
}

func newSocket(family int) (int, error) {
	return syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
}

func writevRaw(fd int, iov []syscall.Iovec) (int, error) {
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}
