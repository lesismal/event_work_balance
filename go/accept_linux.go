//go:build linux && !386

package fib

import "syscall"

// acceptSocket accepts a connection already non-blocking and close-on-exec.
// It calls accept4 directly, since the address syscall.Accept4 would decode
// is not wanted.
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
