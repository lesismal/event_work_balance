//go:build linux && 386

package fib

import "syscall"

// acceptSocket accepts a connection already non-blocking and close-on-exec.
// 32-bit x86 has no accept4 system call of its own, only the socketcall
// multiplexer, which syscall.Accept4 goes through.
func acceptSocket(listenFD int) (int, error) {
	fd, _, err := syscall.Accept4(listenFD, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
	if err != nil {
		return -1, err
	}
	return fd, nil
}
