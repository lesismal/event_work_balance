//go:build linux

package fib

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

func accept4(listenFD int) (int, error) {
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

// resolveListenAddr turns a net.Listen network and address into the socket
// address to bind, using the net package so that a host, a service name and an
// empty address all mean here what they mean there.
func resolveListenAddr(network, addr string) (family int, sa syscall.Sockaddr, err error) {
	switch network {
	case "", "tcp":
		network = "tcp"
	case "tcp4", "tcp6":
	default:
		return 0, nil, net.UnknownNetworkError(network)
	}
	if addr == "" {
		// net.Listen reads an empty address as every interface on a port of
		// the kernel's choosing.
		addr = ":0"
	}
	resolved, err := net.ResolveTCPAddr(network, addr)
	if err != nil {
		return 0, nil, err
	}
	ip4 := resolved.IP.To4()
	switch {
	case network == "tcp4":
		if resolved.IP != nil && ip4 == nil {
			return 0, nil, fmt.Errorf("address %q is not IPv4", addr)
		}
		bound := &syscall.SockaddrInet4{Port: resolved.Port}
		copy(bound.Addr[:], ip4)
		return syscall.AF_INET, bound, nil
	case ip4 != nil:
		// An IPv4 literal under "tcp" binds an IPv4 socket, as net.Listen does.
		bound := &syscall.SockaddrInet4{Port: resolved.Port}
		copy(bound.Addr[:], ip4)
		return syscall.AF_INET, bound, nil
	default:
		bound := &syscall.SockaddrInet6{Port: resolved.Port}
		copy(bound.Addr[:], resolved.IP.To16())
		if resolved.Zone != "" {
			zone, zoneErr := net.InterfaceByName(resolved.Zone)
			if zoneErr != nil {
				return 0, nil, zoneErr
			}
			bound.ZoneId = uint32(zone.Index)
		}
		return syscall.AF_INET6, bound, nil
	}
}

func createListener(config Config, addr string) (int, error) {
	family, bound, err := resolveListenAddr(config.Network, addr)
	if err != nil {
		return -1, err
	}
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
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

func eventfd() (int, error) {
	r0, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(syscall.O_NONBLOCK|syscall.O_CLOEXEC), 0)
	if errno != 0 {
		return -1, errno
	}
	return int(r0), nil
}

func writev(fd int, buffers [][]byte) (int, error) {
	var iov [maxWritevItems]syscall.Iovec
	count := 0
	for _, b := range buffers {
		if len(b) > 0 {
			if count == len(iov) {
				break
			}
			iov[count] = syscall.Iovec{Base: &b[0], Len: uint64(len(b))}
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(count))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}

func writev2(fd int, first, second []byte) (int, error) {
	if len(first) == 0 {
		return syscall.Write(fd, second)
	}
	if len(second) == 0 {
		return syscall.Write(fd, first)
	}
	var iov [2]syscall.Iovec
	count := 0
	if len(first) != 0 {
		iov[count] = syscall.Iovec{Base: &first[0], Len: uint64(len(first))}
		count++
	}
	if len(second) != 0 {
		iov[count] = syscall.Iovec{Base: &second[0], Len: uint64(len(second))}
		count++
	}
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(count))
	if errno != 0 {
		return int(r0), errno
	}
	return int(r0), nil
}
