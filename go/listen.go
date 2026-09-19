//go:build linux || darwin || windows

package fib

import (
	"fmt"
	"net"
	"runtime"
	"syscall"
)

// resolveListenAddr turns a net.Listen network and address into the socket
// address to bind, using the net package so that a host, a service name and an
// empty address all mean here what they mean there.
//
// The "udp" networks resolve the same way, as net.ListenPacket reads them, and
// "unix" takes addr as the socket's path, as net.Listen does.
func resolveListenAddr(network, addr string) (family int, sa syscall.Sockaddr, err error) {
	if isUnixNetwork(network) {
		return resolveUnixAddr(addr)
	}
	switch network {
	case "":
		network = "tcp"
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return 0, nil, net.UnknownNetworkError(network)
	}
	if addr == "" {
		// net.Listen reads an empty address as every interface on a port of
		// the kernel's choosing.
		addr = ":0"
	}
	resolved, err := resolveAddr(network, addr)
	if err != nil {
		return 0, nil, err
	}
	return tcpAddrToSockaddr(network, addr, resolved)
}

// resolveAddr resolves a TCP or UDP address. A UDP address comes back in a
// TCPAddr too: only its IP, port and zone matter from here on.
func resolveAddr(network, addr string) (*net.TCPAddr, error) {
	if !isUDPNetwork(network) {
		return net.ResolveTCPAddr(network, addr)
	}
	resolved, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	return &net.TCPAddr{IP: resolved.IP, Port: resolved.Port, Zone: resolved.Zone}, nil
}

// resolveDialAddr turns a net.Dial network and address into the socket address
// to connect to. A missing host means the local system, as it does to
// net.Dial.
//
// A "unix" address is a path and needs no resolving; raddr stays nil.
func resolveDialAddr(network, addr string) (family int, sa syscall.Sockaddr, raddr *net.TCPAddr, err error) {
	if !isDialNetwork(network) {
		return 0, nil, nil, net.UnknownNetworkError(network)
	}
	if isUnixNetwork(network) {
		family, sa, err = resolveUnixAddr(addr)
		return family, sa, nil, err
	}
	if network == "" {
		network = "tcp"
	}
	raddr, err = resolveAddr(network, addr)
	if err != nil {
		return 0, nil, nil, err
	}
	if raddr.IP == nil || raddr.IP.IsUnspecified() {
		loopback := net.IPv4(127, 0, 0, 1)
		if network == "tcp6" || network == "udp6" || (raddr.IP != nil && raddr.IP.To4() == nil) {
			loopback = net.IPv6loopback
		}
		raddr = &net.TCPAddr{IP: loopback, Port: raddr.Port}
	}
	family, sa, err = tcpAddrToSockaddr(network, addr, raddr)
	return family, sa, raddr, err
}

// isDialNetwork reports whether network is one this package listens and
// dials on. Empty means "tcp".
func isDialNetwork(network string) bool {
	switch network {
	case "", "tcp", "tcp4", "tcp6":
		return true
	}
	return isUDPNetwork(network) || isUnixNetwork(network)
}

// isUnixNetwork reports whether network names a Unix domain stream socket.
func isUnixNetwork(network string) bool { return network == "unix" }

// resolveUnixAddr turns a socket path into the address to bind or connect to.
// On Linux a leading '@' names an abstract socket, as it does to the net
// package; the syscall package makes that translation.
func resolveUnixAddr(path string) (family int, sa syscall.Sockaddr, err error) {
	if path == "" {
		return 0, nil, &net.AddrError{Err: "missing unix socket path", Addr: path}
	}
	return syscall.AF_UNIX, &syscall.SockaddrUnix{Name: path}, nil
}

// isAbstractUnixPath reports whether path names a Linux abstract socket, which
// has no file to remove.
func isAbstractUnixPath(path string) bool {
	return runtime.GOOS == "linux" && len(path) > 0 && path[0] == '@'
}

// sockaddrToAddr reports a socket address the way net reports one: a
// *net.TCPAddr for an IP socket and a *net.UnixAddr for a Unix one. It
// reports nil for anything else.
func sockaddrToAddr(sa syscall.Sockaddr) net.Addr {
	if unix, ok := sa.(*syscall.SockaddrUnix); ok {
		return &net.UnixAddr{Name: unix.Name, Net: "unix"}
	}
	addr, err := sockaddrToTCPAddr(sa)
	if err != nil {
		return nil
	}
	return addr
}

// tcpAddrToSockaddr picks the socket family and address for a resolved TCP
// address. An IPv4 address under "tcp" gets an IPv4 socket, as it does from the
// net package.
func tcpAddrToSockaddr(network, addr string, resolved *net.TCPAddr) (family int, sa syscall.Sockaddr, err error) {
	ip4 := resolved.IP.To4()
	switch {
	case network == "tcp4" || network == "udp4":
		if resolved.IP != nil && ip4 == nil {
			return 0, nil, fmt.Errorf("address %q is not IPv4", addr)
		}
		bound := &syscall.SockaddrInet4{Port: resolved.Port}
		copy(bound.Addr[:], ip4)
		return syscall.AF_INET, bound, nil
	case ip4 != nil:
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

// sockaddrToTCPAddr reports a bound socket address the way net reports one.
func sockaddrToTCPAddr(sa syscall.Sockaddr) (*net.TCPAddr, error) {
	switch bound := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port}, nil
	case *syscall.SockaddrInet6:
		addr := &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port}
		if bound.ZoneId != 0 {
			if zone, zoneErr := net.InterfaceByIndex(int(bound.ZoneId)); zoneErr == nil {
				addr.Zone = zone.Name
			}
		}
		return addr, nil
	default:
		return nil, fmt.Errorf("listener is not TCP: %T", sa)
	}
}
