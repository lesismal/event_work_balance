//go:build linux || darwin || windows

package fib

import (
	"fmt"
	"net"
	"syscall"
)

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
