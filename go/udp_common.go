package fib

import (
	"errors"
	"time"
)

// DefaultUDPIdleTimeout is how long a UDP peer may stay silent before its
// connection is closed, when Config.UDPIdleTimeout is left at zero.
const DefaultUDPIdleTimeout = 60 * time.Second

// ErrUDPIdleTimeout is what OnClose receives for a UDP peer that neither sent
// nor was sent anything for Config.UDPIdleTimeout.
var ErrUDPIdleTimeout = errors.New("fib: udp peer idle timeout")

const (
	// maxDatagramSize holds the largest UDP payload, so no datagram is ever
	// truncated on the way in.
	maxDatagramSize = 64 << 10
	// maxQueuedDatagrams bounds the datagrams a connection holds for its
	// handler. Beyond it new ones are dropped, as the kernel drops them when
	// a socket's receive buffer is full: a peer that sends faster than its
	// handler keeps up loses datagrams instead of growing memory.
	maxQueuedDatagrams = 1024
	// maxDatagramsPerRound bounds the datagrams one socket may deliver in a
	// single round of the event loop, so a flooded socket cannot starve the
	// others. UDP sockets are level-triggered, so what is left is picked up in
	// the next round.
	maxDatagramsPerRound = 256
)

// isUDPNetwork reports whether network names UDP.
func isUDPNetwork(network string) bool {
	switch network {
	case "udp", "udp4", "udp6":
		return true
	}
	return false
}

// udpIdleTimeout reads Config.UDPIdleTimeout: zero is the default, and a
// negative value turns the timeout off, which is reported as zero.
func udpIdleTimeout(configured time.Duration) time.Duration {
	switch {
	case configured == 0:
		return DefaultUDPIdleTimeout
	case configured < 0:
		return 0
	}
	return configured
}

// udpSweepInterval is how often idle peers are looked for: often enough that
// a peer outlives its timeout by at most a quarter of it.
func udpSweepInterval(timeout time.Duration) time.Duration {
	return max(timeout/4, 10*time.Millisecond)
}
