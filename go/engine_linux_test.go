//go:build linux

package fib

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// TestDefaultBacklogMatchesKernelLimit keeps the accept queue as deep as the
// one net.Listen asks for, which is what every framework built on it gets. An
// overflowing accept queue does not refuse connections: the kernel drops the
// client's ACK, so a connection burst shows up only as clients sitting on
// SYN-ACK retransmission timers. Measured against a 3000-connection burst, the
// historical SOMAXCONN of 128 cost 1399 upgrades per second and a median of
// 1.06s, against 63064 per second and 34ms at the kernel's own limit.
func TestDefaultBacklogMatchesKernelLimit(t *testing.T) {
	want := syscall.SOMAXCONN
	if data, err := os.ReadFile("/proc/sys/net/core/somaxconn"); err == nil {
		if limit, parseErr := strconv.Atoi(strings.TrimSpace(string(data))); parseErr == nil && limit > 0 {
			want = limit
			if want > 1<<16-1 {
				want = 1<<16 - 1
			}
		}
	}
	if got := DefaultConfig().Backlog; got != want {
		t.Fatalf("default backlog = %d, want the kernel limit %d", got, want)
	}
}
