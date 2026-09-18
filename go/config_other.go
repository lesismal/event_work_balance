//go:build !linux && !darwin && !windows

package fib

import "github.com/lesismal/fib/go/taskpool"

type Config struct {
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". An empty Network means "tcp", and an
	// empty Addr means ":0".
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares its connection table, task pool and buffer pool
	// across all of them.
	Addrs                           []string
	Backlog, WorkerCount, MaxEvents int
	ReadBufferSize                  int
	WriteBufferHighWatermark        int
	UseWritev                       bool
	// TaskPoolMode picks the scheduler the workers run under. Prefer
	// SetTaskPoolMode over assigning it, so that WorkerCount and MaxEvents
	// follow the mode rather than staying at numbers tuned for the other one.
	TaskPoolMode   taskpool.Mode
	SharedTaskPool bool
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeCond)
	return Config{Network: "tcp", Addr: ":9000", Backlog: 128, WorkerCount: sizing.WorkerCount, MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024, WriteBufferHighWatermark: 4 * 1024, UseWritev: true, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
}
