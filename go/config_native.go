//go:build linux || darwin || windows

package fib

import (
	"github.com/lesismal/fib/go/taskpool"
)

// Config controls listener and worker-pool sizing.
type Config struct {
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". A host resolves through the net
	// package, and a zero port asks the kernel to choose one. An empty Network
	// means "tcp", and an empty Addr means ":0", again as net.Listen reads
	// them.
	//
	// "tcp" with no host listens on both families where the kernel has IPv6,
	// "tcp4" and "tcp6" pin it to one. This is the same choice net.Listen
	// makes from the same arguments.
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares a single event loop, descriptor table, task
	// pool and buffer pool across all of them, where a server per address
	// gives each its own copy of all four.
	Addrs          []string
	Backlog        int
	WorkerCount    int
	MaxEvents      int
	ReadBufferSize int
	// WriteBufferHighWatermark pauses socket reads while at least this many
	// bytes are waiting to be written. This bounds userspace buffering while
	// TCP backpressure catches up. Set below zero to disable write backpressure.
	//
	// Crossing it is not free: the worker hands a command to the event loop,
	// which costs an eventfd write and an epoll_ctl. A watermark near the
	// message size makes every reply a crossing, so keep it well above one
	// round's worth of output.
	WriteBufferHighWatermark int
	// MaxPendingBytes caps the bytes this server may hold across all of its
	// connections waiting for their sockets. WriteBufferHighWatermark bounds a
	// single connection, which at 100k connections still admits a per-server
	// total of watermark*100k; this is the bound on the sum. Reads pause on
	// every connection while the budget is exhausted. Zero means unlimited.
	MaxPendingBytes int64
	UseWritev       bool
	// TaskPoolMode picks the scheduler the workers run under. Prefer
	// SetTaskPoolMode over assigning it, so that WorkerCount and MaxEvents
	// follow the mode rather than staying at numbers tuned for the other one.
	TaskPoolMode taskpool.Mode
	// MinWorkerCount is the resident floor a ModeAdaptive pool retires down
	// to, with WorkerCount as the ceiling it grows to. Zero means one worker
	// per P. The other modes ignore it.
	MinWorkerCount int
	SharedTaskPool bool
	// TaskPool, when set, runs the engine's connections instead of a pool the
	// engine builds from the fields above. See SetTaskPool.
	TaskPool TaskPool
	// InlineHandlers runs a ready connection's round on the event loop instead
	// of handing it to a worker.
	//
	// The handoff is not free, and at high message rates it is the dominant
	// cost: it makes a goroutine runnable, and that goroutine has to be given a
	// P before it can issue the read. An execution trace of a 100k-connection
	// echo run measured 872 seconds of runnable-but-not-running time in a
	// 2-second window, almost all of it on workers woken from the loop.
	// Skipping the handoff measured 446k echoes/s against 395k for the same
	// build with workers, and 104k accepted connections/s against 95k.
	//
	// The cost is that a handler now blocks its whole server: the loop cannot
	// collect events, accept, or serve any other connection while it runs. Set
	// this only when every handler is short and never blocks. Handlers that do
	// I/O, take contended locks, or run unbounded work want the worker pool,
	// which exists precisely so that one slow connection cannot stall the rest.
	InlineHandlers bool
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeElastic)
	return Config{Network: "tcp", Addr: ":9000", Backlog: defaultBacklog(), WorkerCount: sizing.WorkerCount,
		MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024,
		WriteBufferHighWatermark: defaultWriteHighWatermark, MaxPendingBytes: defaultMaxPendingBytes,
		UseWritev: true, TaskPoolMode: taskpool.ModeElastic, SharedTaskPool: true}
}
