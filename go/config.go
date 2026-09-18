package fib

import (
	"runtime"

	"github.com/lesismal/fib/go/taskpool"
)

// PoolSizing is how many workers a task pool may run and how many tasks it may
// hold waiting for them. MaxEvents also sizes the epoll batch the event loop
// collects in one round.
type PoolSizing struct {
	WorkerCount int
	MaxEvents   int
}

// Pools are oversubscribed relative to the cores they run on, because a worker
// drives its connection's read and write syscalls itself and so spends much of
// a round inside the kernel rather than on a P. Sizing a pool to GOMAXPROCS
// leaves cores idle whenever its workers are in flight.
//
// How far to oversubscribe depends on the mode, because the worker count does
// not mean the same thing in both.
//
// ModeCond creates every worker up front and parks it on a condition variable,
// so the count is a population of goroutines that exists whether or not there
// is work, and paying for more of them than the load needs makes the scheduler
// move more goroutines between the same cores. A 100k-connection echo run
// measured, on a five-core cpuset: 255k echoes/s with 16 workers, 319k with 64,
// 331k with 250, 350k with 500, then back down to 340k with 2000 and 314k with
// 5000.
//
// ModeElastic forks a worker per submission while it has capacity and retires
// it after a short idle linger, so the count is a ceiling instead of a
// population: raising it costs nothing until the load actually climbs to it,
// and lowering it throttles a burst that the cores could have absorbed. Its
// default is therefore an order of magnitude higher than the cond one.
//
// Note that either pool needs a GOMAXPROCS above the core count to pay off,
// since its workers hold their P while they are in a syscall. See the note on
// GOMAXPROCS in README.zh-CN.md: the same run went from 330k to 415k echoes/s,
// and from 55k to 71k accepted connections/s, on GOMAXPROCS alone.
const (
	condWorkersPerCPU    = 100
	condMinWorkers       = 256
	elasticWorkersPerCPU = 1000
	elasticMinWorkers    = 10000

	// The queue holds connections the loop has made runnable but no worker has
	// picked up yet, so it is sized from the pool rather than independently,
	// within bounds that keep a small pool's queue usable and a large pool's
	// queue from being allocated far wider than a round can fill.
	eventsPerWorker = 10
	minMaxEvents    = 10000
	maxMaxEvents    = 100000
)

// DefaultPoolSizing reports the sizing mode is tuned for, which is what
// DefaultConfig and SetTaskPoolMode install.
func DefaultPoolSizing(mode taskpool.Mode) PoolSizing {
	cpuCount := runtime.GOMAXPROCS(0)
	workerCount := cpuCount * condWorkersPerCPU
	minWorkers := condMinWorkers
	if mode == taskpool.ModeElastic {
		workerCount = cpuCount * elasticWorkersPerCPU
		minWorkers = elasticMinWorkers
	}
	if workerCount < minWorkers {
		workerCount = minWorkers
	}
	maxEvents := workerCount * eventsPerWorker
	if maxEvents < minMaxEvents {
		maxEvents = minMaxEvents
	} else if maxEvents > maxMaxEvents {
		maxEvents = maxMaxEvents
	}
	return PoolSizing{WorkerCount: workerCount, MaxEvents: maxEvents}
}

// SetTaskPoolMode switches the task pool mode and moves the pool sizing to the
// one that mode is tuned for, since what a worker count buys differs between
// them. Sizing pinned by SetPoolSizing is left alone, so the two may be called
// in either order.
func (c *Config) SetTaskPoolMode(mode taskpool.Mode) *Config {
	c.TaskPoolMode = mode
	if !c.customPoolSizing {
		sizing := DefaultPoolSizing(mode)
		c.WorkerCount = sizing.WorkerCount
		c.MaxEvents = sizing.MaxEvents
	}
	return c
}

// SetPoolSizing pins the pool sizing to the caller's own numbers, which a later
// SetTaskPoolMode then keeps. A value that is not positive leaves that field at
// what it already held.
//
// WorkerCount is a population of parked goroutines under ModeCond and a ceiling
// on forked ones under ModeElastic; DefaultPoolSizing documents what each mode
// does with it.
func (c *Config) SetPoolSizing(workerCount, maxEvents int) *Config {
	if workerCount > 0 {
		c.WorkerCount = workerCount
	}
	if maxEvents > 0 {
		c.MaxEvents = maxEvents
	}
	c.customPoolSizing = true
	return c
}
