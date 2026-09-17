package epoll

import (
	"runtime"
	"testing"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	// The exact sizing is defaultPoolSizing's business and is tuned by
	// measurement; restating its arithmetic here would only pin the test to
	// whatever the constants happen to be. What DefaultConfig owes is that it
	// reports that sizing rather than one of its own.
	wantWorkers, wantEvents := defaultPoolSizing()
	if config.WorkerCount != wantWorkers {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, wantWorkers)
	}
	if config.MaxEvents != wantEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, wantEvents)
	}
	// Workers are deliberately oversubscribed relative to cores: each one runs
	// its connection's syscalls inline, so a pool the size of GOMAXPROCS leaves
	// cores idle whenever workers are in the kernel.
	if config.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("WorkerCount = %d, want more than GOMAXPROCS %d", config.WorkerCount, runtime.GOMAXPROCS(0))
	}
	// The event batch sizes the task queue, which has to be able to hold a
	// round's worth of runnable connections; a queue narrower than the pool
	// would make the event loop wait on workers it has already woken.
	if config.MaxEvents < config.WorkerCount {
		t.Fatalf("MaxEvents = %d, want at least WorkerCount %d", config.MaxEvents, config.WorkerCount)
	}
	if !config.UseWritev {
		t.Fatal("UseWritev = false, want adaptive writev enabled by default")
	}
	if config.WriteBufferHighWatermark != defaultWriteHighWatermark {
		t.Fatalf("WriteBufferHighWatermark = %d, want %d", config.WriteBufferHighWatermark, defaultWriteHighWatermark)
	}
	if config.MaxPendingBytes != defaultMaxPendingBytes {
		t.Fatalf("MaxPendingBytes = %d, want %d", config.MaxPendingBytes, defaultMaxPendingBytes)
	}
	// The server-wide budget has to sit well above a single connection's, or it
	// would pause every connection as soon as one of them backed up.
	if config.MaxPendingBytes <= int64(config.WriteBufferHighWatermark) {
		t.Fatalf("MaxPendingBytes = %d, want more than the per-connection watermark %d",
			config.MaxPendingBytes, config.WriteBufferHighWatermark)
	}
	if config.TaskPoolMode != taskpool.ModeElastic {
		t.Fatalf("TaskPoolMode = %v, want elastic", config.TaskPoolMode)
	}
	if !config.SharedTaskPool {
		t.Fatal("SharedTaskPool = false, want shared workers by default")
	}
}
