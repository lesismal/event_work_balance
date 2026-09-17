//go:build linux

package epoll

import (
	"runtime"
	"testing"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	// The exact sizing is DefaultPoolSizing's business and is tuned by
	// measurement; restating its arithmetic here would only pin the test to
	// whatever the constants happen to be. What DefaultConfig owes is that it
	// reports that sizing rather than one of its own.
	want := DefaultPoolSizing(config.TaskPoolMode)
	if config.WorkerCount != want.WorkerCount {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, want.WorkerCount)
	}
	if config.MaxEvents != want.MaxEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, want.MaxEvents)
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

// A worker count means a population of goroutines under one mode and a ceiling
// under the other, so the modes cannot share one default.
func TestDefaultPoolSizingDiffersByMode(t *testing.T) {
	cond := DefaultPoolSizing(taskpool.ModeCond)
	elastic := DefaultPoolSizing(taskpool.ModeElastic)
	if cond.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("cond WorkerCount = %d, want more than GOMAXPROCS %d", cond.WorkerCount, runtime.GOMAXPROCS(0))
	}
	// Elastic only forks what the load asks for, so its ceiling is set far
	// above the population cond pays for up front.
	if elastic.WorkerCount <= cond.WorkerCount {
		t.Fatalf("elastic WorkerCount = %d, want more than cond's %d", elastic.WorkerCount, cond.WorkerCount)
	}
	for _, sizing := range []PoolSizing{cond, elastic} {
		if sizing.MaxEvents < minMaxEvents || sizing.MaxEvents > maxMaxEvents {
			t.Fatalf("MaxEvents = %d, want within [%d, %d]", sizing.MaxEvents, minMaxEvents, maxMaxEvents)
		}
	}
}

// Switching the mode has to carry the sizing with it, or a config keeps numbers
// tuned for the mode it no longer runs.
func TestSetTaskPoolModeMovesSizing(t *testing.T) {
	config := DefaultConfig()
	config.SetTaskPoolMode(taskpool.ModeCond)
	want := DefaultPoolSizing(taskpool.ModeCond)
	if config.TaskPoolMode != taskpool.ModeCond {
		t.Fatalf("TaskPoolMode = %v, want cond", config.TaskPoolMode)
	}
	if config.WorkerCount != want.WorkerCount || config.MaxEvents != want.MaxEvents {
		t.Fatalf("sizing = %d/%d, want cond's %d/%d",
			config.WorkerCount, config.MaxEvents, want.WorkerCount, want.MaxEvents)
	}

	config.SetTaskPoolMode(taskpool.ModeElastic)
	want = DefaultPoolSizing(taskpool.ModeElastic)
	if config.WorkerCount != want.WorkerCount || config.MaxEvents != want.MaxEvents {
		t.Fatalf("sizing = %d/%d, want elastic's %d/%d",
			config.WorkerCount, config.MaxEvents, want.WorkerCount, want.MaxEvents)
	}
}

// Sizing the caller chose is theirs to keep, whichever order the two setters
// are called in.
func TestSetPoolSizingSurvivesModeSwitch(t *testing.T) {
	const workers, events = 512, 4096
	for _, order := range []string{"sizing first", "mode first"} {
		t.Run(order, func(t *testing.T) {
			config := DefaultConfig()
			if order == "sizing first" {
				config.SetPoolSizing(workers, events)
				config.SetTaskPoolMode(taskpool.ModeCond)
			} else {
				config.SetTaskPoolMode(taskpool.ModeCond)
				config.SetPoolSizing(workers, events)
			}
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
			// And it still holds across a further switch.
			config.SetTaskPoolMode(taskpool.ModeElastic)
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("after switching modes sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
		})
	}
}

// A field left at zero means "leave it", so that one of the two can be tuned
// without having to restate the other.
func TestSetPoolSizingKeepsFieldsLeftUnset(t *testing.T) {
	config := DefaultConfig()
	events := config.MaxEvents
	config.SetPoolSizing(64, 0)
	if config.WorkerCount != 64 {
		t.Fatalf("WorkerCount = %d, want 64", config.WorkerCount)
	}
	if config.MaxEvents != events {
		t.Fatalf("MaxEvents = %d, want it left at %d", config.MaxEvents, events)
	}
}
