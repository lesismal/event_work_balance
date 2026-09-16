package epoll

import (
	"runtime"
	"testing"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	wantWorkers := runtime.GOMAXPROCS(0)
	wantEvents := wantWorkers * 64
	if wantEvents < 1024 {
		wantEvents = 1024
	} else if wantEvents > 16384 {
		wantEvents = 16384
	}
	if config.WorkerCount != wantWorkers {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, wantWorkers)
	}
	if config.MaxEvents != wantEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, wantEvents)
	}
	if !config.UseWritev {
		t.Fatal("UseWritev = false, want adaptive writev enabled by default")
	}
	if config.WriteBufferHighWatermark != 4*1024 {
		t.Fatalf("WriteBufferHighWatermark = %d, want 4096", config.WriteBufferHighWatermark)
	}
	if config.TaskPoolMode != taskpool.ModeCond {
		t.Fatalf("TaskPoolMode = %v, want cond", config.TaskPoolMode)
	}
	if !config.SharedTaskPool {
		t.Fatal("SharedTaskPool = false, want shared workers by default")
	}
}
