package epoll

import (
	"runtime"
	"testing"

	"github.com/lesismal/auto-balance-epoll/go/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	wantWorkers := runtime.GOMAXPROCS(0)
	wantEvents := wantWorkers * 256
	if wantEvents < 4096 {
		wantEvents = 4096
	} else if wantEvents > 65536 {
		wantEvents = 65536
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
	if config.TaskPoolMode != taskpool.ModeCond {
		t.Fatalf("TaskPoolMode = %v, want cond", config.TaskPoolMode)
	}
}
