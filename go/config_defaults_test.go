package epoll

import (
	"runtime"
	"testing"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	wantWorkers := runtime.NumCPU() * 1000
	if runtime.NumCPU() >= 4 && wantWorkers < 10000 {
		wantWorkers = 10000
	}
	wantEvents := runtime.NumCPU() * 1000
	if wantEvents < 10000 {
		wantEvents = 10000
	} else if wantEvents > 100000 {
		wantEvents = 100000
	}
	if config.WorkerCount != wantWorkers {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, wantWorkers)
	}
	if config.MaxEvents != wantEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, wantEvents)
	}
}
