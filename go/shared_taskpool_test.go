package fib

import (
	"testing"

	"github.com/lesismal/fib/go/taskpool"
)

func TestSharedTaskPoolReferenceLifecycle(t *testing.T) {
	config := Config{WorkerCount: 1, MaxEvents: 4, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
	first, releaseFirst := acquireTaskPool(config)
	second, releaseSecond := acquireTaskPool(config)
	if first != second {
		t.Fatal("identical configurations did not share a task pool")
	}
	releaseFirst()
	done := make(chan struct{}, 1)
	if !second.(*taskpool.TaskPool).Go(func() { done <- struct{}{} }) {
		t.Fatal("shared pool stopped before its final owner released it")
	}
	<-done
	releaseSecond()
	sharedTaskPools.Lock()
	remaining := len(sharedTaskPools.entries)
	sharedTaskPools.Unlock()
	if remaining != 0 {
		t.Fatalf("shared pool entries remaining = %d", remaining)
	}
}

// An adaptive pool starts at the floor MinWorkerCount sets, and pools that
// differ only in that floor are not shared, since sharing would hand one of
// them the other's floor.
func TestAdaptiveTaskPoolHonoursMinWorkerCount(t *testing.T) {
	config := Config{WorkerCount: 64, MinWorkerCount: 3, MaxEvents: 64,
		TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true}
	pool, release := acquireTaskPool(config)
	defer release()
	if got := pool.(*taskpool.TaskPool).Workers(); got != 3 {
		t.Fatalf("Workers() = %d, want the floor of 3", got)
	}
	config.MinWorkerCount = 5
	other, releaseOther := acquireTaskPool(config)
	defer releaseOther()
	if other == pool {
		t.Fatal("pools with different floors were shared")
	}
}
