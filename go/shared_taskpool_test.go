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
	if !second.Go(func() { done <- struct{}{} }) {
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
