package taskpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrencyBoundAndDrain(t *testing.T) {
	const limit = 4
	tp := New(limit, 32)
	release := make(chan struct{})
	var running atomic.Int64
	var peak atomic.Int64
	for i := 0; i < 32; i++ {
		if !tp.Go(func() {
			current := running.Add(1)
			for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
			}
			<-release
			running.Add(-1)
		}) {
			t.Fatal("submission rejected before Stop")
		}
	}
	deadline := time.Now().Add(time.Second)
	for peak.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(release)
	tp.Stop()
	if got := peak.Load(); got > limit || got < 2 {
		t.Fatalf("peak concurrency = %d, want 2..%d", got, limit)
	}
	if tp.Go(func() {}) {
		t.Fatal("submission accepted after Stop")
	}
}

func TestStopWaitsAndRecoversPanics(t *testing.T) {
	tp := New(2, 2)
	started := make(chan struct{})
	finish := make(chan struct{})
	var panicSeen atomic.Bool
	tp.SetPanicHandler(func(any, []byte) { panicSeen.Store(true) })
	tp.Go(func() { panic("boom") })
	tp.Go(func() { close(started); <-finish })
	<-started
	done := make(chan struct{})
	go func() { tp.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("Stop returned while a task was running")
	case <-time.After(10 * time.Millisecond):
	}
	close(finish)
	<-done
	if !panicSeen.Load() {
		t.Fatal("panic handler was not called")
	}
}

func TestEachTaskRunsOnce(t *testing.T) {
	tp := New(8, 64)
	var counts [100]atomic.Int64
	var submitted sync.WaitGroup
	for i := range counts {
		submitted.Add(1)
		go func(index int) {
			defer submitted.Done()
			if !tp.Go(func() { counts[index].Add(1) }) {
				t.Errorf("task %d rejected", index)
			}
		}(i)
	}
	submitted.Wait()
	tp.Stop()
	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d ran %d times", i, got)
		}
	}
}
