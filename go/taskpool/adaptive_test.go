package taskpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blocker is a set of tasks that hold their worker until released, recording
// how many ran at once.
type blocker struct {
	release chan struct{}
	opened  sync.Once
	running atomic.Int64
	peak    atomic.Int64
	done    sync.WaitGroup
}

func newBlocker() *blocker { return &blocker{release: make(chan struct{})} }

// open releases the tasks. Tests also defer it, so that a failure does not
// leave the pool's Stop waiting on tasks that can never finish.
func (b *blocker) open() { b.opened.Do(func() { close(b.release) }) }

func (b *blocker) task() func() {
	b.done.Add(1)
	return func() {
		defer b.done.Done()
		current := b.running.Add(1)
		for old := b.peak.Load(); current > old && !b.peak.CompareAndSwap(old, current); old = b.peak.Load() {
		}
		<-b.release
		b.running.Add(-1)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAdaptiveStartsAtFloor(t *testing.T) {
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: 5, MaxWorkers: 64, QueueSize: 64})
	defer tp.Stop()
	if got := tp.Workers(); got != 5 {
		t.Fatalf("Workers() = %d right after NewAdaptive, want the floor of 5", got)
	}
}

// Tasks that arrive while every worker is busy start new workers, so they run
// alongside the others instead of waiting behind them, up to the ceiling and
// never past it.
func TestAdaptiveGrowsToCeilingAndNoFurther(t *testing.T) {
	const ceiling = 16
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: 1, MaxWorkers: ceiling, QueueSize: 64})
	defer tp.Stop()
	b := newBlocker()
	defer b.open()
	for i := 0; i < 3*ceiling; i++ {
		if !tp.Go(b.task()) {
			t.Fatal("task rejected before Stop")
		}
	}
	waitFor(t, "the pool to reach its ceiling", func() bool { return b.running.Load() == ceiling })
	if got := tp.Workers(); got != ceiling {
		t.Fatalf("Workers() = %d under load, want %d", got, ceiling)
	}
	b.open()
	b.done.Wait()
	if got := b.peak.Load(); got != ceiling {
		t.Fatalf("peak concurrency = %d, want %d", got, ceiling)
	}
}

// Workers the load no longer needs are retired, a half at a time, until the
// pool is back at its floor, and never below it.
func TestAdaptiveShrinksBackToFloor(t *testing.T) {
	const floor, ceiling = 2, 32
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: floor, MaxWorkers: ceiling, QueueSize: 64,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()
	b := newBlocker()
	defer b.open()
	for i := 0; i < ceiling; i++ {
		tp.Go(b.task())
	}
	waitFor(t, "the burst to run", func() bool { return b.running.Load() == ceiling })
	b.open()
	b.done.Wait()
	waitFor(t, "the pool to shrink to its floor", func() bool { return tp.Workers() == floor })
	time.Sleep(50 * time.Millisecond)
	if got := tp.Workers(); got != floor {
		t.Fatalf("Workers() = %d after idling at the floor, want %d", got, floor)
	}
}

// A floor of zero lets an idle pool retire every worker, and work arriving
// afterwards starts them again.
func TestAdaptiveZeroFloorRestartsWorkers(t *testing.T) {
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: 0, MaxWorkers: 4, QueueSize: 16,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()
	var ran atomic.Int64
	for i := 0; i < 8; i++ {
		tp.Go(func() { ran.Add(1) })
	}
	waitFor(t, "the first tasks to run", func() bool { return ran.Load() == 8 })
	waitFor(t, "every worker to retire", func() bool { return tp.Workers() == 0 })
	done := make(chan struct{})
	tp.Go(func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a task submitted to a pool with no workers never ran")
	}
}

func TestAdaptiveResize(t *testing.T) {
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: 1, MaxWorkers: 8, QueueSize: 64,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()

	// Raising the floor starts workers at once.
	if !tp.Resize(6, 8) {
		t.Fatal("Resize refused on an adaptive pool")
	}
	if got := tp.Workers(); got != 6 {
		t.Fatalf("Workers() = %d after raising the floor to 6", got)
	}

	// Raising the ceiling lets the load grow the pool past the old one.
	tp.Resize(1, 24)
	b := newBlocker()
	defer b.open()
	for i := 0; i < 24; i++ {
		tp.Go(b.task())
	}
	waitFor(t, "the pool to grow to the new ceiling", func() bool { return b.running.Load() == 24 })

	// Lowering the ceiling under load retires the busy workers over it as
	// they finish, and the queued tasks then run within the new ceiling.
	tp.Resize(1, 4)
	b2 := newBlocker()
	defer b2.open()
	for i := 0; i < 16; i++ {
		tp.Go(b2.task())
	}
	b.open()
	b.done.Wait()
	waitFor(t, "the pool to drop to the lowered ceiling", func() bool { return tp.Workers() <= 4 })
	waitFor(t, "queued tasks to start", func() bool { return b2.running.Load() == 4 })
	b2.open()
	b2.done.Wait()
	if got := b2.peak.Load(); got > 4 {
		t.Fatalf("peak concurrency after lowering the ceiling = %d, want at most 4", got)
	}
}

func TestResizeRefusedByOtherModes(t *testing.T) {
	for _, mode := range []Mode{ModeCond, ModeElastic} {
		tp := NewWithMode(mode, 4, 4)
		if tp.Resize(1, 8) {
			t.Errorf("%v pool accepted Resize", mode)
		}
		tp.Stop()
	}
}

func TestAdaptiveRejectsBadRange(t *testing.T) {
	for _, r := range [][2]int{{-1, 4}, {5, 4}, {0, 0}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("range %v accepted", r)
				}
			}()
			NewAdaptive(AdaptiveConfig{MinWorkers: r[0], MaxWorkers: r[1], QueueSize: 4}).Stop()
		}()
	}
}

// Growth and retirement race submissions constantly under a short interval.
// Every task must still run exactly once, and Stop must drain them.
func TestAdaptiveEachTaskRunsOnceWhileResizing(t *testing.T) {
	tp := NewAdaptive(AdaptiveConfig{MinWorkers: 0, MaxWorkers: 64, QueueSize: 32,
		ShrinkInterval: time.Millisecond})
	const producers, perProducer = 8, 2000
	var counts [producers * perProducer]atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			batch := make([]Task, 0, 16)
			for i := 0; i < perProducer; i++ {
				index := base + i
				task := taskFunc(func() { counts[index].Add(1) })
				if i%3 == 0 {
					if !tp.GoTask(task) {
						t.Errorf("task %d rejected", index)
					}
					continue
				}
				batch = append(batch, task)
				if len(batch) == cap(batch) {
					if n := tp.GoTasks(batch); n != len(batch) {
						t.Errorf("batch accepted %d of %d", n, len(batch))
					}
					batch = batch[:0]
				}
				if i%500 == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
			if n := tp.GoTasks(batch); n != len(batch) {
				t.Errorf("batch accepted %d of %d", n, len(batch))
			}
		}(p * perProducer)
	}
	stopResizing := make(chan struct{})
	resized := make(chan struct{})
	go func() {
		defer close(resized)
		for i := 0; ; i++ {
			select {
			case <-stopResizing:
				return
			default:
			}
			tp.Resize(i%4, 8+i%57)
			time.Sleep(500 * time.Microsecond)
		}
	}()
	wg.Wait()
	close(stopResizing)
	<-resized
	tp.Stop()
	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d ran %d times", i, got)
		}
	}
	if got := tp.Workers(); got != 0 {
		t.Fatalf("Workers() = %d after Stop", got)
	}
}
