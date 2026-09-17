package taskpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// shardedWorkers is a worker count large enough that newCondBackend shards,
// whatever GOMAXPROCS the test runs under.
const shardedWorkers = workersPerShard * 4

func TestShardCountStaysWithinBounds(t *testing.T) {
	limit := runtime.GOMAXPROCS(0)
	for _, workers := range []int{1, workersPerShard - 1, workersPerShard, 1000} {
		got := shardCount(workers)
		if got < 1 {
			t.Fatalf("shardCount(%d) = %d, want at least 1", workers, got)
		}
		if got > limit {
			t.Fatalf("shardCount(%d) = %d, want at most GOMAXPROCS %d", workers, got, limit)
		}
		if got > workers {
			t.Fatalf("shardCount(%d) = %d, want no more shards than workers", workers, got)
		}
	}
	if shardCount(workersPerShard-1) != 1 {
		t.Fatal("a pool smaller than one shard should not be sharded")
	}
}

// A sharded pool must still honour the worker budget it was given: the shards
// divide those workers rather than each getting the full count.
func TestShardedPoolRespectsTotalWorkerLimit(t *testing.T) {
	if shardCount(shardedWorkers) < 2 {
		t.Skip("GOMAXPROCS too low to shard")
	}
	tp := NewWithMode(ModeCond, shardedWorkers, shardedWorkers*4)
	defer tp.Stop()
	if _, ok := tp.backend.(*shardedPool); !ok {
		t.Fatalf("backend = %T, want a sharded pool", tp.backend)
	}

	release := make(chan struct{})
	var running, peak atomic.Int64
	var started sync.WaitGroup
	// Submit more than the worker budget so that, if any shard ran more
	// workers than its share, the peak would exceed the total.
	const tasks = shardedWorkers * 4
	started.Add(tasks)
	for i := 0; i < tasks; i++ {
		if !tp.Go(func() {
			current := running.Add(1)
			for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
			}
			started.Done()
			<-release
			running.Add(-1)
		}) {
			t.Fatal("submission rejected before Stop")
		}
	}
	close(release)
	started.Wait()
	if got := peak.Load(); got > shardedWorkers {
		t.Fatalf("peak concurrency = %d, want at most %d", got, shardedWorkers)
	}
}

// Round-robin submission has to reach every shard, otherwise sharding would
// concentrate the load on one lock rather than spreading it.
func TestShardedPoolSpreadsAcrossShards(t *testing.T) {
	shards := shardCount(shardedWorkers)
	if shards < 2 {
		t.Skip("GOMAXPROCS too low to shard")
	}
	pool := newCondBackend(&executor{}, shardedWorkers, shardedWorkers*4).(*shardedPool)
	defer pool.stop()

	seen := make(map[backend]bool, shards)
	for i := 0; i < shards*3; i++ {
		seen[pool.pick()] = true
	}
	if len(seen) != shards {
		t.Fatalf("round-robin reached %d of %d shards", len(seen), shards)
	}
}

// Every task must run exactly once no matter which shard it landed on, and a
// stopped pool must reject further work on all of them.
func TestShardedPoolRunsEveryTaskOnce(t *testing.T) {
	if shardCount(shardedWorkers) < 2 {
		t.Skip("GOMAXPROCS too low to shard")
	}
	// A queue far smaller than the work forces the queue-full wait path on
	// individual shards while other shards are still draining.
	tp := NewWithMode(ModeCond, shardedWorkers, shardedWorkers)
	var counts [2000]atomic.Int64
	tasks := make([]Task, len(counts))
	for i := range tasks {
		index := i
		tasks[i] = taskFunc(func() { counts[index].Add(1) })
	}
	// Submit in batches so several shards are fed.
	for start := 0; start < len(tasks); start += 64 {
		end := min(start+64, len(tasks))
		if got := tp.GoTasks(tasks[start:end]); got != end-start {
			t.Fatalf("GoTasks accepted %d of %d tasks", got, end-start)
		}
	}
	tp.Stop()
	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d ran %d times, want 1", i, got)
		}
	}
	if got := tp.GoTasks(tasks); got != 0 {
		t.Fatalf("GoTasks accepted %d tasks after Stop, want 0", got)
	}
}
