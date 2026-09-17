package taskpool

import (
	"runtime"
	"sync/atomic"
)

// workersPerShard is how many workers share one queue and one lock. A shard
// wants enough workers that a batch landing on it has somewhere to go, and few
// enough that they are not all contending for the same mutex.
const workersPerShard = 8

// shardedPool spreads submissions across several independent backends.
//
// One cond-based pool funnels every producer and every worker through a single
// mutex. That is fine while the pool is small, but the pool has to be
// oversubscribed relative to the cores to keep them busy, and by then the lock
// is the limit: every submitting event loop and every waking worker queues
// behind it. Giving each shard its own queue, lock and share of the workers
// leaves a producer contending only with that shard's workers.
//
// Batches go to shards round-robin. A whole batch lands on one shard, so the
// balance comes from there being far more batches than shards rather than from
// splitting any single one.
type shardedPool struct {
	shards []backend
	next   atomic.Uint32
}

func (p *shardedPool) pick() backend {
	return p.shards[int(p.next.Add(1)-1)%len(p.shards)]
}

func (p *shardedPool) submit(task Task) bool { return p.pick().submit(task) }

func (p *shardedPool) submitBatch(tasks []Task) int { return p.pick().submitBatch(tasks) }

func (p *shardedPool) stop() {
	for _, shard := range p.shards {
		shard.stop()
	}
}

// shardCount picks how many shards workerCount workers should be split over.
// It never exceeds GOMAXPROCS, since shards past that point would contend for
// cores rather than relieve contention.
func shardCount(workerCount int) int {
	shards := workerCount / workersPerShard
	if limit := runtime.GOMAXPROCS(0); shards > limit {
		shards = limit
	}
	if shards < 1 {
		shards = 1
	}
	return shards
}

// newCondBackend builds the cond-mode backend, sharding it when there are
// enough workers to make that worthwhile. A pool small enough to fit in one
// shard skips the indirection entirely.
func newCondBackend(executor *executor, workerCount, queueSize int) backend {
	shards := shardCount(workerCount)
	if shards <= 1 {
		return newCondPool(executor, workerCount, queueSize)
	}
	pool := &shardedPool{shards: make([]backend, 0, shards)}
	for i := 0; i < shards; i++ {
		// Spread the remainder over the leading shards so the worker total is
		// exactly what the caller asked for.
		workers := workerCount / shards
		if i < workerCount%shards {
			workers++
		}
		queue := queueSize / shards
		if i < queueSize%shards {
			queue++
		}
		pool.shards = append(pool.shards, newCondPool(executor, workers, queue))
	}
	return pool
}
