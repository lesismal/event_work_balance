package epoll

import (
	"sync"

	"github.com/lesismal/fib/go/taskpool"
)

type taskPoolKey struct {
	mode      taskpool.Mode
	workers   int
	queueSize int
}

type sharedTaskPoolEntry struct {
	pool *taskpool.TaskPool
	refs int
}

var sharedTaskPools = struct {
	sync.Mutex
	entries map[taskPoolKey]*sharedTaskPoolEntry
}{entries: make(map[taskPoolKey]*sharedTaskPoolEntry)}

func acquireTaskPool(config Config) (*taskpool.TaskPool, func()) {
	if !config.SharedTaskPool {
		pool := taskpool.NewWithMode(config.TaskPoolMode, config.WorkerCount, config.MaxEvents)
		return pool, pool.Stop
	}
	key := taskPoolKey{mode: config.TaskPoolMode, workers: config.WorkerCount, queueSize: config.MaxEvents}
	sharedTaskPools.Lock()
	entry := sharedTaskPools.entries[key]
	if entry == nil {
		entry = &sharedTaskPoolEntry{pool: taskpool.NewWithMode(key.mode, key.workers, key.queueSize)}
		sharedTaskPools.entries[key] = entry
	}
	entry.refs++
	sharedTaskPools.Unlock()
	var once sync.Once
	return entry.pool, func() {
		once.Do(func() {
			sharedTaskPools.Lock()
			entry.refs--
			last := entry.refs == 0
			if last {
				delete(sharedTaskPools.entries, key)
			}
			sharedTaskPools.Unlock()
			if last {
				entry.pool.Stop()
			}
		})
	}
}
