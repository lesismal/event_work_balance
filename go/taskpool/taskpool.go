// Package taskpool provides a bounded, elastic goroutine pool.
package taskpool

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// TaskPool follows nbio/taskpool's elastic execution model: submissions fork
// workers while capacity is available, overflow is buffered, and a worker
// drains queued work before retiring. A dispatcher is kept as the final
// execution slot so queued work cannot be stranded between worker exits.
type TaskPool struct {
	maxWorkers int64
	active     atomic.Int64
	tasks      chan func()
	dispatcher chan struct{}

	mu       sync.Mutex
	stopped  bool
	stopOnce sync.Once
	taskWG   sync.WaitGroup
	workerWG sync.WaitGroup

	panicHandler func(any, []byte)
}

// New creates a pool with at most maxConcurrent simultaneous tasks.
func New(maxConcurrent, queueSize int) *TaskPool {
	if maxConcurrent <= 0 {
		panic("taskpool: maxConcurrent must be greater than zero")
	}
	if queueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	tp := &TaskPool{
		maxWorkers: int64(maxConcurrent - 1),
		tasks:      make(chan func(), queueSize),
		dispatcher: make(chan struct{}),
	}
	tp.workerWG.Add(1)
	go tp.dispatch()
	return tp
}

// SetPanicHandler sets an optional handler for task panics.
func (tp *TaskPool) SetPanicHandler(handler func(any, []byte)) {
	tp.mu.Lock()
	tp.panicHandler = handler
	tp.mu.Unlock()
}

// Go schedules f. It returns false after Stop has begun.
func (tp *TaskPool) Go(f func()) bool {
	if f == nil {
		return true
	}
	tp.mu.Lock()
	if tp.stopped {
		tp.mu.Unlock()
		return false
	}
	tp.taskWG.Add(1)
	if tp.fork(f) {
		tp.mu.Unlock()
		return true
	}
	tp.mu.Unlock()
	// The task wait-group was incremented under the stop lock, so Stop cannot
	// miss this accepted task even if queue backpressure blocks this send.
	tp.tasks <- f
	return true
}

// Call runs f synchronously with the pool's panic isolation.
func (tp *TaskPool) Call(f func()) { tp.call(f) }

// Stop rejects new work, drains accepted tasks, and joins pool goroutines.
func (tp *TaskPool) Stop() {
	tp.stopOnce.Do(func() {
		tp.mu.Lock()
		tp.stopped = true
		tp.mu.Unlock()
		tp.taskWG.Wait()
		close(tp.dispatcher)
		tp.workerWG.Wait()
	})
}

func (tp *TaskPool) fork(first func()) bool {
	for {
		active := tp.active.Load()
		if active >= tp.maxWorkers {
			return false
		}
		if tp.active.CompareAndSwap(active, active+1) {
			tp.workerWG.Add(1)
			go tp.run(first)
			return true
		}
	}
}

func (tp *TaskPool) run(task func()) {
	defer tp.workerWG.Done()
	defer tp.active.Add(-1)
	for {
		tp.execute(task)
		select {
		case task = <-tp.tasks:
			continue
		default:
			return
		}
	}
}

func (tp *TaskPool) dispatch() {
	defer tp.workerWG.Done()
	for {
		select {
		case task := <-tp.tasks:
			if !tp.fork(task) {
				tp.execute(task)
			}
		case <-tp.dispatcher:
			return
		}
	}
}

func (tp *TaskPool) execute(f func()) {
	defer tp.taskWG.Done()
	tp.call(f)
}

func (tp *TaskPool) call(f func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			tp.mu.Lock()
			handler := tp.panicHandler
			tp.mu.Unlock()
			if handler != nil {
				handler(recovered, debug.Stack())
			}
		}
	}()
	f()
}
