// Package taskpool provides a bounded goroutine pool.
package taskpool

import (
	"runtime/debug"
	"sync"
)

// Task avoids allocating a closure or method value when a reusable object is
// submitted repeatedly.
type Task interface {
	RunTask()
}

type taskFunc func()

func (f taskFunc) RunTask() { f() }

// TaskPool keeps a fixed set of workers alive. Network readiness events are
// frequent and short-lived; retiring a worker whenever its queue is briefly
// empty causes goroutine creation and stack growth to dominate the workload.
type TaskPool struct {
	tasks chan Task
	stop  chan struct{}

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
		tasks: make(chan Task, queueSize),
		stop:  make(chan struct{}),
	}
	tp.workerWG.Add(maxConcurrent)
	for i := 0; i < maxConcurrent; i++ {
		go tp.worker()
	}
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
	return tp.GoTask(taskFunc(f))
}

// GoTask schedules a reusable task without creating a closure.
func (tp *TaskPool) GoTask(task Task) bool {
	if task == nil {
		return true
	}
	tp.mu.Lock()
	if tp.stopped {
		tp.mu.Unlock()
		return false
	}
	tp.taskWG.Add(1)
	tp.mu.Unlock()
	// The task wait-group was incremented under the stop lock, so Stop cannot
	// miss this accepted task even if queue backpressure blocks this send.
	tp.tasks <- task
	return true
}

// Call runs f synchronously with the pool's panic isolation.
func (tp *TaskPool) Call(f func()) { tp.call(taskFunc(f)) }

// Stop rejects new work, drains accepted tasks, and joins pool goroutines.
func (tp *TaskPool) Stop() {
	tp.stopOnce.Do(func() {
		tp.mu.Lock()
		tp.stopped = true
		tp.mu.Unlock()
		tp.taskWG.Wait()
		close(tp.stop)
		tp.workerWG.Wait()
	})
}

func (tp *TaskPool) worker() {
	defer tp.workerWG.Done()
	for {
		select {
		case task := <-tp.tasks:
			tp.execute(task)
		case <-tp.stop:
			return
		}
	}
}

func (tp *TaskPool) execute(task Task) {
	defer tp.taskWG.Done()
	tp.call(task)
}

func (tp *TaskPool) call(task Task) {
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
	task.RunTask()
}
