// Package taskpool provides interchangeable bounded task schedulers.
package taskpool

import (
	"fmt"
	"runtime/debug"
	"sync"
)

type Mode uint8

const (
	ModeCond Mode = iota
	ModeFixed
	ModeElastic
)

func (m Mode) String() string {
	switch m {
	case ModeFixed:
		return "fixed"
	case ModeElastic:
		return "elastic"
	case ModeCond:
		return "cond"
	default:
		return fmt.Sprintf("Mode(%d)", m)
	}
}

func (m Mode) Valid() bool { return m == ModeFixed || m == ModeElastic || m == ModeCond }

type Task interface{ RunTask() }

type taskFunc func()

func (f taskFunc) RunTask() { f() }

type backend interface {
	submit(Task) bool
	submitBatch([]Task) int
	stop()
}

type executor struct {
	mu      sync.RWMutex
	handler func(any, []byte)
}

func (e *executor) setPanicHandler(handler func(any, []byte)) {
	e.mu.Lock()
	e.handler = handler
	e.mu.Unlock()
}

func (e *executor) call(task Task) {
	defer func() {
		if recovered := recover(); recovered != nil {
			e.mu.RLock()
			handler := e.handler
			e.mu.RUnlock()
			if handler != nil {
				handler(recovered, debug.Stack())
			}
		}
	}()
	task.RunTask()
}

type TaskPool struct {
	executor *executor
	backend  backend
}

func New(maxConcurrent, queueSize int) *TaskPool {
	return NewWithMode(ModeCond, maxConcurrent, queueSize)
}

func NewWithMode(mode Mode, maxConcurrent, queueSize int) *TaskPool {
	if maxConcurrent <= 0 {
		panic("taskpool: maxConcurrent must be greater than zero")
	}
	if queueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	executor := &executor{}
	pool := &TaskPool{executor: executor}
	switch mode {
	case ModeFixed:
		pool.backend = newFixedPool(executor, maxConcurrent, queueSize)
	case ModeElastic:
		pool.backend = newElasticPool(executor, maxConcurrent, queueSize)
	case ModeCond:
		pool.backend = newCondPool(executor, maxConcurrent, queueSize)
	default:
		panic("taskpool: invalid mode")
	}
	return pool
}

func (tp *TaskPool) SetPanicHandler(handler func(any, []byte)) {
	tp.executor.setPanicHandler(handler)
}

func (tp *TaskPool) Go(f func()) bool {
	if f == nil {
		return true
	}
	return tp.GoTask(taskFunc(f))
}

func (tp *TaskPool) GoTask(task Task) bool {
	if task == nil {
		return true
	}
	return tp.backend.submit(task)
}

// GoTasks submits tasks in order and returns how many were accepted. Tasks
// must be non-nil. A short count means the pool stopped; the suffix
// tasks[n:] was not accepted.
func (tp *TaskPool) GoTasks(tasks []Task) int {
	if len(tasks) == 0 {
		return 0
	}
	return tp.backend.submitBatch(tasks)
}

func (tp *TaskPool) Call(f func()) { tp.executor.call(taskFunc(f)) }

func (tp *TaskPool) Stop() { tp.backend.stop() }
