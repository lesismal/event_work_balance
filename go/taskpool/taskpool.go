// Package taskpool provides interchangeable bounded task schedulers.
package taskpool

import (
	"fmt"
	"runtime/debug"
	"sync"
)

type Mode uint8

const (
	// ModeCond runs a fixed set of workers that park on a condition variable
	// between tasks. The worker count is the number of goroutines that exist.
	ModeCond Mode = iota
	// ModeElastic forks a worker per submission while it has capacity, lets
	// idle ones linger briefly, and retires them after that. The worker count
	// is a ceiling rather than a population.
	ModeElastic
	// ModeAdaptive parks its workers on a condition variable as ModeCond does,
	// but grows the population when tasks arrive with no idle worker to take
	// them and retires workers that stay idle, between a floor and a ceiling
	// that Resize can move while the pool runs. See NewAdaptive.
	ModeAdaptive
)

func (m Mode) String() string {
	switch m {
	case ModeElastic:
		return "elastic"
	case ModeCond:
		return "cond"
	case ModeAdaptive:
		return "adaptive"
	default:
		return fmt.Sprintf("Mode(%d)", m)
	}
}

func (m Mode) Valid() bool { return m == ModeElastic || m == ModeCond || m == ModeAdaptive }

type Task interface{ RunTask() }

type taskFunc func()

func (f taskFunc) RunTask() { f() }

type backend interface {
	submit(Task) bool
	submitBatch([]Task) int
	stop()
	workerCount() int
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

// New creates a ModeAdaptive pool that grows to maxConcurrent workers under
// load and retires down to ten workers per P when idle.
func New(maxConcurrent, queueSize int) *TaskPool {
	return NewWithMode(ModeAdaptive, maxConcurrent, queueSize)
}

// NewWithMode creates a pool of the given mode. For ModeAdaptive,
// maxConcurrent is the ceiling and the floor is ten workers per P; NewAdaptive
// sets both.
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
	case ModeElastic:
		pool.backend = newElasticPool(executor, maxConcurrent, queueSize)
	case ModeCond:
		pool.backend = newCondBackend(executor, maxConcurrent, queueSize)
	case ModeAdaptive:
		pool.backend = newAdaptiveBackend(executor, AdaptiveConfig{
			MinWorkers: defaultMinWorkers(maxConcurrent), MaxWorkers: maxConcurrent, QueueSize: queueSize,
		})
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

// Workers reports how many workers the pool is running: the fixed count under
// ModeCond, the forked workers under ModeElastic, and the current population
// under ModeAdaptive.
func (tp *TaskPool) Workers() int { return tp.backend.workerCount() }

// Resize moves a ModeAdaptive pool's floor and ceiling while it runs. Raising
// the floor starts workers at once, and lowering the ceiling retires the
// workers over it: idle ones at once, busy ones as they finish their task.
// It reports false, and changes nothing, for a pool of any other mode. It
// panics if maxWorkers is not positive or minWorkers is not between zero and
// maxWorkers.
func (tp *TaskPool) Resize(minWorkers, maxWorkers int) bool {
	adaptive, ok := tp.backend.(*adaptiveBackend)
	if !ok {
		return false
	}
	validateAdaptiveRange(minWorkers, maxWorkers)
	adaptive.resize(minWorkers, maxWorkers)
	return true
}
