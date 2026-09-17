package taskpool

import (
	"sync"
	"sync/atomic"
	"time"
)

// idleLinger is how long a worker stays available after finishing its task
// before giving up its slot. A worker that exits the moment the queue looks
// empty is replaced by a fork on the very next submission, and a forked
// goroutine starts on a fresh stack that has to grow again through the whole
// handler chain: before workers lingered, newstack and copystack were 29% of a
// 100k-connection echo profile. Submissions arrive in epoll-round bursts, so a
// short linger is enough to carry a worker from one burst to the next.
const idleLinger = 50 * time.Millisecond

// elasticPool preserves nbio/taskpool's fork-first scheduling model: new
// submissions create workers while capacity is available, overflow is queued,
// and a dispatcher occupies the final execution slot so work is not stranded.
type elasticPool struct {
	executor   *executor
	maxWorkers int64
	active     atomic.Int64
	tasks      chan Task
	dispatcher chan struct{}
	// idle counts workers parked waiting for a task. Submitting to one of them
	// is what lets the pool reuse a warm stack instead of forking.
	idle     atomic.Int64
	mu       sync.Mutex
	stopped  bool
	stopOnce sync.Once
	taskWG   sync.WaitGroup
	workerWG sync.WaitGroup
}

func newElasticPool(executor *executor, maxConcurrent, queueSize int) *elasticPool {
	p := &elasticPool{
		executor: executor, maxWorkers: int64(maxConcurrent - 1),
		tasks: make(chan Task, queueSize), dispatcher: make(chan struct{}),
	}
	p.workerWG.Add(1)
	go p.dispatch()
	return p
}

func (p *elasticPool) submit(task Task) bool {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.taskWG.Add(1)
	// Hand the task to a worker that is already running before forking a new
	// one. Forking is only worth its fresh stack when there is nobody waiting.
	if p.idle.Load() > 0 {
		select {
		case p.tasks <- task:
			p.mu.Unlock()
			return true
		default:
		}
	}
	if p.fork(task) {
		p.mu.Unlock()
		return true
	}
	p.mu.Unlock()
	p.tasks <- task
	return true
}

func (p *elasticPool) submitBatch(tasks []Task) int {
	for i, task := range tasks {
		if !p.submit(task) {
			return i
		}
	}
	return len(tasks)
}

func (p *elasticPool) stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		p.taskWG.Wait()
		close(p.dispatcher)
		p.workerWG.Wait()
	})
}

func (p *elasticPool) fork(first Task) bool {
	for {
		active := p.active.Load()
		if active >= p.maxWorkers {
			return false
		}
		if p.active.CompareAndSwap(active, active+1) {
			p.workerWG.Add(1)
			go p.run(first)
			return true
		}
	}
}

func (p *elasticPool) run(task Task) {
	defer p.workerWG.Done()
	defer p.active.Add(-1)
	// One timer per worker, reset per wait, rather than a fresh timer per idle
	// spell: this is the hot path the linger exists to keep warm.
	timer := time.NewTimer(idleLinger)
	defer timer.Stop()
	for {
		p.execute(task)
		select {
		case task = <-p.tasks:
			continue
		default:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(idleLinger)
		p.idle.Add(1)
		select {
		case task = <-p.tasks:
			p.idle.Add(-1)
		case <-timer.C:
			p.idle.Add(-1)
			return
		}
	}
}

func (p *elasticPool) dispatch() {
	defer p.workerWG.Done()
	for {
		select {
		case task := <-p.tasks:
			if !p.fork(task) {
				p.execute(task)
			}
		case <-p.dispatcher:
			return
		}
	}
}

func (p *elasticPool) execute(task Task) {
	defer p.taskWG.Done()
	p.executor.call(task)
}
