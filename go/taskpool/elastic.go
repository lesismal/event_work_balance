package taskpool

import (
	"sync"
	"sync/atomic"
)

// elasticPool preserves nbio/taskpool's fork-first scheduling model: new
// submissions create workers while capacity is available, overflow is queued,
// and a dispatcher occupies the final execution slot so work is not stranded.
type elasticPool struct {
	executor   *executor
	maxWorkers int64
	active     atomic.Int64
	tasks      chan Task
	dispatcher chan struct{}
	mu         sync.Mutex
	stopped    bool
	stopOnce   sync.Once
	taskWG     sync.WaitGroup
	workerWG   sync.WaitGroup
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
	for {
		p.execute(task)
		select {
		case task = <-p.tasks:
		default:
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
