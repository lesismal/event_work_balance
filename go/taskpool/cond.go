package taskpool

import "sync"

type condPool struct {
	executor *executor
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	queue    []Task
	head     int
	tail     int
	count    int
	stopped  bool
	stopOnce sync.Once
	pending  sync.WaitGroup
	workers  sync.WaitGroup
}

func newCondPool(executor *executor, workerCount, queueSize int) *condPool {
	if queueSize == 0 {
		queueSize = 1
	}
	p := &condPool{executor: executor, queue: make([]Task, queueSize)}
	p.notEmpty = sync.NewCond(&p.mu)
	p.notFull = sync.NewCond(&p.mu)
	p.workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go p.worker()
	}
	return p
}

func (p *condPool) submit(task Task) bool {
	p.mu.Lock()
	for !p.stopped && p.count == len(p.queue) {
		p.notFull.Wait()
	}
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.pending.Add(1)
	p.queue[p.tail] = task
	p.tail++
	if p.tail == len(p.queue) {
		p.tail = 0
	}
	p.count++
	p.notEmpty.Signal()
	p.mu.Unlock()
	return true
}

func (p *condPool) stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.notEmpty.Broadcast()
		p.notFull.Broadcast()
		p.mu.Unlock()
		p.pending.Wait()
		p.mu.Lock()
		p.notEmpty.Broadcast()
		p.mu.Unlock()
		p.workers.Wait()
	})
}

func (p *condPool) worker() {
	defer p.workers.Done()
	for {
		p.mu.Lock()
		for p.count == 0 && !p.stopped {
			p.notEmpty.Wait()
		}
		if p.count == 0 && p.stopped {
			p.mu.Unlock()
			return
		}
		task := p.queue[p.head]
		p.queue[p.head] = nil
		p.head++
		if p.head == len(p.queue) {
			p.head = 0
		}
		p.count--
		p.notFull.Signal()
		p.mu.Unlock()
		p.executor.call(task)
		p.pending.Done()
	}
}
