package taskpool

import "sync"

type fixedPool struct {
	executor *executor
	tasks    chan Task
	stopCh   chan struct{}
	mu       sync.Mutex
	stopped  bool
	stopOnce sync.Once
	taskWG   sync.WaitGroup
	workerWG sync.WaitGroup
}

func newFixedPool(executor *executor, workers, queueSize int) *fixedPool {
	p := &fixedPool{executor: executor, tasks: make(chan Task, queueSize), stopCh: make(chan struct{})}
	p.workerWG.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *fixedPool) submit(task Task) bool {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.taskWG.Add(1)
	p.mu.Unlock()
	p.tasks <- task
	return true
}

func (p *fixedPool) stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()
		p.taskWG.Wait()
		close(p.stopCh)
		p.workerWG.Wait()
	})
}

func (p *fixedPool) worker() {
	defer p.workerWG.Done()
	for {
		select {
		case task := <-p.tasks:
			p.executor.call(task)
			p.taskWG.Done()
		case <-p.stopCh:
			return
		}
	}
}
