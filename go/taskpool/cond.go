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
	// waiters and fullWaiters count goroutines parked on notEmpty/notFull.
	// Signaling only when a counter is nonzero keeps the uncontended
	// enqueue/dequeue paths free of runtime notify-list traffic.
	waiters     int
	fullWaiters int
	stopped     bool
	stopOnce    sync.Once
	pending     sync.WaitGroup
	workers     sync.WaitGroup
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

func (p *condPool) enqueueLocked(task Task) {
	p.pending.Add(1)
	p.queue[p.tail] = task
	p.tail++
	if p.tail == len(p.queue) {
		p.tail = 0
	}
	p.count++
}

func (p *condPool) submit(task Task) bool {
	p.mu.Lock()
	for !p.stopped && p.count == len(p.queue) {
		p.fullWaiters++
		p.notFull.Wait()
		p.fullWaiters--
	}
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.enqueueLocked(task)
	if p.waiters > 0 {
		p.notEmpty.Signal()
	}
	p.mu.Unlock()
	return true
}

// submitBatch enqueues tasks under a single lock acquisition, waking at most
// one parked worker per enqueued task. It returns how many tasks were
// accepted; a shorter count means the pool stopped mid-batch and the suffix
// was rejected.
func (p *condPool) submitBatch(tasks []Task) int {
	submitted := 0
	p.mu.Lock()
	// wakeBudget bounds how many parked workers this batch still has to wake.
	// A worker that has been signaled keeps taking tasks on its own, and a
	// worker only parks when the queue is empty, so the tasks after the budget
	// runs out are picked up without a signal. The budget is recomputed after
	// every wait, because workers park again while a full queue blocks us.
	wakeBudget := p.waiters
	for _, task := range tasks {
		for !p.stopped && p.count == len(p.queue) {
			p.fullWaiters++
			p.notFull.Wait()
			p.fullWaiters--
			wakeBudget = p.waiters
		}
		if p.stopped {
			break
		}
		p.enqueueLocked(task)
		submitted++
		if wakeBudget > 0 {
			p.notEmpty.Signal()
			wakeBudget--
		}
	}
	p.mu.Unlock()
	return submitted
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
			p.waiters++
			p.notEmpty.Wait()
			p.waiters--
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
		if p.fullWaiters > 0 {
			p.notFull.Signal()
		}
		p.mu.Unlock()
		p.executor.call(task)
		p.pending.Done()
	}
}
