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
	p.queue[p.tail] = task
	p.tail++
	if p.tail == len(p.queue) {
		p.tail = 0
	}
	p.count++
}

// signal wakes n parked workers. It runs after the queue lock is released: a
// worker woken while the submitter still holds the lock only gets as far as
// that lock, so signaling under it turns every handoff into two acquisitions
// and leaves the submitter, which is an event loop, holding the lock through
// the whole wake-up.
func (p *condPool) signal(n int) {
	for ; n > 0; n-- {
		p.notEmpty.Signal()
	}
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
	p.pending.Add(1)
	p.enqueueLocked(task)
	wake := p.waiters > 0
	p.mu.Unlock()
	if wake {
		p.notEmpty.Signal()
	}
	return true
}

// submitBatch enqueues tasks under a single lock acquisition, waking at most
// one parked worker per enqueued task. It returns how many tasks were
// accepted; a shorter count means the pool stopped mid-batch and the suffix
// was rejected.
func (p *condPool) submitBatch(tasks []Task) int {
	submitted := 0
	wake := 0
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return 0
	}
	// wakeBudget bounds how many parked workers this batch still has to wake.
	// A worker that has been signaled keeps taking tasks on its own, and a
	// worker only parks when the queue is empty, so the tasks after the budget
	// runs out are picked up without a signal. The budget is recomputed after
	// every wait, because workers park again while a full queue blocks us.
	wakeBudget := p.waiters
	// One Add for the whole batch rather than one per task: the counter is
	// shared by every producer, and inside the queue lock its cache line
	// bounces extend the hold time that the wake-ups queue behind. It may only
	// be raised once this batch has seen !stopped under the lock, because
	// stop() sets that flag under the same lock before waiting on the counter.
	p.pending.Add(len(tasks))
	for _, task := range tasks {
		for !p.stopped && p.count == len(p.queue) {
			// Deferring the wake-ups is only safe while this batch keeps
			// running: parking with tasks enqueued that no worker has been
			// told about is a lost wake-up.
			p.signal(wake)
			wake = 0
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
			wake++
			wakeBudget--
		}
	}
	if rejected := len(tasks) - submitted; rejected > 0 {
		p.pending.Add(-rejected)
	}
	p.mu.Unlock()
	p.signal(wake)
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
		wakeFull := p.fullWaiters > 0
		p.mu.Unlock()
		if wakeFull {
			p.notFull.Signal()
		}
		p.executor.call(task)
		p.pending.Done()
	}
}
