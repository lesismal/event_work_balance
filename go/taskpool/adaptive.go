package taskpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// defaultShrinkInterval is how often an adaptive pool looks for workers it
// did not need. It is long next to the elastic pool's idle linger on purpose:
// a resident worker is kept for its warm stack, and giving one up only to fork
// its replacement a moment later is the churn this pool exists to avoid.
const defaultShrinkInterval = time.Second

// AdaptiveConfig sizes a ModeAdaptive pool.
type AdaptiveConfig struct {
	// MinWorkers is the resident floor: the pool starts this many workers and
	// never retires below it. Zero lets the pool retire every worker while it
	// is idle, and start them again when work arrives.
	MinWorkers int
	// MaxWorkers is the ceiling the pool grows to under load. It must be
	// greater than zero and not below MinWorkers.
	MaxWorkers int
	// QueueSize bounds the tasks waiting for a worker. A submission that finds
	// the queue full waits for room.
	QueueSize int
	// ShrinkInterval is how often the pool retires workers that stayed idle
	// for the whole of the previous interval. Zero means one second.
	ShrinkInterval time.Duration
}

// NewAdaptive creates a ModeAdaptive pool.
//
// Its workers park on a condition variable between tasks, as ModeCond's do,
// but their number follows the load. A task that arrives with no idle worker
// to take it starts a new one, up to MaxWorkers, so a burst is absorbed by
// more workers rather than a longer queue. Once per ShrinkInterval the pool
// looks at the fewest workers that sat idle at any moment of the interval:
// those were never needed, and half of them are retired, never taking the pool
// below MinWorkers. Halving rather than retiring them all lets a pool that
// grew for a burst step back down over a few intervals instead of dropping
// the workers the next burst would want.
func NewAdaptive(config AdaptiveConfig) *TaskPool {
	validateAdaptiveRange(config.MinWorkers, config.MaxWorkers)
	if config.QueueSize < 0 {
		panic("taskpool: queueSize must not be negative")
	}
	executor := &executor{}
	return &TaskPool{executor: executor, backend: newAdaptiveBackend(executor, config)}
}

func validateAdaptiveRange(minWorkers, maxWorkers int) {
	if maxWorkers <= 0 {
		panic("taskpool: maxWorkers must be greater than zero")
	}
	if minWorkers < 0 || minWorkers > maxWorkers {
		panic("taskpool: minWorkers must be between zero and maxWorkers")
	}
}

// defaultMinWorkers is the resident floor NewWithMode gives an adaptive pool:
// ten workers per P, so that every core has warm workers to run on.
func defaultMinWorkers(maxWorkers int) int {
	return min(maxWorkers, 20*runtime.GOMAXPROCS(0))
}

// adaptiveBackend spreads an adaptive pool over shards, for the same reason
// ModeCond is sharded, and runs the one goroutine that shrinks all of them.
type adaptiveBackend struct {
	shards      []*adaptivePool
	next        atomic.Uint32
	workerWG    sync.WaitGroup
	stopJanitor chan struct{}
	janitorDone chan struct{}
	stopOnce    sync.Once
}

func newAdaptiveBackend(executor *executor, config AdaptiveConfig) *adaptiveBackend {
	interval := config.ShrinkInterval
	if interval <= 0 {
		interval = defaultShrinkInterval
	}
	queueSize := config.QueueSize
	shards := shardCount(config.MaxWorkers)
	if queueSize < shards {
		// Every shard needs a slot to queue into.
		queueSize = shards
	}
	b := &adaptiveBackend{stopJanitor: make(chan struct{}), janitorDone: make(chan struct{})}
	for i := 0; i < shards; i++ {
		p := newAdaptivePool(executor, &b.workerWG, share(queueSize, shards, i))
		b.shards = append(b.shards, p)
	}
	b.resize(config.MinWorkers, config.MaxWorkers)
	go b.janitor(interval)
	return b
}

// share is shard i's part of total, with the remainder spread over the
// leading shards so the parts add up to total.
func share(total, shards, i int) int {
	part := total / shards
	if i < total%shards {
		part++
	}
	return part
}

func (b *adaptiveBackend) pick() *adaptivePool {
	return b.shards[int(b.next.Add(1)-1)%len(b.shards)]
}

func (b *adaptiveBackend) submit(task Task) bool { return b.pick().submit(task) }

func (b *adaptiveBackend) submitBatch(tasks []Task) int { return b.pick().submitBatch(tasks) }

func (b *adaptiveBackend) stop() {
	b.stopOnce.Do(func() {
		close(b.stopJanitor)
		<-b.janitorDone
		for _, p := range b.shards {
			p.stop()
		}
		b.workerWG.Wait()
	})
}

func (b *adaptiveBackend) workerCount() int {
	total := 0
	for _, p := range b.shards {
		total += p.workerCount()
	}
	return total
}

// resize splits a new floor and ceiling over the shards. The shard count was
// fixed when the pool was built, so a ceiling below it still leaves every
// shard one worker, which a shard needs to run what lands on it.
func (b *adaptiveBackend) resize(minWorkers, maxWorkers int) {
	for i, p := range b.shards {
		p.resize(share(minWorkers, len(b.shards), i), max(1, share(maxWorkers, len(b.shards), i)))
	}
}

func (b *adaptiveBackend) janitor(interval time.Duration) {
	defer close(b.janitorDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, p := range b.shards {
				p.shrink()
			}
		case <-b.stopJanitor:
			return
		}
	}
}

// adaptivePool is one shard: a bounded ring queue and a population of workers
// parked on a condition variable that grows with the load and shrinks when the
// janitor finds workers it did not need.
type adaptivePool struct {
	executor *executor
	workerWG *sync.WaitGroup
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	queue    []Task
	head     int
	tail     int
	count    int
	// idle counts parked workers no submission has claimed yet, and signals
	// the claims whose wake-up no worker has taken up yet. Counting a
	// signaled worker as idle until it runs would let the next task count on
	// it too, and skip starting the worker that task needed: with tasks that
	// block, it would then wait behind them forever.
	idle        int
	signals     int
	fullWaiters int
	minWorkers  int
	maxWorkers  int
	// workers counts the live workers, including those just started and not
	// yet running.
	workers int
	// retire is how many idle workers the last shrink asked to exit. It is
	// replaced, not added to, by the next one, so a request the load
	// overtook does not linger.
	retire int
	// lowIdle is the fewest workers parked at any moment since the last
	// shrink: the ones that were never needed in that interval.
	lowIdle int
	stopped bool
	pending sync.WaitGroup
}

func newAdaptivePool(executor *executor, workerWG *sync.WaitGroup, queueSize int) *adaptivePool {
	p := &adaptivePool{executor: executor, workerWG: workerWG, queue: make([]Task, max(queueSize, 1))}
	p.notEmpty = sync.NewCond(&p.mu)
	p.notFull = sync.NewCond(&p.mu)
	return p
}

func (p *adaptivePool) enqueueLocked(task Task) {
	p.queue[p.tail] = task
	p.tail++
	if p.tail == len(p.queue) {
		p.tail = 0
	}
	p.count++
}

// spawnLocked reserves a new worker if the ceiling allows one. The caller
// starts it with startWorkers, after releasing the lock where it can, so that
// a burst's forks do not extend the hold time submitters queue behind. The
// WaitGroup is raised here, under the lock, because stop waits on it once the
// queue drains, and the reservation must be counted before then.
func (p *adaptivePool) spawnLocked() bool {
	if p.workers >= p.maxWorkers {
		return false
	}
	p.workers++
	p.workerWG.Add(1)
	return true
}

func (p *adaptivePool) startWorkers(n int) {
	for ; n > 0; n-- {
		go p.worker()
	}
}

// claimLocked hands the task just queued to a parked worker, if one is not
// already spoken for, and reports whether it did. The caller signals after
// releasing the lock.
func (p *adaptivePool) claimLocked() bool {
	if p.idle == 0 {
		return false
	}
	p.idle--
	p.signals++
	p.lowIdle = min(p.lowIdle, p.idle)
	return true
}

func (p *adaptivePool) signal(n int) {
	for ; n > 0; n-- {
		p.notEmpty.Signal()
	}
}

// waitForRoomLocked parks a submitter until the queue has room. A full queue
// with room under the ceiling is a reason to grow, so a worker is started
// before waiting; it runs as soon as the lock is released.
func (p *adaptivePool) waitForRoomLocked() {
	if p.spawnLocked() {
		go p.worker()
	}
	p.fullWaiters++
	p.notFull.Wait()
	p.fullWaiters--
}

func (p *adaptivePool) submit(task Task) bool {
	p.mu.Lock()
	for !p.stopped && p.count == len(p.queue) {
		p.waitForRoomLocked()
	}
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.pending.Add(1)
	p.enqueueLocked(task)
	// An idle worker takes the task if there is one. Otherwise every worker
	// is busy, and the task is load the pool has not grown to meet yet.
	wake, spawn := p.claimLocked(), false
	if !wake {
		spawn = p.spawnLocked()
	}
	p.mu.Unlock()
	if wake {
		p.notEmpty.Signal()
	}
	if spawn {
		go p.worker()
	}
	return true
}

// submitBatch enqueues tasks under one lock acquisition. Each task either
// claims one of the parked workers or, once they are all spoken for, starts a
// new worker while the ceiling allows. It returns how many tasks were
// accepted; a shorter count means the pool stopped mid-batch.
func (p *adaptivePool) submitBatch(tasks []Task) int {
	submitted, wake, spawn := 0, 0, 0
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return 0
	}
	p.pending.Add(len(tasks))
	for _, task := range tasks {
		for !p.stopped && p.count == len(p.queue) {
			// Parking with tasks enqueued that no worker has been told about
			// would be a lost wake-up, so settle the deferred ones first.
			p.signal(wake)
			p.startWorkers(spawn)
			wake, spawn = 0, 0
			p.waitForRoomLocked()
		}
		if p.stopped {
			break
		}
		p.enqueueLocked(task)
		submitted++
		if p.claimLocked() {
			wake++
		} else if p.spawnLocked() {
			spawn++
		}
	}
	if rejected := len(tasks) - submitted; rejected > 0 {
		p.pending.Add(-rejected)
	}
	p.mu.Unlock()
	p.signal(wake)
	p.startWorkers(spawn)
	return submitted
}

func (p *adaptivePool) worker() {
	defer p.workerWG.Done()
	p.mu.Lock()
	for {
		if p.workers > p.maxWorkers {
			// A resize lowered the ceiling. Leave even with work queued: the
			// workers that stay will run it.
			p.exitLocked()
			return
		}
		if p.count > 0 {
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
			p.mu.Lock()
			continue
		}
		if p.stopped {
			p.exitLocked()
			return
		}
		if p.retire > 0 && p.workers > p.minWorkers {
			// Only an idle worker retires: one that finds work takes it, and
			// the retirement falls to whichever worker next finds none.
			p.retire--
			p.exitLocked()
			return
		}
		p.idle++
		p.notEmpty.Wait()
		// Whichever worker wakes takes up an outstanding claim, since any of
		// them will run the task it was made for. One woken with no claim
		// outstanding, by a broadcast or a shrink, was still counted idle.
		if p.signals > 0 {
			p.signals--
		} else {
			p.idle--
			p.lowIdle = min(p.lowIdle, p.idle)
		}
	}
}

// exitLocked retires the calling worker and releases the lock. A worker that
// was woken for a task and leaves without taking it passes the wake-up on, so
// that the task is not left waiting for the next submission.
func (p *adaptivePool) exitLocked() {
	p.workers--
	handoff := p.count > 0 && p.claimLocked()
	p.mu.Unlock()
	if handoff {
		p.notEmpty.Signal()
	}
}

// shrink retires half of the workers that stayed idle through the whole
// interval since the last call, keeping the pool at or above its floor.
func (p *adaptivePool) shrink() {
	p.mu.Lock()
	idle := p.lowIdle
	p.lowIdle = p.idle
	retire := 0
	if surplus := p.workers - p.minWorkers; idle > 0 && surplus > 0 && !p.stopped {
		retire = min((idle+1)/2, surplus)
	}
	p.retire = retire
	wake := min(retire, p.idle)
	p.mu.Unlock()
	p.signal(wake)
}

// resize moves the floor and ceiling. Raising the floor starts the missing
// workers at once; lowering the ceiling wakes the parked workers so that the
// ones over it leave now, while busy ones leave as they finish their task.
func (p *adaptivePool) resize(minWorkers, maxWorkers int) {
	p.mu.Lock()
	p.minWorkers, p.maxWorkers = minWorkers, maxWorkers
	spawn := 0
	for !p.stopped && p.workers < minWorkers && p.spawnLocked() {
		spawn++
	}
	over := p.workers > maxWorkers
	p.mu.Unlock()
	p.startWorkers(spawn)
	if over {
		p.notEmpty.Broadcast()
	}
}

func (p *adaptivePool) workerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers
}

// stop rejects new tasks, waits for the queued ones to run, and then wakes
// every parked worker so that it exits. The backend waits for the workers.
func (p *adaptivePool) stop() {
	p.mu.Lock()
	p.stopped = true
	p.notEmpty.Broadcast()
	p.notFull.Broadcast()
	p.mu.Unlock()
	p.pending.Wait()
	p.mu.Lock()
	p.notEmpty.Broadcast()
	p.mu.Unlock()
}
