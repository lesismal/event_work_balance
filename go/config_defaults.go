package epoll

import "runtime"

// workersPerCPU oversubscribes the task pool relative to the cores it runs on.
// A worker drives its connection's read and write syscalls inline, so it spends
// much of a round inside the kernel rather than on a P. Sizing the pool to
// GOMAXPROCS therefore leaves cores idle whenever workers are in flight.
//
// The oversubscription is bounded, though, and the earlier factor of 1000 was
// far past the point where it helps. A 100k-connection echo run measured, on a
// five-core cpuset: 255k echoes/s with 16 workers, 319k with 64, 331k with 250,
// 350k with 500, and then back down to 340k with 2000 and 314k with 5000.
// Beyond a few hundred the pool only adds goroutines for the scheduler to move
// between the same cores.
//
// Note that a pool sized like this needs a GOMAXPROCS above the core count to
// pay off, because its workers hold their P while they are in a syscall. See
// the note on GOMAXPROCS in README.zh-CN.md: the same run went from 330k to
// 415k echoes/s, and from 55k to 71k accepted connections/s, on GOMAXPROCS
// alone.
const workersPerCPU = 100

func defaultPoolSizing() (workerCount, maxEvents int) {
	cpuCount := runtime.GOMAXPROCS(0)
	workerCount = cpuCount * workersPerCPU
	if workerCount < 256 {
		workerCount = 256
	}
	maxEvents = workerCount * 10
	if maxEvents < 10000 {
		maxEvents = 10000
	} else if maxEvents > 10000*10 {
		maxEvents = 10000 * 10
	}
	return workerCount, maxEvents
}
