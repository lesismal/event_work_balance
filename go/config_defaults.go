package epoll

import "runtime"

// workersPerCPU oversubscribes the task pool relative to the cores it runs on.
// A worker drives its connection's read and write syscalls inline, so it spends
// much of a round inside the kernel rather than on a P. Sizing the pool to
// GOMAXPROCS therefore leaves cores idle whenever workers are in flight: a
// 100k-connection echo run measured 191% CPU against the 500% its cpuset
// allowed. Oversubscribing keeps a runnable worker behind every core without
// making the queue so wide that latency collapses into it.
const workersPerCPU = 1000

func defaultPoolSizing() (workerCount, maxEvents int) {
	cpuCount := runtime.GOMAXPROCS(0)
	workerCount = cpuCount * workersPerCPU
	if workerCount < 10000 {
		workerCount = 10000
	}
	maxEvents = workerCount * 10
	if maxEvents < 10000 {
		maxEvents = 10000
	} else if maxEvents > 10000*10 {
		maxEvents = 10000 * 10
	}
	return workerCount, maxEvents
}
