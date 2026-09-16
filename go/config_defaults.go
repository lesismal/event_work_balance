package epoll

import "runtime"

func defaultPoolSizing() (workerCount, maxEvents int) {
	cpuCount := runtime.GOMAXPROCS(0)
	workerCount = cpuCount
	maxEvents = cpuCount * 64
	if maxEvents < 1024 {
		maxEvents = 1024
	} else if maxEvents > 16384 {
		maxEvents = 16384
	}
	return workerCount, maxEvents
}
