package epoll

import "runtime"

func defaultPoolSizing() (workerCount, maxEvents int) {
	cpuCount := runtime.GOMAXPROCS(0)
	workerCount = cpuCount
	maxEvents = cpuCount * 256
	if maxEvents < 4096 {
		maxEvents = 4096
	} else if maxEvents > 65536 {
		maxEvents = 65536
	}
	return workerCount, maxEvents
}
