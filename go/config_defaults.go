package epoll

import "runtime"

func defaultPoolSizing() (workerCount, maxEvents int) {
	cpuCount := runtime.NumCPU()
	workerCount = cpuCount * 1000
	if cpuCount >= 4 && workerCount < 10000 {
		workerCount = 10000
	}
	maxEvents = cpuCount * 1000
	if maxEvents < 10000 {
		maxEvents = 10000
	} else if maxEvents > 100000 {
		maxEvents = 100000
	}
	return workerCount, maxEvents
}
