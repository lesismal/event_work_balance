package taskpool

import "testing"

type benchmarkTask struct{ done chan struct{} }

func (t *benchmarkTask) RunTask() { t.done <- struct{}{} }

func BenchmarkGoTask(b *testing.B) {
	tp := New(1, 1)
	defer tp.Stop()
	task := &benchmarkTask{done: make(chan struct{}, 1)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !tp.GoTask(task) {
			b.Fatal("task rejected")
		}
		<-task.done
	}
}
