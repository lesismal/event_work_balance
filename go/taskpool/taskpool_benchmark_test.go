package taskpool

import "testing"

type benchmarkTask struct{ done chan struct{} }

func (t *benchmarkTask) RunTask() { t.done <- struct{}{} }

func BenchmarkGoTask(b *testing.B) {
	for _, mode := range []Mode{ModeElastic, ModeCond} {
		b.Run(mode.String(), func(b *testing.B) {
			tp := NewWithMode(mode, 1, 1)
			defer tp.Stop()
			task := &benchmarkTask{done: make(chan struct{}, 1)}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !tp.GoTask(task) {
					b.Fatal("task rejected")
				}
				<-task.done
			}
		})
	}
}
