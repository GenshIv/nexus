package sharded

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func BenchmarkShardedMailbox_MPMC(b *testing.B) {
	numShards := uint64(runtime.GOMAXPROCS(0))
	if numShards < 1 {
		numShards = 1
	}
	q := NewShardedMailbox[int](numShards)

	var idCounter uint64

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddUint64(&idCounter, 1)
		for pb.Next() {
			// Каждая горутина выполняет полный цикл "запрос-ответ",
			// используя свой собственный ID для маршрутизации.
			q.Enqueue(id, 1)
			_ = q.Dequeue(id)
		}
	})
}
