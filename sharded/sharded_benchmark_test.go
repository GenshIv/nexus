package sharded

import (
	"sync"
	"testing"
	_ "unsafe"
)

//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin()

// BenchmarkShardedQueue_MPMC measures the performance of ShardedQueue
// with multiple producers and multiple consumers.
func BenchmarkShardedQueue_MPMC(b *testing.B) {
	numShards := uint64(2) // Must be a power of 2 (for 4 cpu)
	shardCapacity := uint64(1024)
	q := NewShardedQueue[int](numShards, shardCapacity)

	numConsumers := int(numShards)

	var consumerWg sync.WaitGroup
	consumerWg.Add(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func() {
			defer consumerWg.Done()
			for {
				if _, ok := q.Dequeue(); !ok {
					break
				}
			}
		}()
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		procID := runtime_procPin()
		runtime_procUnpin()

		for pb.Next() {
			q.Enqueue(uint64(procID), 1)
		}
	})

	q.Close()

	consumerWg.Wait()
}
