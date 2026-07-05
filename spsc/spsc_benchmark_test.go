package spsc

import (
	"sync"
	"testing"
)

// BenchmarkSPSCQueue_Separate measures the performance of the custom SPSC queue
// with one producer and one consumer.
func BenchmarkSPSCQueue_Separate(b *testing.B) {
	capacity := uint64(1024 * 1024)
	q := NewSPSCQueue(capacity)

	var wg sync.WaitGroup
	wg.Add(2)

	b.ResetTimer()
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if ok := q.Enqueue(1); !ok {
				break
			}
		}
		q.Close()
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if _, ok := q.Dequeue(); !ok {
				break
			}
		}
	}()

	wg.Wait()
}

// BenchmarkStdChannel_Separate measures the performance of a standard buffered Go channel
// with one producer and one consumer.
func BenchmarkStdChannel_Separate(b *testing.B) {
	capacity := 1024 * 1024
	q := make(chan interface{}, capacity)

	var wg sync.WaitGroup
	wg.Add(2)

	b.ResetTimer()
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			q <- 1
		}
		close(q)
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			_, ok := <-q
			if !ok {
				break
			}
		}
	}()

	wg.Wait()
}
