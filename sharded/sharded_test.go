package sharded

import (
	"runtime"
	"sync"
	"testing"
)

// BenchmarkStandardChannel_MPMC benchmarks the standard buffered Go channel in an MPMC scenario.
func BenchmarkStandardChannel_MPMC(b *testing.B) {
	// channelCapacity := uint64(1024 * 4)
	ch := make(chan int)

	numProducers := runtime.GOMAXPROCS(0)
	numConsumers := numProducers

	b.ResetTimer()
	b.SetParallelism(numProducers)

	var wg sync.WaitGroup
	wg.Add(numProducers + numConsumers)

	for p := 0; p < numProducers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/numProducers; i++ {
				ch <- i
			}
		}()
	}

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/numConsumers; i++ {
				<-ch
			}
		}()
	}

	wg.Wait()
	close(ch)
}
