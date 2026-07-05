package sharded

import (
	"runtime"
	"sync"
	"testing"
)

func BenchmarkShardedMailbox_Throughput(b *testing.B) {
	// Теперь конструктор сам определяет оптимальное количество шардов.
	q := NewShardedMailbox[int]()
	defer q.Close()

	numProducers := runtime.GOMAXPROCS(0)
	numConsumers := runtime.GOMAXPROCS(0)

	var wg sync.WaitGroup
	wg.Add(numProducers + numConsumers)

	opsPerProducer := b.N / numProducers
	if opsPerProducer == 0 {
		opsPerProducer = 1
	}
	opsPerConsumer := b.N / numConsumers
	if opsPerConsumer == 0 {
		opsPerConsumer = 1
	}

	b.ResetTimer()

	// --- Продюсеры ---
	for p := 0; p < numProducers; p++ {
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < opsPerProducer; i++ {
				if err := q.Enqueue(uint64(producerID), i); err != nil && err != ErrClosed {
					b.Errorf("Enqueue failed: %v", err)
					return
				}
			}
		}(p)
	}

	// --- Консюмеры ---
	for c := 0; c < numConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()
			for i := 0; i < opsPerConsumer; i++ {
				_, err := q.Dequeue(uint64(consumerID))
				if err != nil && err != ErrClosed {
					b.Errorf("Dequeue failed: %v", err)
					return
				}
			}
		}(c)
	}

	// Ждем, пока все продюсеры и консюмеры не закончат работу.
	wg.Wait()

}
