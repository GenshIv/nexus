package sharded

import (
	"runtime"
	"sync"
	"testing"
)

func BenchmarkShardedMailbox_Throughput(b *testing.B) {
	numShards := uint64(runtime.GOMAXPROCS(0))
	if numShards < 1 {
		numShards = 1
	}
	q := NewShardedMailbox[int]()

	numProducers := runtime.GOMAXPROCS(0)
	numConsumers := runtime.GOMAXPROCS(0)

	var wg sync.WaitGroup
	wg.Add(numProducers + numConsumers)

	// b.N - это общее количество операций.
	// Мы должны убедиться, что количество отправленных и полученных сообщений равно.
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
				err := q.Enqueue(uint64(producerID), i)
				if err != nil {
					b.Error(err)
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
				if err != nil {
					b.Error(err)
				}
			}
		}(c)
	}

	// Ждем, пока все продюсеры и консюмеры не закончат работу.
	wg.Wait()
	q.Close()

}
