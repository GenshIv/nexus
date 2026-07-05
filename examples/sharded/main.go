package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/GenshIv/nexus/sharded"
)

func main() {
	// Для лучшей производительности используйте количество шардов, равное GOMAXPROCS.
	// Это позволяет каждому ядру работать со своим набором почтовых ящиков.
	numShards := uint64(runtime.GOMAXPROCS(0))
	if numShards < 1 {
		numShards = 1
	}
	mailbox := sharded.NewShardedMailbox[int](numShards)

	var wg sync.WaitGroup
	// Мы запустим 10 пар "продюсер-консюмер" для демонстрации.
	numPairs := 10

	wg.Add(numPairs * 2)

	fmt.Printf("Launching %d producer-consumer pairs on %d shards...\n", numPairs, numShards)

	// Запускаем консюмеров
	for i := 0; i < numPairs; i++ {
		go func(consumerID int) {
			defer wg.Done()
			// Dequeue является блокирующей операцией.
			// Она будет ждать, пока соответствующий продюсер не отправит данные.
			item := mailbox.Dequeue(uint64(consumerID))
			fmt.Printf("Consumer %d received: %d\n", consumerID, item)
		}(i)
	}

	// Запускаем продюсеров
	for i := 0; i < numPairs; i++ {
		go func(producerID int) {
			defer wg.Done()
			item := 1000 + producerID
			// Enqueue является блокирующей операцией.
			// Она будет искать свободный ящик и ждать, если все заняты.
			mailbox.Enqueue(uint64(producerID), item)
			fmt.Printf("Producer %d sent: %d\n", producerID, item)
		}(i)
	}

	// Ждем, пока все горутины не завершат свою работу.
	wg.Wait()
	fmt.Println("\nAll messages have been successfully exchanged.")
}
