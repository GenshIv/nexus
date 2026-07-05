package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/GenshIv/nexus/sharded"
)

func main() {
	// Создаем ShardedMailbox. Если 0, количество шардов будет определено автоматически.
	mailbox := sharded.NewShardedMailbox[int](0)

	var wg sync.WaitGroup
	numMessages := 100

	wg.Add(numMessages * 2)

	fmt.Printf("Launching %d producer-consumer pairs on %d shards...\n", numMessages, mailbox.NumShards())

	// Launch consumers
	for i := 0; i < numMessages; i++ {
		go func(consumerID int) {
			defer wg.Done()
			// Dequeue is a blocking call.
			item := mailbox.Dequeue(uint64(consumerID))
			fmt.Printf("Consumer %d received: %d\n", consumerID, item)
		}(i)
	}

	// Launch producers
	for i := 0; i < numMessages; i++ {
		go func(producerID int) {
			defer wg.Done()
			item := 1000 + producerID
			// Enqueue is a blocking call.
			mailbox.Enqueue(uint64(producerID), item)
		}(i)
	}

	wg.Wait()
	fmt.Println("\nAll messages have been successfully exchanged.")
}
