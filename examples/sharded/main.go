package main

import (
	"fmt"
	"sync"

	"github.com/GenshIv/nexus/sharded"
)

func main() {
	mailbox := sharded.NewShardedMailbox[int]()

	var wg sync.WaitGroup
	numMessages := 100

	wg.Add(numMessages * 2)

	fmt.Printf("Launching %d producer-consumer pairs on %d shards...\n", numMessages, mailbox.ShardCount())

	for i := 0; i < numMessages; i++ {
		go func(consumerID int) {
			defer wg.Done()
			item, err := mailbox.Dequeue(uint64(consumerID))
			if err != nil {
				fmt.Printf("Consumer %d failed: %v\n", consumerID, err)
				return
			}
			fmt.Printf("Consumer %d received: %d\n", consumerID, item)
		}(i)
	}

	for i := 0; i < numMessages; i++ {
		go func(producerID int) {
			defer wg.Done()
			item := 1000 + producerID
			err := mailbox.Enqueue(uint64(producerID), item)
			if err != nil {
				fmt.Printf("Producer %d failed: %v\n", producerID, err)
				return
			}
			fmt.Printf("Producer %d sent: %d\n", producerID, item)
		}(i)
	}

	wg.Wait()
	fmt.Println("\nAll messages have been successfully exchanged.")
	mailbox.Close()
}
