package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/GenshIv/nexus/sharded"
)

func main() {
	numShards := uint64(2) // Must be a power of 2
	shardCapacity := uint64(4)
	q := sharded.NewShardedQueue[int](numShards, shardCapacity)

	var wg sync.WaitGroup
	wg.Add(2) // One for producer, one for consumer

	// Producer Goroutine
	go func() {
		defer wg.Done()
		producerID := uint64(0) // Example producer ID
		for i := 0; i < 10; i++ {
			item := i + 100
			// This call is non-blocking. It returns 'false' if all shards are full.
			if !q.Enqueue(producerID, item) {
				fmt.Printf("Sharded queue is full, dropping item: %d\n", item)
				// In a real app, you might retry, use a backoff strategy, or drop the item.
				runtime.Gosched() // Yield to consumer
			} else {
				fmt.Printf("Produced: %d\n", item)
			}
		}
		q.Close() // Signal that no more items will be enqueued
	}()

	// Consumer Goroutine
	go func() {
		defer wg.Done()
		for {
			// This call is non-blocking. It returns 'false' if the queue is empty.
			if item, ok := q.Dequeue(); ok {
				fmt.Printf("Consumed: %d\n", item)
			} else if q.IsClosed() {
				// If the queue is closed and we confirmed it's empty, we can exit.
				break
			} else {
				// Queue is not closed but is temporarily empty. Yield to other goroutines.
				runtime.Gosched()
			}
		}
	}()

	wg.Wait() // Wait for both producer and consumer to finish
	fmt.Println("Sharded MPMC Example Finished")
}
