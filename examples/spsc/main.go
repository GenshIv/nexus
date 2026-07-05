package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/GenshIv/nexus/spsc"
)

func main() {
	capacity := uint64(4) // Must be a power of 2
	q := spsc.NewSPSCQueue(capacity)

	var wg sync.WaitGroup
	wg.Add(2) // One for producer, one for consumer

	// Producer Goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			item := fmt.Sprintf("item-%d", i)

			// This call will block if the queue is full, and wait for the consumer.
			// It only returns 'false' if the queue has been closed.
			if !q.Enqueue(item) {
				fmt.Println("Queue was closed, stopping producer.")
				break
			}
			fmt.Printf("Produced: %s\n", item)
		}
		// Close the queue only after the producer has finished sending all items.
		q.Close()
	}()

	// Consumer Goroutine
	go func() {
		defer wg.Done()
		// Add a small delay to demonstrate the producer blocking when the queue is full.
		time.Sleep(10 * time.Millisecond)
		for {
			// This call will block if the queue is empty, and wait for the producer.
			// It returns 'false' only when the queue is closed AND empty.
			if item, ok := q.Dequeue(); ok {
				fmt.Printf("Consumed: %v\n", item)
				time.Sleep(5 * time.Millisecond) // Simulate work
			} else {
				// Queue is closed and empty, so we exit.
				break
			}
		}
	}()

	wg.Wait() // Wait for both producer and consumer to finish
	fmt.Println("SPSC Example Finished")
}
