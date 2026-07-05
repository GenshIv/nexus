# Nexus: High-Performance Go Queues

Nexus is a Go project providing high-performance, lock-free queue implementations for various concurrency patterns. It is designed for systems requiring extremely fast context delivery, such as low-level database operations (interacting with the core or drivers), lightweight communication between goroutines, and efficient sharding.

### Core Advantages
*   **Blazing Fast**: Operates significantly faster than standard Go channels.
*   **Zero Allocations**: Designed to avoid memory allocations in hot paths for predictable performance and reduced garbage collector pressure.
*   **Intelligent Load Balancing**: The `sharded` queue includes a smart internal balancer for efficient work distribution.
*   **Fine-Grained Tuning**: Performance can be finely tuned for specific loads, as demonstrated in the benchmarks.

It currently includes:

*   **`spsc`**: A Single Producer Single Consumer (SPSC) queue optimized for speed and efficiency.
*   **`sharded`**: A Sharded Multiple Producer Multiple Consumer (MPMC) queue designed for high throughput in concurrent environments.

## Performance

Benchmarks are run on an `AMD Ryzen 9 7950X3D 16-Core Processor`.

### `spsc` (Single Producer, Single Consumer)

The `spsc` queue is approximately **10 times faster** than a standard buffered channel for SPSC workloads.

```
$env:GOMAXPROCS=8; go test -bench="." -benchmem -benchtime=5s ./spsc/
goos: windows
goarch: amd64
pkg: github.com/GenshIv/nexus/spsc
cpu: AMD Ryzen 9 7950X3D 16-Core Processor          
BenchmarkSPSCQueue_Separate-8           1000000000               2.530 ns/op           0 B/op          0 allocs/op
BenchmarkStdChannel_Separate-8          218232487               27.53 ns/op            0 B/op          0 allocs/op
```

### `sharded` (Multiple Producer, Multiple Consumer)

The `sharded` queue is approximately **7.8 times faster** than a standard buffered channel for MPMC workloads.

```
$env:GOMAXPROCS=8; go test -bench="." -benchmem -benchtime=5s ./sharded/
goos: windows
goarch: amd64
pkg: github.com/GenshIv/nexus/sharded
cpu: AMD Ryzen 9 7950X3D 16-Core Processor          
BenchmarkShardedQueue_MPMC-8            1000000000               4.866 ns/op           0 B/op          0 allocs/op
BenchmarkStandardChannel_MPMC-8         159876902               38.09 ns/op            0 B/op          0 allocs/op
```
**Note on `sharded` performance**: On modern CPUs with heterogeneous cores (like performance-cores and efficiency-cores), the results for the sharded queue may vary. The benchmark shows optimal results when goroutines are scheduled on high-performance cores. If scheduled on less performant cores, the results may be lower, depending on the core's speed.

## `spsc` Package: Single Producer Single Consumer Queue

The `spsc` package offers a highly optimized, lock-free SPSC queue implementation. It uses a ring buffer and atomic operations, with batching for improved performance.

### Features
*   **Lock-Free**: Achieves concurrency without mutexes, relying on atomic operations.
*   **High Performance**: Optimized for single producer, single consumer scenarios.
*   **Batching**: Uses internal batching to reduce the overhead of atomic updates.
*   **Blocking**: `Enqueue` and `Dequeue` calls will block (wait) if the queue is full or empty, respectively.

### Usage Example

```go
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
```

## `sharded` Package: Sharded Multiple Producer Multiple Consumer Queue

The `sharded` package provides a lock-free MPMC queue implementation that uses sharding to scale performance.

### Features
*   **Lock-Free MPMC**: Designed for high concurrency without traditional locks.
*   **Sharding**: Distributes load across multiple internal queues (shards) to reduce contention.
*   **Non-Blocking**: `Enqueue` and `Dequeue` are non-blocking and return immediately.
*   **Generic**: Supports any type `T`.

### Usage Example

```go
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
```

### Installation

This project uses Go modules. To use it in your own Go project:

1.  Make sure you have Go 1.18+ installed.
2.  Initialize your module if you haven't already:
    ```bash
    go mod init your_module_name
    ```
3.  Add Nexus as a dependency:
    ```bash
    go get github.com/GenshIv/nexus
    ```
4.  Import the packages:
    ```go
    import (
        "github.com/GenshIv/nexus/spsc"
        "github.com/GenshIv/nexus/sharded"
    )
    ```

### Running Benchmarks

To run the benchmarks for the `spsc` package:
```bash
go test -bench=. -cpu=1 -run=^# -benchtime=1s ./spsc
```

To run the benchmarks for the `sharded` package:
```bash
go test -bench=. -cpu=4 -run=^# -benchtime=1s ./sharded
```

Adjust `-cpu` to match the number of cores you want to test with.

## Author Igor Ivanuto

## Contributing

Feel free to open issues or pull requests if you have suggestions or improvements.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
