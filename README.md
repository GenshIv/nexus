# Nexus: High-Performance Go Concurrency Primitives

Nexus is a Go project providing high-performance, lock-free concurrency primitives designed for systems requiring extremely fast, low-level message passing.

It currently includes:

*   **`spsc`**: A Single Producer Single Consumer (SPSC) queue.
*   **`sharded`**: A **Sharded Mailbox** for highly scalable, lock-free MPMC (Multiple Producer Multiple Consumer) message exchange.

## Core Philosophy

The primitives in this project are built for speed and scalability by adhering to core principles of mechanical sympathy:

*   **Lock-Free**: All structures rely on atomic operations instead of slower, mutex-based locks.
*   **Contention-Free Design**: The `sharded` mailbox uses a fully decentralized architecture, eliminating central bottlenecks and ensuring performance scales with the number of cores.
*   **Cache-Friendly**: Data structures are padded to prevent false sharing between CPU cores.

## Performance

Benchmarks are run on an `AMD Ryzen 9 7950X3D 16-Core Processor`.

### `sharded` (Sharded Mailbox vs. Standard Channel)

The `ShardedMailbox` demonstrates excellent performance and scalability, outperforming a standard **unbuffered** Go channel (the closest ideological equivalent) by a significant margin.

#### High Concurrency (32 CPU Cores)

At 32 cores, `ShardedMailbox` is **~19.3 times faster** than a standard channel, showcasing its superior scalability.

```
$env:GOMAXPROCS=32; go test -bench="." -benchmem -benchtime=5s ./sharded/
goos: windows
goarch: amd64
pkg: github.com/GenshIv/nexus/sharded
cpu: AMD Ryzen 9 7950X3D 16-Core Processor          
BenchmarkShardedMailbox_MPMC-32         667756815                9.007 ns/op           0 B/op          0 allocs/op
BenchmarkStandardChannel_MPMC-32        33362506               173.8 ns/op             0 B/op          0 allocs/op
```

#### Low Concurrency (2 CPU Cores)

Even at low concurrency, `ShardedMailbox` maintains a performance advantage.

```
$env:GOMAXPROCS=2; go test -bench="." -benchmem -benchtime=5s ./sharded/ 
goos: windows
goarch: amd64
pkg: github.com/GenshIv/nexus/sharded
cpu: AMD Ryzen 9 7950X3D 16-Core Processor          
BenchmarkShardedMailbox_MPMC-2          286453402               21.56 ns/op            0 B/op          0 allocs/op
BenchmarkStandardChannel_MPMC-2         203034567               29.75 ns/op            0 B/op          0 allocs/op
```

## `sharded` Package: Sharded Mailbox

The `sharded` package provides a `ShardedMailbox` structure. This is not a traditional queue but a series of single-element mailboxes ("shards") that allow for a highly concurrent, lock-free exchange of messages.

The core principle is **"producer locks and sends, consumer receives and unlocks."**

### Features
*   **Fully Decentralized**: Each producer and consumer operates based on its own ID, eliminating central bottlenecks.
*   **Scalable**: Performance increases with the number of available CPU cores.
*   **Blocking API**: `Enqueue` and `Dequeue` calls block until a corresponding slot is found, simplifying user code.

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
	// Use a number of shards equal to the number of CPUs for best performance.
	numShards := uint64(runtime.GOMAXPROCS(0))
	mailbox := sharded.NewShardedMailbox[int](numShards)

	var wg sync.WaitGroup
	numMessages := 100

	wg.Add(numMessages * 2)

	// Launch consumers
	for i := 0; i < numMessages; i++ {
		go func(consumerID int) {
			defer wg.Done()
			item := mailbox.Dequeue(uint64(consumerID))
			fmt.Printf("Consumer %d received: %d\n", consumerID, item)
		}(i)
	}

	// Launch producers
	for i := 0; i < numMessages; i++ {
		go func(producerID int) {
			defer wg.Done()
			item := 1000 + producerID
			mailbox.Enqueue(uint64(producerID), item)
		}(i)
	}

	wg.Wait()
	fmt.Println("All messages exchanged.")
}
```

## Installation

```bash
go get github.com/GenshIv/nexus
```

## Author
Igor Ivanuto

## License
This project is licensed under the MIT License.
