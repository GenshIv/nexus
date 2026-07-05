# Nexus: High-Performance Go Concurrency Primitives

Nexus is a Go project providing high-performance, lock-free concurrency primitives designed for systems requiring extremely fast, low-level message passing.

It currently includes:

*   **`spsc`**: A lock-free, Single-Producer-Single-Consumer (SPSC) queue.
*   **`sharded`**: A **Sharded Mailbox** for highly scalable, lock-free, Multiple-Producer-Multiple-Consumer (MPMC) message exchange.

## Core Philosophy

The primitives in this project are built for speed and scalability by adhering to core principles of mechanical sympathy:

*   **Lock-Free**: All structures rely on atomic operations instead of slower, mutex-based locks, minimizing kernel context switches.
*   **Decentralized Design**: The `sharded` mailbox uses a fully decentralized architecture. There are no central counters or shared "hint" variables, which eliminates bottlenecks and ensures performance scales with the number of cores.
*   **Cache-Friendly**: Data structures are padded to prevent false sharing between CPU cores.

## `sharded` Package: Sharded Mailbox

The `ShardedMailbox` is not a traditional queue but a collection of single-element "mailboxes" (shards). It is designed to facilitate a high-throughput exchange of messages between many producers and many consumers.

The core principle is **"producer locks and sends, consumer receives and unlocks."**

### How Sharding Works

1.  The `ShardedMailbox` is initialized with `N` independent mailboxes (shards).
2.  Each producer and consumer is assigned a unique ID. This ID determines their "home" shard.
3.  **Producers (`Enqueue`)**: A producer attempts to "lock" and place a message in a mailbox. It uses a "casino" strategy, starting with a shard next to its home shard, to immediately distribute load and avoid creating "hot spots." If all mailboxes are locked, it waits (blocks).
4.  **Consumers (`Dequeue`)**: A consumer attempts to "unlock" and retrieve a message. It first checks its home shard for high cache affinity. If empty, it attempts to "steal" work from other shards. If all mailboxes are empty, it waits (blocks).

This decentralized approach minimizes contention and allows for excellent scalability on multi-core systems.

## Performance

The `ShardedMailbox` demonstrates excellent performance and scalability, significantly outperforming a standard **unbuffered** Go channel (which is also a blocking operation and the closest ideological equivalent).

Benchmarks are run on a 64-core `AMD EPYC 7763` server processor.

### `sharded` (MPMC)

At 4 cores, `ShardedMailbox` is **~7 times faster** than a standard channel. This demonstrates superior performance even at lower concurrency.

```
goos: linux
goarch: amd64
pkg: github.com/GenshIv/nexus/sharded
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkShardedMailbox_MPMC-4    	30498808	        40.03 ns/op
BenchmarkStandardChannel_MPMC-4   	 4457665	       279.5 ns/op
```

### `spsc` (SPSC)

The SPSC queue is **~10.6 times faster** than a standard channel in a single-producer-single-consumer scenario.

```
goos: linux
goarch: amd64
pkg: github.com/GenshIv/nexus/spsc
cpu: AMD EPYC 7763 64-Core Processor                
BenchmarkSPSCQueue_Separate-4    	238401357	         5.094 ns/op
BenchmarkStdChannel_Separate-4   	23047090	        54.08 ns/op
```

## Usage

### Sharded Mailbox Example

```go
package main

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/GenshIv/nexus/sharded"
)

func main() {
	// Use a number of shards equal to GOMAXPROCS for best performance.
	numShards := uint64(runtime.GOMAXPROCS(0))
	mailbox := sharded.NewShardedMailbox[int](numShards)

	var wg sync.WaitGroup
	numMessages := 100
	wg.Add(numMessages * 2)

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
