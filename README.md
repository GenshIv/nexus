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

> **Note**: The `ShardedMailbox` design favors throughput and resilience over strict FIFO (First-In, First-Out) ordering across different shards. While the order of messages sent by a single producer to a single consumer is generally preserved, no such guarantee is made for the global order of messages across all producers and consumers.

### How Sharding Works

1.  The `ShardedMailbox` is initialized with `N` independent mailboxes (shards).
2.  Each producer and consumer is assigned a unique ID. This ID determines their "home" shard.
3.  **Producers (`Enqueue`)**: A producer attempts to "lock" and place a message in a mailbox. It uses a "casino" strategy, starting with a shard next to its home shard, to immediately distribute load and avoid creating "hot spots." If all mailboxes are locked, it waits (blocks).
4.  **Consumers (`Dequeue`)**: A consumer attempts to "unlock" and retrieve a message. It first checks its home shard for high cache affinity. If empty, it attempts to "steal" work from other shards. If all mailboxes are empty, it waits (blocks).

This decentralized approach minimizes contention and allows for excellent scalability on multi-core systems.

### Design Rationale: Self-Balancing Through Shard Locks

A classic MPMC queue with a single ring buffer suffers from severe **contention** on highly parallel systems (32+ cores). All producers and consumers must atomically update the same `head` and `tail` counters, causing a "traffic jam" on the memory bus.

The `ShardedMailbox` solves this by leveraging the Go runtime's scheduler for **self-balancing**.

1.  **Internal Shard Locks**: Each shard is an independent mailbox with its own lock state ("empty" or "full"). This is the only point of contention.
2.  **Go Scheduler**: The Go runtime independently distributes goroutines (producers and consumers) across available CPU cores. More powerful cores naturally execute more operations.
3.  **Emergent Behavior**: Because our `Enqueue` and `Dequeue` operations are blocking, a goroutine on a fast core will quickly find a free/full shard, complete its operation, and move on to the next. A goroutine on a slower core will simply wait longer on its shard's lock. This creates a natural, emergent load-balancing effect: **more messages are processed on more performant cores** without any central coordinator.
4.  **Guaranteed Delivery**: Once a producer or consumer "locks" a shard for its session, it does not release it until the operation is complete, guaranteeing message delivery without interference from other links in the chain.

This approach trusts the Go runtime to handle thread scheduling while providing a fine-grained, decentralized locking mechanism that scales. Instead of trying to outsmart the platform, we use its inherent characteristics (like scheduler behavior and non-uniform core performance) to enhance competitiveness and ensure stable system scaling. We don't just calculate where it's best to run; we allow the system to self-tune and adapt. As a result, overhead is minimized, and performance gains a direct correlation with scaling.

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

## Use Cases

### 1. High-Frequency Trading (HFT) and Fintech
This is the "native" environment for such structures.
*   **Why**: In trading systems, every microsecond of latency in transmitting market quotes or order execution signals is critical.
*   **Application**: Transferring signals between a UDP market data parsing stream and a business logic (strategy) stream. The `spsc` queue will work tens of times faster here than channels, removing latency "jitter."

### 2. Real-time Game Event Processing
*   **Application**: You have thousands of players performing actions (clicks, purchases, leveling up). You need to update the "world state" in real-time.
*   **Why**: `ShardedMailbox` allows distributing the load across processing threads without deadlocks, ensuring player state is not corrupted by data access conflicts.

### 3. Database and Storage Engines
*   **Application**: Implementing Write-Ahead Log (WAL) or LSM-tree structures.
*   **Why**: When data needs to be quickly flushed from memory to disk, the writing thread must not block threads that are reading data. Nexus acts as an ideally fast buffer between application code and disk I/O.

### 4. Network Proxies and API Gateways
*   **Application**: Transferring packets between a network reading thread and a processing thread (e.g., for filtering HTTP headers or TLS termination).
*   **Why**: API gateways (like Envoy or custom Go solutions) handle tens of thousands of requests per second. At these speeds, standard channels become a bottleneck. This module allows building lock-free pipelines that can handle immense load on a single node.

### 5. AI/ML Inference Pipelines
*   **Application**: Transferring frames or tensors between a capture stream (e.g., from a camera or video stream) and a model inference stream (TensorRT/ONNX).
*   **Why**: If the model runs fast but the data transport (queue) is slow, the GPU sits idle. The `spsc` queue is an ideal solution for feeding data to the model without CPU-side bottlenecks.

### Summary: Where Nexus Wins
Use Nexus wherever:
*   **Latency is critical**: When 50-100 ns of channel latency is "too much."
*   **Concurrency is high**: When you have 32, 64, or 128 cores, and standard mutexes start fighting for cache lines.
*   **Predictability is needed**: You want to avoid GC "freezes" caused by extra allocations and runtime object creation.

### When NOT to Use
*   **Low-load applications**: For a web service with 10 requests per second, standard channels are a perfect choice because they are simpler to maintain.
*   **Complex `select` logic**: Go channels are unique in their ability to wait for events from multiple sources. Nexus is a "fast pipe"; it has no selection logic, only direct data exchange.

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

## Future Directions

*   **Continuous Benchmarking**: Setting up regular benchmark runs in GitHub Actions to ensure performance does not degrade with new Go versions or code changes.
*   **Zero-Copy Potential**: For extreme performance scenarios, exploring the possibility of passing objects via `unsafe` pointers or using `sync.Pool` to reuse mailbox objects, completely eliminating GC overhead over long runs.

## Author
Igor Ivanuto

## License
This project is licensed under the MIT License.
