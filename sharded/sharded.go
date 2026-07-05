package sharded

import (
	"runtime"
	"sync/atomic"
)

type slot[T any] struct {
	item     T
	sequence uint64
}

const cacheLineSize = 64

const enqueueBatchSize = 8

// lockFreeMPMCShardQueue represents a lock-free MPMC queue, used as a shard.
// It uses a ring buffer and atomic operations to ensure thread safety.
// The algorithm is based on using sequence numbers for each slot.
type lockFreeMPMCShardQueue[T any] struct {
	_        [cacheLineSize]byte
	buffer   []slot[T]
	capacity uint64
	mask     uint64

	head atomic.Uint64
	_    [cacheLineSize - 8]byte

	tail atomic.Uint64
	_    [cacheLineSize - 8]byte

	closed atomic.Uint32
	_      [cacheLineSize - 4]byte
}

func newLockFreeMPMCShardQueue[T any](capacity uint64) *lockFreeMPMCShardQueue[T] {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of 2")
	}
	q := &lockFreeMPMCShardQueue[T]{
		buffer:   make([]slot[T], capacity),
		capacity: capacity,
		mask:     capacity - 1,
	}

	for i := uint64(0); i < capacity; i++ {
		atomic.StoreUint64(&q.buffer[i].sequence, i)
	}
	q.closed.Store(0)
	return q
}

// TryEnqueue attempts to add an item to the queue. It returns true on success, false if the queue is full or closed.
func (q *lockFreeMPMCShardQueue[T]) TryEnqueue(item T) bool {
	if q.closed.Load() == 1 {
		return false
	}

	var currentHead uint64
	var slotIdx uint64
	var slotSequence uint64
	const spinCount = 100
	spinAttempts := 0

	for {
		currentHead = q.head.Load()
		slotIdx = currentHead & q.mask
		slotSequence = atomic.LoadUint64(&q.buffer[slotIdx].sequence)

		if slotSequence == currentHead {
			if q.head.CompareAndSwap(currentHead, currentHead+1) {
				q.buffer[slotIdx].item = item
				atomic.StoreUint64(&q.buffer[slotIdx].sequence, currentHead+1)
				return true
			}
		} else if slotSequence < currentHead {
			if currentHead-q.tail.Load() >= q.capacity {
				return false
			}
		}

		if q.closed.Load() == 1 {
			return false
		}

		spinAttempts++
		if spinAttempts > spinCount {
			runtime.Gosched()
			spinAttempts = 0
		}
	}
}

// TryDequeue attempts to retrieve an item from the queue. It returns (item, true) on success, (nil, false) if the queue is empty or closed.
func (q *lockFreeMPMCShardQueue[T]) TryDequeue() (item T, ok bool) {
	if q.closed.Load() == 1 && q.IsEmpty() {
		return item, false
	}

	var currentTail uint64
	var slotIdx uint64
	var slotSequence uint64
	const spinCount = 100
	spinAttempts := 0

	for {
		currentTail = q.tail.Load()
		slotIdx = currentTail & q.mask
		slotSequence = atomic.LoadUint64(&q.buffer[slotIdx].sequence)

		if slotSequence == currentTail+1 {
			if q.tail.CompareAndSwap(currentTail, currentTail+1) {
				item = q.buffer[slotIdx].item
				atomic.StoreUint64(&q.buffer[slotIdx].sequence, currentTail+q.capacity)
				return item, true
			}
		} else if slotSequence == currentTail {
			if q.head.Load() == currentTail {
				if q.closed.Load() == 1 {
					return item, false
				}
				return item, false
			}
		}

		if q.closed.Load() == 1 && q.IsEmpty() {
			return item, false
		}

		spinAttempts++
		if spinAttempts > spinCount {
			runtime.Gosched()
			spinAttempts = 0
		}
	}
}

// IsEmpty returns true if the queue is empty.
func (q *lockFreeMPMCShardQueue[T]) IsEmpty() bool {
	return q.head.Load() == q.tail.Load()
}

// Close closes the queue.
func (q *lockFreeMPMCShardQueue[T]) Close() {
	q.closed.Store(1)
}

// IsClosed returns true if the queue is closed.
func (q *lockFreeMPMCShardQueue[T]) IsClosed() bool {
	return q.closed.Load() == 1
}

// ShardedQueue represents a sharded queue, consisting of multiple lockFreeMPMCShardQueue instances.
type ShardedQueue[T any] struct {
	shards        []*lockFreeMPMCShardQueue[T]
	numShards     uint64
	mask          uint64
	consumerIndex atomic.Uint64
	_             [cacheLineSize - 8]byte
}

// NewShardedQueue creates a new sharded queue.
// numShards must be a power of 2.
func NewShardedQueue[T any](numShards, shardCapacity uint64) *ShardedQueue[T] {
	if numShards == 0 || (numShards&(numShards-1)) != 0 {
		panic("numShards must be a power of 2")
	}

	q := &ShardedQueue[T]{
		shards:    make([]*lockFreeMPMCShardQueue[T], numShards),
		numShards: numShards,
		mask:      numShards - 1,
	}

	for i := uint64(0); i < numShards; i++ {
		q.shards[i] = newLockFreeMPMCShardQueue[T](shardCapacity)
	}
	q.consumerIndex.Store(0)
	return q
}

// Enqueue adds an item to one of the shard queues.
// This method is non-blocking.
func (q *ShardedQueue[T]) Enqueue(producerID uint64, item T) bool {
	homeShardIndex := producerID & q.mask

	for i := uint64(1); i < q.numShards; i++ {
		shardIndex := (homeShardIndex + i) & q.mask
		if q.shards[shardIndex].TryEnqueue(item) {
			return true
		}
	}

	return false
}

// Dequeue retrieves an item from one of the shard queues.
// It blocks until an item is available or the queue is closed and empty.
func (q *ShardedQueue[T]) Dequeue() (item T, ok bool) {
	startIndex := q.consumerIndex.Add(1)

	for {
		for i := uint64(0); i < q.numShards; i++ {
			shardIndex := (startIndex + i) & q.mask
			shard := q.shards[shardIndex]

			if item, ok := shard.TryDequeue(); ok {
				return item, true
			}
		}

		if q.IsClosed() {
			allEmpty := true
			for _, shard := range q.shards {
				if !shard.IsEmpty() {
					allEmpty = false
					break
				}
			}
			if allEmpty {
				return item, false
			}
		}

		runtime.Gosched()
	}
}

// Close closes all shard queues.
func (q *ShardedQueue[T]) Close() {
	for _, shard := range q.shards {
		shard.Close()
	}
}

// IsClosed returns true if all shard queues are closed.
func (q *ShardedQueue[T]) IsClosed() bool {
	for _, shard := range q.shards {
		if !shard.IsClosed() {
			return false
		}
	}
	return true
}
