package sharded

import (
	"runtime"
	"sync/atomic"
)

const cacheLineSize = 64

type mailbox[T any] struct {
	_     [cacheLineSize]byte
	item  T
	state atomic.Uint32 // 0 = empty, 1 = full
	_     [cacheLineSize]byte
}

type ShardedMailbox[T any] struct {
	shards []*mailbox[T]
	mask   uint64
}

func NewShardedMailbox[T any](numShards uint64) *ShardedMailbox[T] {
	if numShards == 0 || (numShards&(numShards-1)) != 0 {
		panic("numShards must be a power of 2")
	}
	mb := &ShardedMailbox[T]{
		shards: make([]*mailbox[T], numShards),
		mask:   numShards - 1,
	}
	for i := range mb.shards {
		mb.shards[i] = &mailbox[T]{}
	}
	return mb
}

func (mb *ShardedMailbox[T]) TrySend(shardID uint64, item T) bool {
	shard := mb.shards[shardID&mb.mask]
	if shard.state.CompareAndSwap(0, 1) {
		shard.item = item
		return true
	}
	return false
}

func (mb *ShardedMailbox[T]) TryReceive(shardID uint64) (T, bool) {
	shard := mb.shards[shardID&mb.mask]
	if shard.state.CompareAndSwap(1, 0) {
		return shard.item, true
	}
	var zero T
	return zero, false
}

func (mb *ShardedMailbox[T]) Enqueue(producerID uint64, item T) {
	homeShardIndex := producerID & mb.mask
	for {
		// "Казино": ищем свободный слот, начиная с соседа.
		for i := uint64(1); i <= uint64(len(mb.shards)); i++ {
			if mb.TrySend((homeShardIndex+i)&mb.mask, item) {
				return
			}
		}
		runtime.Gosched()
	}
}

func (mb *ShardedMailbox[T]) Dequeue(consumerID uint64) T {
	homeShardIndex := consumerID & mb.mask
	for {
		// Симметричное "казино": ищем полный слот, начиная с соседа.
		for i := uint64(1); i <= uint64(len(mb.shards)); i++ {
			if item, ok := mb.TryReceive((homeShardIndex + i) & mb.mask); ok {
				return item
			}
		}
		runtime.Gosched()
	}
}
