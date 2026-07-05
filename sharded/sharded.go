package sharded

import (
	"runtime"
	"sync/atomic"
)

const cacheLineSize = 64

type mailbox[T any] struct {
	_       [cacheLineSize]byte
	item    T
	state   atomic.Uint32 // 0 = empty, 1 = full
	isClose atomic.Bool
	_       [cacheLineSize]byte
}

type ShardedMailbox[T any] struct {
	shards []*mailbox[T]
	mask   uint64
}

func NewShardedMailbox[T any]() *ShardedMailbox[T] {
	mb := new(ShardedMailbox[T])
	mb.NewShards(uint64(runtime.GOMAXPROCS(0)))
	return mb
}

func (mb *ShardedMailbox[T]) NewShards(numShards uint64) {

	mb.shards = make([]*mailbox[T], numShards)
	mb.mask = numShards - 1

	for i := range mb.shards {
		mb.shards[i] = &mailbox[T]{}
	}

}

func (mb *ShardedMailbox[T]) TrySend(shardID uint64, item T) (bool, error) {
	shard := mb.shards[shardID&mb.mask]

	if shard.isClose.Load() {
		return false, nil
	}

	if shard.state.CompareAndSwap(0, 1) {
		shard.item = item
		return true, nil
	}
	return false, nil
}

func (mb *ShardedMailbox[T]) TryReceive(shardID uint64) (T, bool, error) {
	var zero T
	shard := mb.shards[shardID]

	if shard.isClose.Load() {
		return zero, false, nil
	}

	if shard.state.CompareAndSwap(1, 0) {
		return shard.item, true, nil
	}
	return zero, false, nil
}

func (mb *ShardedMailbox[T]) Enqueue(producerID uint64, item T) error {
	homeShardIndex := producerID & mb.mask
	for {
		// "Казино": ищем свободный слот, начиная с соседа.
		for i := uint64(1); i <= uint64(len(mb.shards)); i++ {
			if ok, err := mb.TrySend((homeShardIndex+i)&mb.mask, item); ok || err != nil {
				return err
			}
		}
		runtime.Gosched()
	}
}

func (mb *ShardedMailbox[T]) Dequeue(consumerID uint64) (T, error) {
	homeShardIndex := consumerID & mb.mask
	for {
		// Симметричное "казино": ищем полный слот, начиная с соседа.
		for i := uint64(1); i <= uint64(len(mb.shards)); i++ {
			if item, ok, err := mb.TryReceive((homeShardIndex + i) & mb.mask); ok {
				return item, err
			}
		}
		runtime.Gosched()
	}
}

func (mb *ShardedMailbox[T]) Close() {
	// Симметричное "казино": ищем полный слот, начиная с соседа.
	for i := uint64(0); i < uint64(len(mb.shards)); i++ {
		mb.shards[i].isClose.Store(true)
	}
	runtime.Gosched()
}
