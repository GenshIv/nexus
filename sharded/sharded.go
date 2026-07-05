package sharded

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

const cacheLineSize = 64

var (
	ErrClosed = errors.New("mailbox is closed")
	ErrFull   = errors.New("mailbox shard is full")
	ErrEmpty  = errors.New("mailbox shard is empty")
)

type mailbox[T any] struct {
	_       [cacheLineSize]byte
	item    T
	state   atomic.Uint32
	isClose atomic.Bool
	_       [cacheLineSize]byte
}

type ShardedMailbox[T any] struct {
	shards []*mailbox[T]
	mask   uint64
}

func NewShardedMailbox[T any]() *ShardedMailbox[T] {
	mb := new(ShardedMailbox[T])
	numShards := uint64(runtime.GOMAXPROCS(0))
	if numShards == 0 {
		numShards = 1
	}
	if (numShards & (numShards - 1)) != 0 {
		numShards = nextPowerOf2(numShards)
	}
	mb.newShards(numShards)
	return mb
}

func nextPowerOf2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

func (mb *ShardedMailbox[T]) newShards(numShards uint64) {
	if numShards == 0 || (numShards&(numShards-1)) != 0 {
		panic("numShards must be a power of 2")
	}
	mb.shards = make([]*mailbox[T], numShards)
	mb.mask = numShards - 1

	for i := range mb.shards {
		mb.shards[i] = &mailbox[T]{}
	}
}

func (mb *ShardedMailbox[T]) ShardCount() int {
	return len(mb.shards)
}

func (mb *ShardedMailbox[T]) TrySend(shardID uint64, item T) (bool, error) {
	shard := mb.shards[shardID&mb.mask]

	if shard.isClose.Load() {
		return false, ErrClosed
	}

	if shard.state.CompareAndSwap(0, 1) {
		shard.item = item
		return true, nil
	}
	return false, ErrFull
}

func (mb *ShardedMailbox[T]) TryReceive(shardID uint64) (T, bool, error) {
	var zero T
	shard := mb.shards[shardID&mb.mask]

	if shard.isClose.Load() {
		return zero, false, ErrClosed
	}

	if shard.state.CompareAndSwap(1, 0) {
		return shard.item, true, nil
	}
	return zero, false, ErrEmpty
}

func (mb *ShardedMailbox[T]) Enqueue(producerID uint64, item T) error {
	homeShardIndex := producerID & mb.mask
	for {
		for i := uint64(0); i < uint64(len(mb.shards)); i++ {
			ok, err := mb.TrySend((homeShardIndex+i)&mb.mask, item)
			if ok {
				return nil
			}
			if err == ErrClosed {
				return ErrClosed
			}
		}
		runtime.Gosched()
	}
}

func (mb *ShardedMailbox[T]) Dequeue(consumerID uint64) (T, error) {
	var zero T
	homeShardIndex := consumerID & mb.mask
	for {
		for i := uint64(0); i < uint64(len(mb.shards)); i++ {
			item, ok, err := mb.TryReceive((homeShardIndex + i) & mb.mask)
			if ok {
				return item, nil
			}
			if err == ErrClosed {
				continue
			}
		}

		allClosed := true
		for _, shard := range mb.shards {
			if !shard.isClose.Load() {
				allClosed = false
				break
			}
		}

		if allClosed {
			allEmpty := true
			for i := uint64(0); i < uint64(len(mb.shards)); i++ {
				item, ok, _ := mb.TryReceive((homeShardIndex + i) & mb.mask)
				if ok {
					return item, nil
				}
				if mb.shards[(homeShardIndex+i)&mb.mask].state.Load() == 1 {
					allEmpty = false
				}
			}
			if allEmpty {
				return zero, ErrClosed
			}
		}

		runtime.Gosched()
	}
}

func (mb *ShardedMailbox[T]) Close() {
	for _, shard := range mb.shards {
		shard.isClose.Store(true)
	}

	for {
		allEmpty := true
		for _, shard := range mb.shards {
			if shard.state.Load() == 1 {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			break
		}
		time.Sleep(time.Millisecond)
	}
}
