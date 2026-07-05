package spsc

import (
	"runtime"
	"sync/atomic"
)

const batchSize = 64

type SPSCQueue struct {
	head uint64
	_    [7]uint64
	tail uint64
	_    [7]uint64

	producerLocalHead uint64
	producerBatchEnd  uint64
	_                 [7]uint64
	consumerLocalTail uint64
	consumerBatchEnd  uint64
	_                 [7]uint64

	buffer   []interface{}
	capacity uint64
	mask     uint64
	closed   uint32
}

func NewSPSCQueue(capacity uint64) *SPSCQueue {
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		panic("capacity must be a power of 2")
	}

	if batchSize > capacity {
		panic("batchSize must be less than or equal to capacity")
	}
	return &SPSCQueue{
		buffer:   make([]interface{}, capacity),
		capacity: capacity,
		mask:     capacity - 1,
		closed:   0,
	}
}

func (q *SPSCQueue) Enqueue(item interface{}) bool {
	for {
		if atomic.LoadUint32(&q.closed) == 1 {
			return false
		}

		if q.producerLocalHead == q.producerBatchEnd {
			for {
				if atomic.LoadUint32(&q.closed) == 1 {
					return false
				}

				currentHead := atomic.LoadUint64(&q.head)
				currentTail := atomic.LoadUint64(&q.tail)

				if currentHead-currentTail+batchSize > q.capacity {
					runtime.Gosched()
					continue
				}

				newHead := atomic.AddUint64(&q.head, batchSize)
				q.producerLocalHead = newHead - batchSize
				q.producerBatchEnd = newHead

				break
			}
		}

		q.buffer[q.producerLocalHead&q.mask] = item
		q.producerLocalHead++
		return true
	}
}

func (q *SPSCQueue) Dequeue() (interface{}, bool) {
	for {
		if atomic.LoadUint32(&q.closed) == 1 {
			if atomic.LoadUint64(&q.head) == atomic.LoadUint64(&q.tail) {
				return nil, false
			}
		}

		if q.consumerLocalTail == q.consumerBatchEnd {
			for {
				if atomic.LoadUint32(&q.closed) == 1 {
					if atomic.LoadUint64(&q.head) == atomic.LoadUint64(&q.tail) {
						return nil, false
					}
				}

				currentHead := atomic.LoadUint64(&q.head)
				currentTail := atomic.LoadUint64(&q.tail)

				if currentHead-currentTail < batchSize {
					runtime.Gosched()
					continue
				}

				newTail := atomic.AddUint64(&q.tail, batchSize)
				q.consumerLocalTail = newTail - batchSize
				q.consumerBatchEnd = newTail
				break
			}
		}

		item := q.buffer[q.consumerLocalTail&q.mask]
		q.buffer[q.consumerLocalTail&q.mask] = nil
		q.consumerLocalTail++
		return item, true
	}
}

func (q *SPSCQueue) IsEmpty() bool {
	return atomic.LoadUint64(&q.head) == atomic.LoadUint64(&q.tail)
}

func (q *SPSCQueue) IsFull() bool {
	return atomic.LoadUint64(&q.head)-atomic.LoadUint64(&q.tail) == q.capacity
}

func (q *SPSCQueue) Size() uint64 {
	return atomic.LoadUint64(&q.head) - atomic.LoadUint64(&q.tail)
}

func (q *SPSCQueue) Close() {
	atomic.StoreUint32(&q.closed, 1)
}

func (q *SPSCQueue) IsClosed() bool {
	return atomic.LoadUint32(&q.closed) == 1
}
