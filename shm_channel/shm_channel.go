package shm_channel

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/hft-ipc/shm"
	"github.com/edsrzf/mmap-go"

	"github.com/GenshIv/nexus/shm_mutex" // Импортируем наш ShmMutex
)

// ErrClosed is returned when an operation is performed on a closed channel.
var ErrClosed = errors.New("shm_channel: channel is closed")

// ShmChannel represents a buffered channel implemented over shared memory.
// It allows inter-process communication with blocking send/receive operations.
// NOTE: This implementation is NOT MPMC safe due to underlying ringbuf.RingBuffer.
// Use ShardedShmChannel for MPMC scenarios.
type ShmChannel struct {
	file      *os.File  // The file backing the shared memory
	mapped    mmap.MMap // The memory-mapped segment
	capacity  uint64    // The maximum number of items the channel can hold
	itemSize  uint64    // The size of each item (payload)
	closedPtr *uint32   // Pointer to closed flag in shared memory

	// Ring buffer for actual data storage
	rb *ringbuf.RingBuffer
}

// NewShmChannel creates or opens a shared memory channel.
// path is the file path for the memory-mapped file.
// capacity is the number of items the channel can buffer.
// itemSize is the size of each item (payload) that will be sent through the channel.
// It's crucial that itemSize matches ringbuf.PayloadSize.
func NewShmChannel(path string, capacity uint64, itemSize uint64) (*ShmChannel, error) {
	if itemSize != ringbuf.PayloadSize {
		return nil, fmt.Errorf("itemSize (%d) must match ringbuf.PayloadSize (%d)", itemSize, ringbuf.PayloadSize)
	}

	// Calculate total SHM size required for the ring buffer and metadata
	ringbufDataAreaSize := capacity * itemSize
	ringbufTotalSize := uint64(ringbuf.DataOffset) + ringbufDataAreaSize
	// Append metadata (closed flag - 4 bytes)
	shmChannelMetadataSize := uint64(4)
	totalShmSize := ringbufTotalSize + shmChannelMetadataSize

	mapped, file, err := shm.OpenOrCreateMmap(path, int(totalShmSize))
	if err != nil {
		return nil, err
	}

	// Initialize ring buffer within the first part of the SHM segment
	rb := ringbuf.Init(mapped[:ringbufTotalSize], capacity)

	// Get pointer to closed flag in the remaining part of the SHM segment
	closedPtr := (*uint32)(unsafe.Pointer(&mapped[ringbufTotalSize]))

	return &ShmChannel{
		file:      file,
		mapped:    mapped,
		capacity:  capacity,
		itemSize:  itemSize,
		closedPtr: closedPtr,
		rb:        rb,
	}, nil
}

// Send sends an item to the channel. It blocks if the channel is full, or returns ErrClosed if the channel is closed.
func (sc *ShmChannel) Send(item []byte) error {
	if uint64(len(item)) != sc.itemSize {
		panic(fmt.Sprintf("shm_channel: item size (%d) does not match channel's itemSize (%d)", len(item), sc.itemSize))
	}

	for {
		if atomic.LoadUint32(sc.closedPtr) == 1 {
			return ErrClosed
		}
		// Try to push to the ring buffer. ringbuf.Push handles its own MPMC synchronization.
		if sc.rb.Push(sc.mapped, item) {
			return nil // Successfully sent
		}
		runtime.Gosched() // Yield to other goroutines/processes if channel is full
	}
}

// Receive receives an item from the channel. It blocks if the channel is empty, or returns ErrClosed if closed and empty.
func (sc *ShmChannel) Receive(item []byte) error {
	if uint64(len(item)) != sc.itemSize {
		panic(fmt.Sprintf("shm_channel: item size (%d) does not match channel's itemSize (%d)", len(item), sc.itemSize))
	}

	for {
		// Try to pop from the ring buffer. ringbuf.Pop handles its own MPMC synchronization.
		if sc.rb.Pop(sc.mapped, item) {
			return nil // Successfully received
		}
		if atomic.LoadUint32(sc.closedPtr) == 1 {
			// Try to pop one last time in case an item was written just before closing
			if sc.rb.Pop(sc.mapped, item) {
				return nil
			}
			return ErrClosed
		}
		runtime.Gosched() // Yield to other goroutines/processes if channel is empty
	}
}

// Close logically closes the channel, preventing any further Send operations.
func (sc *ShmChannel) Close() error {
	if sc.closedPtr != nil {
		atomic.StoreUint32(sc.closedPtr, 1)
	}
	return nil
}

// Unmap cleans up the shared memory resources.
// This should be called by one of the processes when the channel is no longer needed.
func (sc *ShmChannel) Unmap() error {
	var errs []error
	if err := sc.mapped.Unmap(); err != nil {
		errs = append(errs, fmt.Errorf("failed to unmap shared memory: %w", err))
	}
	if err := sc.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close file: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during ShmChannel unmap: %v", errs)
	}
	return nil
}

// ShardedShmChannel represents a MPMC-safe buffered channel implemented over shared memory
// using multiple underlying ShmChannels (shards) protected by ShmMutexes.
type ShardedShmChannel struct {
	basePath  string
	numShards uint64
	capacity  uint64
	itemSize  uint64
	shards    []*ShmChannel

	sendMutexes   []*shm_mutex.ShmMutex // One mutex per shard for send operations
	recvMutexes   []*shm_mutex.ShmMutex // One mutex per shard for receive operations
	nextSendShard atomic.Uint64         // Round-robin counter for senders
	nextRecvShard atomic.Uint64         // Round-robin counter for receivers
}

// NewShardedShmChannel creates or opens a sharded shared memory channel.
// basePath is the base file path for the memory-mapped files (each shard will have its own file).
// numShards is the number of shards to use.
// capacityPerShard is the capacity of each individual shard.
// itemSize is the size of each item (payload).
func NewShardedShmChannel(basePath string, numShards, capacityPerShard, itemSize uint64) (*ShardedShmChannel, error) {
	if itemSize != ringbuf.PayloadSize {
		return nil, fmt.Errorf("itemSize (%d) must match ringbuf.PayloadSize (%d)", itemSize, ringbuf.PayloadSize)
	}
	if numShards == 0 {
		return nil, fmt.Errorf("numShards must be greater than 0")
	}

	shards := make([]*ShmChannel, numShards)
	sendMutexes := make([]*shm_mutex.ShmMutex, numShards)
	recvMutexes := make([]*shm_mutex.ShmMutex, numShards)

	// Calculate total SHM size required for each shard's ring buffer
	ringbufDataAreaSize := capacityPerShard * itemSize
	ringbufTotalSize := uint64(ringbuf.DataOffset) + ringbufDataAreaSize

	// Each shard also needs a closed flag (4 bytes)
	shmChannelMetadataSize := uint64(4)
	shmChannelTotalSize := ringbufTotalSize + shmChannelMetadataSize

	// And two mutexes (each uses a uint32)
	mutexesSize := uint64(2 * unsafe.Sizeof(uint32(0)))
	totalShardFileSize := shmChannelTotalSize + mutexesSize

	for i := uint64(0); i < numShards; i++ {
		shardPath := fmt.Sprintf("%s_shard_%d.bin", basePath, i)
		mapped, file, err := shm.OpenOrCreateMmap(shardPath, int(totalShardFileSize))
		if err != nil {
			// Clean up already created shards
			for j := uint64(0); j < i; j++ {
				shards[j].Unmap()
				os.Remove(fmt.Sprintf("%s_shard_%d.bin", basePath, j))
			}
			return nil, fmt.Errorf("failed to create shard %d: %w", i, err)
		}

		rb := ringbuf.Init(mapped[:ringbufTotalSize], capacityPerShard)
		shards[i] = &ShmChannel{
			file:      file,
			mapped:    mapped[:shmChannelTotalSize],
			capacity:  capacityPerShard,
			itemSize:  itemSize,
			closedPtr: (*uint32)(unsafe.Pointer(&mapped[ringbufTotalSize])),
			rb:        rb,
		}

		// Initialize mutexes for this shard
		sendMutexes[i] = shm_mutex.NewShmMutex(mapped, uintptr(shmChannelTotalSize))
		recvMutexes[i] = shm_mutex.NewShmMutex(mapped, uintptr(shmChannelTotalSize)+unsafe.Sizeof(uint32(0)))
	}

	return &ShardedShmChannel{
		basePath:    basePath,
		numShards:   numShards,
		capacity:    capacityPerShard * numShards, // Total capacity
		itemSize:    itemSize,
		shards:      shards,
		sendMutexes: sendMutexes,
		recvMutexes: recvMutexes,
	}, nil
}

// Send sends an item to a shard in a round-robin fashion. It blocks if all shards are full, or returns ErrClosed if closed.
func (ssc *ShardedShmChannel) Send(item []byte) error {
	if uint64(len(item)) != ssc.itemSize {
		panic(fmt.Sprintf("shm_channel: item size (%d) does not match channel's itemSize (%d)", len(item), ssc.itemSize))
	}

	var shardIdx uint64
	var shard *ShmChannel
	var sendMutex *shm_mutex.ShmMutex
	for { // Outer loop: keep trying until sent or closed
		startShardIdx := ssc.nextSendShard.Load() // Load once per pass to ensure consistent starting point

		for i := uint64(0); i < ssc.numShards; i++ { // Inner loop to try all shards
			shardIdx = (startShardIdx + i) % ssc.numShards
			shard = ssc.shards[shardIdx]
			sendMutex = ssc.sendMutexes[shardIdx]

			if atomic.LoadUint32(shard.closedPtr) == 1 {
				return ErrClosed
			}

			if sendMutex.TryLock() { // Try to acquire mutex without blocking
				// Re-check closed inside the lock
				if atomic.LoadUint32(shard.closedPtr) == 1 {
					sendMutex.Unlock()
					return ErrClosed
				}
				if shard.rb.Push(shard.mapped, item) {
					sendMutex.Unlock()
					ssc.nextSendShard.Add(1) // Advance round-robin counter on success
					return nil
				}
				sendMutex.Unlock() // Unlock if Push failed but TryLock succeeded
			}
			// If TryLock failed, or Push failed, continue to next shard immediately
		}

		// If we reached here, it means we iterated through all shards
		// and did not successfully send an item to any of them.
		// So, we yield the processor before trying again.
		runtime.Gosched()
	}
}

// Receive tries to receive an item from any shard in a round-robin fashion. It blocks if all
// shards are empty, or returns ErrClosed if closed and empty.
func (ssc *ShardedShmChannel) Receive(item []byte) error {
	if uint64(len(item)) != ssc.itemSize {
		panic(fmt.Sprintf("shm_channel: item size (%d) does not match channel's itemSize (%d)", len(item), ssc.itemSize))
	}

	for { // Outer loop: keep trying until received or closed and empty
		startShardIdx := ssc.nextRecvShard.Load() // Load once per pass to ensure consistent starting point

		for i := uint64(0); i < ssc.numShards; i++ { // Inner loop to try all shards
			shardIdx := (startShardIdx + i) % ssc.numShards
			shard := ssc.shards[shardIdx]
			recvMutex := ssc.recvMutexes[shardIdx]

			if recvMutex.TryLock() { // Try to acquire mutex without blocking
				if shard.rb.Pop(shard.mapped, item) {
					recvMutex.Unlock()
					ssc.nextRecvShard.Add(1) // Advance round-robin counter on success
					return nil
				}
				recvMutex.Unlock() // Unlock if Pop failed but TryLock succeeded
			}
			// If TryLock failed, or Pop failed, continue to next shard immediately
		}

		// Check if all shards are closed
		allClosed := true
		for _, shard := range ssc.shards {
			if atomic.LoadUint32(shard.closedPtr) == 0 {
				allClosed = false
				break
			}
		}

		if allClosed {
			// If all closed, check one more time if we can drain any remaining items
			allEmpty := true
			for i := uint64(0); i < ssc.numShards; i++ {
				shardIdx := (startShardIdx + i) % ssc.numShards
				shard := ssc.shards[shardIdx]
				recvMutex := ssc.recvMutexes[shardIdx]

				if recvMutex.TryLock() {
					if shard.rb.Pop(shard.mapped, item) {
						recvMutex.Unlock()
						return nil // Late pop succeeded
					}
					// Check if truly empty (Head != Tail means not empty)
					if atomic.LoadUint64(&shard.rb.Head) != atomic.LoadUint64(&shard.rb.Tail) {
						allEmpty = false
					}
					recvMutex.Unlock()
				} else {
					allEmpty = false
				}
			}

			if allEmpty {
				return ErrClosed
			}
		}

		// If we reached here, it means we iterated through all shards
		// and did not successfully receive an item from any of them.
		// So, we yield the processor before trying again.
		runtime.Gosched()
	}
}

// Close logically closes all shards.
func (ssc *ShardedShmChannel) Close() error {
	var errs []error
	for i, shard := range ssc.shards {
		if err := shard.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close shard %d: %w", i, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during ShardedShmChannel close: %v", errs)
	}
	return nil
}

// Unmap cleans up the shared memory resources for all shards.
func (ssc *ShardedShmChannel) Unmap() error {
	var errs []error

	for i, shard := range ssc.shards {
		if err := shard.Unmap(); err != nil {
			errs = append(errs, fmt.Errorf("failed to unmap shard %d: %w", i, err))
		}
		os.Remove(fmt.Sprintf("%s_shard_%d.bin", ssc.basePath, i)) // Clean up shard files
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors during ShardedShmChannel unmap: %v", errs)
	}
	return nil
}
