package shm_channel

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GenshIv/hft-ipc/ringbuf"
	"github.com/GenshIv/nexus/spsc"
)

const (
	benchmarkChannelCapacity = 1024                // Capacity of channels (power of 2 for SPSC Queue)
	benchmarkItemSize        = ringbuf.PayloadSize // Payload size matching ringbuf.PayloadSize
	numBenchmarkSenders      = 4
	numBenchmarkReceivers    = 4
)

// goChannelWrapper wraps a standard Go channel to match the Send/Receive interface returning error.
type goChannelWrapper struct {
	ch chan []byte
}

func (w *goChannelWrapper) Send(item []byte) error {
	w.ch <- item
	return nil
}

func (w *goChannelWrapper) Receive(item []byte) error {
	val, ok := <-w.ch
	if !ok {
		return ErrClosed
	}
	copy(item, val)
	return nil
}

func BenchmarkGoChannel_MPMC(b *testing.B) {
	ch := make(chan []byte, benchmarkChannelCapacity)
	wrappedCh := &goChannelWrapper{ch: ch}

	// Receivers
	for i := 0; i < numBenchmarkReceivers; i++ {
		go func() {
			item := make([]byte, benchmarkItemSize)
			for {
				err := wrappedCh.Receive(item)
				if err != nil {
					break
				}
			}
		}()
	}

	b.ResetTimer()

	// Senders
	b.RunParallel(func(pb *testing.PB) {
		item := make([]byte, benchmarkItemSize)
		for pb.Next() {
			_ = wrappedCh.Send(item)
		}
	})

	// Close channel to stop receivers
	close(ch)
}

func BenchmarkShardedShmChannel_MPMC(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	basePath := filepath.Join("test_channels", "bench_sharded_shm_channel_mpmc")

	// Determine the number of shards dynamically based on GOMAXPROCS
	dynamicNumShards := uint64(runtime.GOMAXPROCS(0))
	if dynamicNumShards == 0 {
		dynamicNumShards = uint64(runtime.NumCPU())
	}
	if dynamicNumShards < 1 {
		dynamicNumShards = 1
	}

	// Remove shard files from previous runs
	for i := uint64(0); i < dynamicNumShards; i++ {
		os.Remove(fmt.Sprintf("%s_shard_%d.bin", basePath, i))
	}

	// Create ShardedShmChannel
	ch, err := NewShardedShmChannel(basePath, dynamicNumShards, benchmarkChannelCapacity/dynamicNumShards, benchmarkItemSize)
	if err != nil {
		b.Fatalf("Failed to create ShardedShmChannel: %v", err)
	}
	defer ch.Unmap() // Cleanup resources at the end

	// Receivers
	for i := 0; i < numBenchmarkReceivers; i++ {
		go func() {
			item := make([]byte, benchmarkItemSize)
			for {
				err := ch.Receive(item)
				if err != nil {
					break
				}
			}
		}()
	}

	b.ResetTimer()

	// Senders
	b.RunParallel(func(pb *testing.PB) {
		item := make([]byte, benchmarkItemSize)
		for pb.Next() {
			_ = ch.Send(item)
		}
	})

	// Close logically to notify receivers to finish draining
	ch.Close()

	// Wait a moment for receivers to exit before Unmap runs
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkGoChannel_SPSC(b *testing.B) {
	ch := make(chan []byte, benchmarkChannelCapacity)
	wrappedCh := &goChannelWrapper{ch: ch}

	// Receiver
	go func() {
		item := make([]byte, benchmarkItemSize)
		for {
			err := wrappedCh.Receive(item)
			if err != nil {
				break
			}
		}
	}()

	b.ResetTimer()

	// Sender using RunParallel
	b.RunParallel(func(pb *testing.PB) {
		item := make([]byte, benchmarkItemSize)
		for pb.Next() {
			_ = wrappedCh.Send(item)
		}
	})

	close(ch)
}

func BenchmarkShmChannel_SPSC(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "bench_shm_channel_spsc.bin")
	os.Remove(path)

	ch, err := NewShmChannel(path, benchmarkChannelCapacity, benchmarkItemSize)
	if err != nil {
		b.Fatalf("Failed to create ShmChannel: %v", err)
	}
	defer ch.Unmap()

	// Receiver
	go func() {
		item := make([]byte, benchmarkItemSize)
		for {
			err := ch.Receive(item)
			if err != nil {
				break
			}
		}
	}()

	b.ResetTimer()

	// Sender using RunParallel
	b.RunParallel(func(pb *testing.PB) {
		item := make([]byte, benchmarkItemSize)
		for pb.Next() {
			_ = ch.Send(item)
		}
	})

	ch.Close()
	time.Sleep(10 * time.Millisecond)
}

func BenchmarkSPSCQueue_SPSC(b *testing.B) {
	q := spsc.NewSPSCQueue(benchmarkChannelCapacity)

	// Receiver
	go func() {
		for {
			_, ok := q.Dequeue()
			if !ok {
				break
			}
		}
	}()

	b.ResetTimer()

	// Sender using RunParallel
	b.RunParallel(func(pb *testing.PB) {
		item := make([]byte, benchmarkItemSize)
		for pb.Next() {
			if !q.Enqueue(item) {
				break
			}
		}
	})

	q.Close()
}

func BenchmarkAtomicCounter(b *testing.B) {
	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Add(1)
		}
	})
}
