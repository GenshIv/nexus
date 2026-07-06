package shm_channel

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestShmChannel_Close(t *testing.T) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "test_shm_channel_close.bin")
	os.Remove(path)

	capacity := uint64(5)
	itemSize := uint64(benchmarkItemSize)

	ch, err := NewShmChannel(path, capacity, itemSize)
	if err != nil {
		t.Fatalf("Failed to create ShmChannel: %v", err)
	}
	defer ch.Unmap()

	// Send some items
	item1 := make([]byte, itemSize)
	copy(item1, []byte("hello"))
	if err := ch.Send(item1); err != nil {
		t.Fatalf("Failed to send item1: %v", err)
	}

	item2 := make([]byte, itemSize)
	copy(item2, []byte("world"))
	if err := ch.Send(item2); err != nil {
		t.Fatalf("Failed to send item2: %v", err)
	}

	// Close logically
	if err := ch.Close(); err != nil {
		t.Fatalf("Failed to close channel: %v", err)
	}

	// Try sending on closed channel -> should return ErrClosed
	item3 := make([]byte, itemSize)
	copy(item3, []byte("fails"))
	if err := ch.Send(item3); !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed on Send, got: %v", err)
	}

	// Receive should drain the existing items first
	recv1 := make([]byte, itemSize)
	if err := ch.Receive(recv1); err != nil {
		t.Fatalf("Failed to receive recv1: %v", err)
	}
	if string(recv1[:5]) != "hello" {
		t.Errorf("Expected 'hello', got: %s", string(recv1))
	}

	recv2 := make([]byte, itemSize)
	if err := ch.Receive(recv2); err != nil {
		t.Fatalf("Failed to receive recv2: %v", err)
	}
	if string(recv2[:5]) != "world" {
		t.Errorf("Expected 'world', got: %s", string(recv2))
	}

	// Subsequent receive on closed & empty channel should return ErrClosed
	recv3 := make([]byte, itemSize)
	if err := ch.Receive(recv3); !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed on Receive, got: %v", err)
	}
}

func TestShardedShmChannel_Close(t *testing.T) {
	os.MkdirAll("test_channels", 0755)
	basePath := filepath.Join("test_channels", "test_sharded_shm_channel_close")

	numShards := uint64(2)
	capacityPerShard := uint64(5)
	itemSize := uint64(benchmarkItemSize)

	// Cleanup old files
	for i := uint64(0); i < numShards; i++ {
		os.Remove(fmt.Sprintf("%s_shard_%d.bin", basePath, i))
	}

	ch, err := NewShardedShmChannel(basePath, numShards, capacityPerShard, itemSize)
	if err != nil {
		t.Fatalf("Failed to create ShardedShmChannel: %v", err)
	}
	defer ch.Unmap()

	// Send some items
	item1 := make([]byte, itemSize)
	copy(item1, []byte("hello"))
	if err := ch.Send(item1); err != nil {
		t.Fatalf("Failed to send item1: %v", err)
	}

	item2 := make([]byte, itemSize)
	copy(item2, []byte("world"))
	if err := ch.Send(item2); err != nil {
		t.Fatalf("Failed to send item2: %v", err)
	}

	// Close logically
	if err := ch.Close(); err != nil {
		t.Fatalf("Failed to close channel: %v", err)
	}

	// Try sending on closed channel -> should return ErrClosed
	item3 := make([]byte, itemSize)
	copy(item3, []byte("fails"))
	if err := ch.Send(item3); !errors.Is(err, ErrClosed) {
		t.Errorf("Expected ErrClosed on Send, got: %v", err)
	}

	// Receivers should drain all items
	recv := make([]byte, itemSize)
	drainedCount := 0
	for {
		err := ch.Receive(recv)
		if errors.Is(err, ErrClosed) {
			break
		}
		if err != nil {
			t.Fatalf("Failed to receive item: %v", err)
		}
		drainedCount++
	}

	if drainedCount != 2 {
		t.Errorf("Expected to drain 2 items, but got %d", drainedCount)
	}
}
