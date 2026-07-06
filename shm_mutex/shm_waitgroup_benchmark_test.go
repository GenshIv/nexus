package shm_mutex

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic" // Добавлен импорт atomic
	"testing"
	"unsafe" // Добавлен импорт unsafe

	"github.com/GenshIv/hft-ipc/shm"
)

// BenchmarkShmWaitGroup measures the performance of ShmWaitGroup
func BenchmarkShmWaitGroup(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "bench_shm_waitgroup.bin")
	os.Remove(path) // Ensure clean

	// Allocate shared memory for ShmWaitGroup state
	mapped, file, err := shm.OpenOrCreateMmap(path, int(ShmWaitGroupStateSize))
	if err != nil {
		b.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	wg := NewShmWaitGroup(mapped, 0) // Initialize ShmWaitGroup at offset 0

	numGoroutines := runtime.GOMAXPROCS(0)
	if numGoroutines == 0 {
		numGoroutines = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(numGoroutines)
		for j := 0; j < numGoroutines; j++ {
			go func() {
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

// BenchmarkSyncWaitGroup measures the performance of sync.WaitGroup
func BenchmarkSyncWaitGroup(b *testing.B) {
	var wg sync.WaitGroup

	numGoroutines := runtime.GOMAXPROCS(0)
	if numGoroutines == 0 {
		numGoroutines = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(numGoroutines)
		for j := 0; j < numGoroutines; j++ {
			go func() {
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

// BenchmarkShmAtomicWaitGroup measures the performance of a WaitGroup-like mechanism
// implemented purely with atomic operations on shared memory, without ShmMutex.
func BenchmarkShmAtomicWaitGroup(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "bench_shm_atomic_waitgroup.bin")
	os.Remove(path) // Ensure clean

	// Allocate shared memory for the atomic counter (uint64)
	const counterSize = 8 // Size of uint64
	mapped, file, err := shm.OpenOrCreateMmap(path, counterSize)
	if err != nil {
		b.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	// Pointer to the shared atomic counter
	shmCounterPtr := (*uint64)(unsafe.Pointer(&mapped[0]))
	atomic.StoreUint64(shmCounterPtr, 0) // Initialize counter to 0

	numGoroutines := runtime.GOMAXPROCS(0)
	if numGoroutines == 0 {
		numGoroutines = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add: Increment counter atomically
		atomic.AddUint64(shmCounterPtr, uint64(numGoroutines))

		for j := 0; j < numGoroutines; j++ {
			go func() {
				// Done: Decrement counter atomically
				atomic.AddUint64(shmCounterPtr, ^uint64(0)) // Decrement by 1
			}()
		}

		// Wait: Spin until counter is 0
		for atomic.LoadUint64(shmCounterPtr) != 0 {
			// Spin
			runtime.Gosched()
		}
	}
}

// BenchmarkAtomicWaitGroup measures the performance of a WaitGroup-like mechanism
// implemented purely with atomic operations on a regular in-memory variable.
func BenchmarkAtomicWaitGroup(b *testing.B) {
	var counter uint64 = 0 // Regular in-memory counter

	numGoroutines := runtime.GOMAXPROCS(0)
	if numGoroutines == 0 {
		numGoroutines = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add: Increment counter atomically
		atomic.AddUint64(&counter, uint64(numGoroutines))

		for j := 0; j < numGoroutines; j++ {
			go func() {
				// Done: Decrement counter atomically
				atomic.AddUint64(&counter, ^uint64(0)) // Decrement by 1
			}()
		}

		// Wait: Spin until counter is 0
		for atomic.LoadUint64(&counter) != 0 {
			// Spin
			runtime.Gosched()
		}
	}
}

// BenchmarkShmAtomicCounterAssembly measures the performance of a WaitGroup-like
// mechanism implemented using our custom assembly atomic counter on shared memory.
func BenchmarkShmAtomicCounterAssembly(b *testing.B) {
	os.MkdirAll("test_channels", 0755)
	path := filepath.Join("test_channels", "bench_shm_atomic_asm_waitgroup.bin")
	os.Remove(path) // Ensure clean

	// Allocate shared memory for the atomic counter (uint64)
	const counterSize = 8 // Size of uint64
	mapped, file, err := shm.OpenOrCreateMmap(path, counterSize)
	if err != nil {
		b.Fatalf("Failed to mmap: %v", err)
	}
	defer file.Close()
	defer mapped.Unmap()

	// Initialize counter to 0
	// We can write directly since it's not concurrent initialization.
	*(*uint64)(unsafe.Pointer(&mapped[0])) = 0

	numGoroutines := runtime.GOMAXPROCS(0)
	if numGoroutines == 0 {
		numGoroutines = 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add: Increment counter atomically using assembly function
		ShmAddUint64Assembly(mapped, 0, uint64(numGoroutines))

		for j := 0; j < numGoroutines; j++ {
			go func() {
				// Done: Decrement counter atomically using assembly function
				ShmAddUint64Assembly(mapped, 0, ^uint64(0)) // Decrement by 1
			}()
		}

		// Wait: Spin until counter is 0. We need to load atomically.
		// Since we don't have an assembly Load function, we'll use atomic.LoadUint64 for now.
		for atomic.LoadUint64((*uint64)(unsafe.Pointer(&mapped[0]))) != 0 {
			// Spin
			runtime.Gosched()
		}
	}
}
