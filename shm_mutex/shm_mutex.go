package shm_mutex

import (
	"runtime"
	"sync/atomic"
	"time" // Добавлен импорт time
	"unsafe"
)

const (
	// MutexLocked represents the locked state of the mutex
	MutexLocked uint32 = 1
	// MutexUnlocked represents the unlocked state of the mutex
	MutexUnlocked uint32 = 0

	// ShmWaitGroupStateSize defines the total size required for ShmWaitGroup's internal state in shared memory.
	// It includes space for its internal mutex (uint32) and its counter (uint64).
	ShmWaitGroupStateSize = uintptr(unsafe.Sizeof(uint32(0))) + uintptr(unsafe.Sizeof(uint64(0)))

	// Internal offsets for ShmWaitGroup's state within its allocated segment
	shmWaitGroupMutexOffset   uintptr = 0
	shmWaitGroupCounterOffset uintptr = unsafe.Sizeof(uint32(0))

	// Backoff parameters for ShmMutex
	maxSpins     = 1000                  // Количество спинов перед yield
	maxYields    = 100                   // Количество yield'ов перед сном
	maxSleepTime = 10 * time.Millisecond // Максимальное время сна
)

// ShmMutex provides a mutex that operates on a shared memory segment.
// It uses atomic operations to ensure synchronization.
// The mutex state is stored at a specific offset within the shared memory.
type ShmMutex struct {
	shmSegment []byte  // The shared memory segment
	offset     uintptr // The offset within the segment where the mutex state is stored
}

// NewShmMutex creates a new ShmMutex instance.
// shmSegment is the byte slice representing the shared memory.
// offset is the starting byte offset within shmSegment where the mutex state will be managed.
// It's crucial that this offset points to a 4-byte aligned memory region
// and that no other data interferes with this region.
func NewShmMutex(shmSegment []byte, offset uintptr) *ShmMutex {
	// Ensure the offset is valid and there's enough space for a uint32
	if offset+unsafe.Sizeof(uint32(0)) > uintptr(len(shmSegment)) {
		panic("shm_mutex: offset out of bounds for shared memory segment")
	}
	return &ShmMutex{
		shmSegment: shmSegment,
		offset:     offset,
	}
}

// Lock acquires the mutex. It spins until the mutex is acquired.
func (m *ShmMutex) Lock() {
	spins := 0
	yields := 0
	sleepTime := time.Microsecond // Начальное время сна

	for {
		if atomic.CompareAndSwapUint32(m.statePtr(), MutexUnlocked, MutexLocked) {
			return // Успешно захватили мьютекс
		}

		// Стратегия Backoff
		if spins < maxSpins {
			// Спинним некоторое время
			spins++
			// Можно добавить runtime.procyield() для более явного спиннинга,
			// но runtime.Gosched() или просто цикл часто достаточно.
		} else if yields < maxYields {
			// После спиннинга, отдаем процессор
			runtime.Gosched()
			yields++
		} else {
			// После yield'ов, начинаем спать с экспоненциальным backoff
			time.Sleep(sleepTime)
			sleepTime *= 2 // Удваиваем время сна
			if sleepTime > maxSleepTime {
				sleepTime = maxSleepTime // Ограничиваем максимальное время сна
			}
			spins = 0 // Сбрасываем счетчики после сна
			yields = 0
		}
	}
}

// Unlock releases the mutex.
// It's a programming error to unlock an unlocked mutex.
func (m *ShmMutex) Unlock() {
	// Atomically change the state from Locked to Unlocked.
	// If the mutex was not locked, this indicates a programming error.
	if !atomic.CompareAndSwapUint32(m.statePtr(), MutexLocked, MutexUnlocked) {
		panic("shm_mutex: unlock of unlocked mutex")
	}
}

// statePtr returns a pointer to the uint32 representing the mutex state in shared memory.
func (m *ShmMutex) statePtr() *uint32 {
	// Use unsafe.Pointer to convert the byte slice address + offset to a *uint32.
	return (*uint32)(unsafe.Pointer(&m.shmSegment[m.offset]))
}

// TryLock attempts to acquire the mutex without blocking.
// It returns true if the mutex was acquired, false otherwise.
func (m *ShmMutex) TryLock() bool {
	return atomic.CompareAndSwapUint32(m.statePtr(), MutexUnlocked, MutexLocked)
}

// ShmWaitGroup waits for a collection of goroutines to finish.
// It operates on a shared memory segment, allowing synchronization across processes.
type ShmWaitGroup struct {
	shmSegment []byte // The shared memory segment where the wait group state is stored
	mutex      *ShmMutex
	counterPtr *uint64
}

// NewShmWaitGroup creates a new ShmWaitGroup instance.
// shmSegment is the byte slice representing the shared memory.
// offset is the starting byte offset within shmSegment where the ShmWaitGroup state will be managed.
// The allocated segment must be at least ShmWaitGroupStateSize bytes long.
func NewShmWaitGroup(shmSegment []byte, offset uintptr) *ShmWaitGroup {
	// Ensure the offset is valid and there's enough space for the wait group's state
	if offset+ShmWaitGroupStateSize > uintptr(len(shmSegment)) {
		panic("shm_mutex: ShmWaitGroup offset out of bounds for shared memory segment")
	}

	// Initialize the internal mutex and counter within the allocated segment
	mutex := NewShmMutex(shmSegment, offset+shmWaitGroupMutexOffset)
	counterPtr := (*uint64)(unsafe.Pointer(&shmSegment[offset+shmWaitGroupCounterOffset]))

	// Ensure the counter is initialized to 0
	atomic.StoreUint64(counterPtr, 0)

	return &ShmWaitGroup{
		shmSegment: shmSegment,
		mutex:      mutex,
		counterPtr: counterPtr,
	}
}

// Add increments the ShmWaitGroup counter by 'delta'.
// If the counter becomes zero, all goroutines blocked on Wait are released.
// If the counter goes negative, Add panics.
func (wg *ShmWaitGroup) Add(delta int) {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	newVal := atomic.AddUint64(wg.counterPtr, uint64(delta))
	if newVal == 0 && delta > 0 {
		// This case should not happen if Add is used correctly (only positive delta or negative to reach 0)
		// If it does, it means the counter went from negative to 0, which is an error.
		panic("shm_mutex: ShmWaitGroup counter became zero unexpectedly after Add")
	}
	if newVal < 0 { // Check for negative counter
		panic("shm_mutex: ShmWaitGroup counter became negative")
	}
}

// Done decrements the ShmWaitGroup counter by one.
// If the counter becomes zero, all goroutines blocked on Wait are released.
func (wg *ShmWaitGroup) Done() {
	wg.Add(-1)
}

// Wait blocks until the ShmWaitGroup counter is zero.
func (wg *ShmWaitGroup) Wait() {
	for {
		wg.mutex.Lock()
		val := atomic.LoadUint64(wg.counterPtr)
		wg.mutex.Unlock()

		if val == 0 {
			return
		}
		runtime.Gosched() // Добавлено: отдает управление планировщику
	}
}

// shmAddUint64Assembly performs an atomic addition of delta to the uint64 value at addr.
// It is implemented in assembly for direct hardware access.
//
//go:noescape
func shmAddUint64Assembly(addr *uint64, delta uint64) uint64

// ShmAddUint64Assembly performs an atomic addition of delta to a uint64 value
// stored in shared memory at the given offset.
// It uses an assembly implementation for direct hardware access.
// It returns the new value.
func ShmAddUint64Assembly(shmSegment []byte, offset uintptr, delta uint64) uint64 {
	if offset+unsafe.Sizeof(uint64(0)) > uintptr(len(shmSegment)) {
		panic("shm_mutex: ShmAddUint64Assembly offset out of bounds for shared memory segment")
	}
	ptr := (*uint64)(unsafe.Pointer(&shmSegment[offset]))
	return shmAddUint64Assembly(ptr, delta)
}
