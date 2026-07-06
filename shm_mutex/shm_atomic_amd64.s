#include "textflag.h"

// func shmAddUint64Assembly(addr *uint64, delta uint64) uint64
// This function performs an atomic addition of 'delta' to the uint64 value at 'addr'.
// It returns the new value after the addition.
//
// Arguments:
//   addr *uint64 (passed in AX)
//   delta uint64 (passed in BX)
//
// Return value:
//   uint64 (returned in AX)
TEXT ·shmAddUint64Assembly(SB), NOSPLIT, $0-24
	MOVQ addr+0(FP), AX // AX = addr (*uint64)
	MOVQ delta+8(FP), BX // BX = delta (uint64)

	// LOCK XADDQ (Exchange and Add)
	// Atomically adds the second operand (BX) to the first operand (value at AX).
	// The original value of the first operand is loaded into the second operand (BX).
	// The result of the addition is stored in the first operand (value at AX).
	//
	// After this instruction:
	//   [AX] (memory location pointed by AX) contains the NEW value.
	//   BX contains the ORIGINAL value that was at [AX].
	LOCK
	XADDQ BX, (AX) // Atomically: [AX] = [AX] + BX; BX = old [AX]

	// We need to return the NEW value, which is now stored at [AX].
	// The Go calling convention expects the return value in AX.
	// So, we load the value from [AX] into AX.
	MOVQ (AX), AX // AX = new value from memory

	// Store AX (new value) into return value slot.
	MOVQ AX, ret+16(FP)
	RET
