#pragma once

// on tembo it is 64
// sudo cat /sys/devices/system/cpu/cpu0/cache/index*/coherency_line_size
#define CACHELINE_SIZE 64

// This is the same as a PAUSE instruction, but works on machines
// without the PAUSE instruction
#define CPU_RELAX() asm volatile( "rep;nop" : : : "memory" )

// Presents GCC from reordering instructing around the compiler barrier
#define COMPILER_BARRIER() asm volatile( "" : : : "memory" )

// Stores are always ordered on x86, so this doesn't order them
// However, per Section 11.10 of the SDM, this flushes the store buffer
#define SFENCE() asm volatile( "sfence" : : : "memory" )

// Memory barrier
// Your registers need to be reloaded right now son, and your loads will read any prior stores
#define MFENCE() asm volatile( "mfence" : : : "memory" )

//LFENCE
// If I understand this right, this is more or less a compiler barrier
// However, it's more correct to write an lfence than a compiler barrier when 
// I want an lfence, so...
#define LFENCE() asm volatile( "lfence" : : : "memory" )

#define ALWAYS_INLINE __attribute__( ( always_inline ) )

#define likely( x ) __builtin_expect( ( x ), 1 )
#define unlikely( x ) __builtin_expect( ( x ), 0 )

// Only SIMD if we expect a large number of sites
#if USE_SIMD
#define __SIMD_LOOP__ _Pragma( "GCC ivdep" )
#else
#define __SIMD_LOOP__
#endif

#define negative_to_zero( x ) x < 0 ? 0 : x
