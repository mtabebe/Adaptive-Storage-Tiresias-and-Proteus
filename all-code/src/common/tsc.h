#ifndef __TSC_HDR_H__
#define __TSC_HDR_H__
#include <stdint.h>

/* Taken from Intel:
   https://www.intel.com/content/dam/www/public/us/en/documents/white-papers/ia-32-ia-64-benchmark-code-execution-paper.pdf
   Code from: https://github.com/dterei/Scraps/blob/master/intel_tsc/tsc.h.
   With some modifications to precisely mark the clobbered ASM register lists.
 */

/*
   We use RDTSC and RDTSCP to benchmark function calls because they have low
   overhead (we just read a register)
   Per stack overflow benchmarks, we are looking at around 24 CPU cycles.
   The chief challenge with using RDTSC is that the TSC is not necessarily
   synchronized cores:
   - Not all OS's synchronize it cross core at boot (though most do)
   - There are ASM instructions that can adjust the TSC counter, though it is
   unclear why anyone would do this
   Generally it seems synchronized "enough" so as to not worry about the
   nanosecond-level variance...
   XXX If we get a bunch of jitter, we can RDTSCP first as well (instead of
   RDTSC) and compare the processor ID values,
   dropping any entries we have for cross-core recordings.
 */

/*
   CPUID is a synchronizing instruction, which prevents instructions from being
   reordered before/after it.
   Clobbers rax, rbx, rcx, rdx, so tell GCC as much.
 */
static inline void _sync_tsc( void ) {
    asm volatile( "cpuid" : : : "%rax", "%rbx", "%rcx", "%rdx" );
}

/*
   Return current value of TSC.
   RDTSC puts high bits in %rax, low bits in %rdx.
 */
static inline uint64_t _rdtsc( void ) {

    unsigned lo, hi;
    asm volatile( "rdtsc" : "=a"( hi ), "=d"( lo ):: );
    return ( (uint64_t) hi ) | ( ( (uint64_t) lo ) << 32 );
}

/*
   Return the current value of TSC, but also read the Processor ID.
   Not a serializing instruction, but blocks until the prior instruction has
   executed.
   Uses %rcx as an auxillary register, so mark that in the clobber list.
 */

static inline uint64_t _rdtscp( void ) {
    unsigned lo, hi;
    asm volatile( "rdtscp" : "=a"( hi ), "=d"( lo ) : : "%rcx" );
    return ( (uint64_t) hi ) | ( ( (uint64_t) lo ) << 32 );
}

/*
 Start recording the TSC
*/
static inline uint64_t bench_start( void ) {
    // Synchronizing instruction to avoid processor executing code out of order
    // asm volatile prevents GCC from reordering
    _sync_tsc();

    // Get TSC
    return _rdtsc();
}

/*
 Stop recording the TSC
*/
static inline uint64_t bench_end( void ) {
    // Read the TSC (blocks until prior instructions are executed)
    uint64_t t = _rdtscp();
    // Serializing instruction to avoid code executing out of order
    // Again, ASM volatile prevents reordering
    _sync_tsc();

    return t;
}

#endif  //__TSC_HDR_H__
