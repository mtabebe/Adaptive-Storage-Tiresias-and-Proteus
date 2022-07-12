#pragma once
#include "../common/constants.h"
#include "../common/hw.h"

template <class T>
class alignas( CACHELINE_SIZE ) aligned_atomic {
   private:
    std::atomic<T> value_;

   public:
    ALWAYS_INLINE aligned_atomic();

    ALWAYS_INLINE void store(
        T value, std::memory_order order = std::memory_order_seq_cst );
    ALWAYS_INLINE T load( std::memory_order order = std::memory_order_seq_cst );
};

#include "aligned_atomic.tcc"
