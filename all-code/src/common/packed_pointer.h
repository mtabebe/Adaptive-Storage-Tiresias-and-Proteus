#pragma once

#include "hw.h"
#include <atomic>
#include <cstdint>

typedef uint64_t                    packed_pointer;
typedef std::atomic<packed_pointer> atomic_packed_pointer;

namespace packed_pointer_ops {
ALWAYS_INLINE packed_pointer set_packed_pointer_int( uint64_t data,
                                                     uint16_t flag_bits );
ALWAYS_INLINE packed_pointer set_packed_pointer_ptr( void *   ptr,
                                                     uint16_t flag_bits );
ALWAYS_INLINE packed_pointer set_flag_bits( const packed_pointer pp,
                                            uint16_t             flag_bits );
ALWAYS_INLINE packed_pointer set_as_int( const packed_pointer pp,
                                         uint64_t             data );
ALWAYS_INLINE packed_pointer set_as_pointer( const packed_pointer pp,
                                             void *               pointer );

ALWAYS_INLINE void *get_pointer( const packed_pointer );
ALWAYS_INLINE uint64_t get_int( const packed_pointer );
ALWAYS_INLINE uint16_t get_flag_bits( const packed_pointer pp );
}  // packed_pointer_ops::

#include "packed_pointer-inl.h"
