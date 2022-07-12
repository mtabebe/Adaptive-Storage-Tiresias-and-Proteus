#pragma once

#include <glog/logging.h>

static_assert( sizeof( void* ) == 8, "Pointer must be 8 bytes" );
static_assert( sizeof( uint16_t ) == 2, "uint16_t must be 2 bytes" );
static_assert( sizeof( uint64_t ) == 8, "uint64_t must be 8 bytes" );

namespace packed_pointer_ops {
const static uint64_t k_flag_bits_max = -1ull >> 16;
const static uint32_t k_shift = 48;

inline packed_pointer set_packed_pointer_int( uint64_t d, uint16_t flag_bits ) {
    DVLOG( 50 ) << "Setting data:" << d << ", flag_bits:" << flag_bits;
    packed_pointer pp = 0;

    // make sure it fits
    DCHECK_EQ( 0, ( d >> k_shift ) );

    uint64_t flag_bits_shift =
        ( uint64_t )( ( (uint64_t) flag_bits ) << k_shift );
    pp = d | flag_bits_shift;
    DVLOG( 50 ) << "Set packed_pointer:" << pp << " as:" << d << " | "
                << flag_bits_shift;
    return pp;
}

inline packed_pointer set_packed_pointer_ptr( void* ptr, uint16_t flag_bits ) {
    uint64_t d = (uint64_t) ptr;
    return set_packed_pointer_int( d, flag_bits );
}

inline packed_pointer set_flag_bits( const packed_pointer pp,
                                     uint16_t             flag_bits ) {
    return set_packed_pointer_int( get_int( pp ), flag_bits );
}
inline packed_pointer set_as_int( const packed_pointer pp, uint64_t data ) {
    return set_packed_pointer_int( data, get_flag_bits( pp ) );
}

inline packed_pointer set_as_pointer( const packed_pointer pp, void* ptr ) {
    return set_packed_pointer_ptr( ptr, get_flag_bits( pp ) );
}

inline void* get_pointer( const packed_pointer pp ) {
    void* ptr = (void*) get_int( pp );
    return ptr;
}
inline uint64_t get_int( const packed_pointer pp ) {
    uint64_t i = pp & k_flag_bits_max;
    return i;
}
inline uint16_t get_flag_bits( const packed_pointer pp ) {
    uint16_t e = pp >> k_shift;
    return e;
}
}  // packed_pointer_ops::
