#pragma once

#include <cstring>

#include <glog/logging.h>

// ARBITRARY CASTING FOR SWEET TRANSMISSION OF DATA
// T E M P L A T E S
// E E
// M   M
// P     P
// L       L
// A        A
// T          T
// E           E
// S              S

template <class S>
inline char* struct_to_buffer( S* s ) {
    return (char*) s;
}
template <class S>
inline S* buffer_to_struct( char* buf, uint32_t size ) {
    DCHECK_EQ( size, sizeof( S ) );
    S* s = (S*) buf;
    return s;
}

template <class S>
inline void copy_struct( S* src, S* dest ) {
    std::memcpy( struct_to_buffer<S>( dest ), struct_to_buffer<S>( src ),
                 sizeof( S ) );
}
