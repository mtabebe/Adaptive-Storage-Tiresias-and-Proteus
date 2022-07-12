#pragma once

#include "../common/hw.h"

template <class S>
ALWAYS_INLINE char* struct_to_buffer( S* s );

template <class S>
ALWAYS_INLINE S* buffer_to_struct( char* buf, uint32_t size );

template <class S>
ALWAYS_INLINE void copy_struct( S* src, S* dest );

#include "struct_conversion-inl.h"
