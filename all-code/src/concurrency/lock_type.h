#pragma once

#include <string>

#include "../common/hw.h"

enum lock_type {
    UNKNOWN_LOCK,
    SPINLOCK,
    MUTEX_LOCK,
    SEMAPHORE,
};

ALWAYS_INLINE std::string lock_type_string( const lock_type& l_type );
ALWAYS_INLINE lock_type string_to_lock_type( const std::string& s );

#include "lock_type-inl.h"
